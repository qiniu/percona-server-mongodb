/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/s/chunk_manager_inlock.h"

#include <vector>

#include "mongo/base/owned_pointer_vector.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/matcher/extensions_callback_noop.h"
#include "mongo/db/query/collation/collation_index_key.h"
#include "mongo/db/query/index_bounds_builder.h"
#include "mongo/db/query/query_planner.h"
#include "mongo/db/query/query_planner_common.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"
#include <boost/optional.hpp>

namespace mongo {
namespace {

// Used to generate sequence numbers to assign to each newly created ChunkManager
AtomicUInt32 nextCMILSequenceNumber(0);
int MaxSizeSingleChunksMap = 10000;

void checkAllElementsAreOfType(BSONType type, const BSONObj& o) {
    for (const auto&& element : o) {
        uassert(ErrorCodes::ConflictingOperationInProgress,
                str::stream() << "Not all elements of " << o << " are of type " << typeName(type),
                element.type() == type);
    }
}

}  // namespace


std::string extractKeyStringInternalWithLock(const BSONObj& shardKeyValue, Ordering ordering) {
    BSONObjBuilder strippedKeyValue;
    for (const auto& elem : shardKeyValue) {
        strippedKeyValue.appendAs(elem, ""_sd);
    }

    KeyString ks(KeyString::Version::V1, strippedKeyValue.done(), ordering);
    return {ks.getBuffer(), ks.getSize()};
}


ChunkManagerEX::ChunkManagerEX(NamespaceString nss,
                               KeyPattern shardKeyPattern,
                               std::unique_ptr<CollatorInterface> defaultCollator,
                               bool unique,
                               ChunkVersion collectionVersion)
    : _sequenceNumber(nextCMILSequenceNumber.addAndFetch(1)),
      _nss(std::move(nss)),
      _shardKeyPattern(shardKeyPattern),
      _shardKeyOrdering(Ordering::make(_shardKeyPattern.toBSON())),
      _defaultCollator(std::move(defaultCollator)),
      _unique(unique),
      _maxSizeSingleChunksMap(MaxSizeSingleChunksMap),
      _shardVersionSize(_shardVersions.size()),
      _collectionVersion(collectionVersion) {}


ChunkManagerEX::ChunkManagerEX(std::shared_ptr<ChunkManagerEX> other,
                               NamespaceString nss,
                               KeyPattern shardKeyPattern,
                               std::unique_ptr<CollatorInterface> defaultCollator,
                               bool unique)
    : _sequenceNumber(nextCMILSequenceNumber.addAndFetch(1)),
      _nss(nss),
      _shardKeyPattern(shardKeyPattern),
      _shardKeyOrdering(Ordering::make(_shardKeyPattern.toBSON())),
      _defaultCollator(std::move(defaultCollator)),
      _unique(unique),
      _maxSizeSingleChunksMap(MaxSizeSingleChunksMap) {
    if (other) {
        _topIndexMap = other->getTopIndexMap();
        _shardVersions = other->getShardVersionMap();
        _collectionVersion = other->getVersion();
    }
}

ChunkManagerEX::~ChunkManagerEX() = default;

std::shared_ptr<Chunk> ChunkManagerEX::findIntersectingChunk(const BSONObj& shardKey,
                                                             const BSONObj& collation) const {
    const bool hasSimpleCollation = (collation.isEmpty() && !_defaultCollator) ||
        SimpleBSONObjComparator::kInstance.evaluate(collation == CollationSpec::kSimpleSpec);
    if (!hasSimpleCollation) {
        for (BSONElement elt : shardKey) {
            uassert(ErrorCodes::ShardKeyNotFound,
                    str::stream() << "Cannot target single shard due to collation of key "
                                  << elt.fieldNameStringData(),
                    !CollationIndexKey::isCollatableType(elt.type()));
        }
    }

    const auto it = _topIndexMap.upper_bound(_extractKeyString(shardKey));
    if (it == _topIndexMap.end()) {
        log() << "never";
    }

    const auto itSecond = it->second->upper_bound(_extractKeyString(shardKey));

    uassert(ErrorCodes::ShardKeyNotFound,
            str::stream() << "Cannot target single shard using key " << shardKey,
            itSecond != it->second->end() && itSecond->second->containsKey(shardKey));


    return itSecond->second;
}

std::shared_ptr<Chunk> ChunkManagerEX::findIntersectingChunkWithSimpleCollation(
    const BSONObj& shardKey) const {
    return findIntersectingChunk(shardKey, CollationSpec::kSimpleSpec);
}


int ChunkManagerEX::numChunks() const {
    int total_chunks = 0;
    for (auto& it : _topIndexMap) {
        total_chunks += it.second->size();
    }
    return total_chunks;
}


void ChunkManagerEX::getShardIdsForQuery(OperationContext* txn,
                                         const BSONObj& query,
                                         const BSONObj& collation,
                                         std::set<ShardId>* shardIds) const {
    //LOG(1) << "getShardIdsForQuery=" << query.toString();
    auto qr = stdx::make_unique<QueryRequest>(_nss);
    qr->setFilter(query);

    if (!collation.isEmpty()) {
        qr->setCollation(collation);
    } else if (_defaultCollator) {
        qr->setCollation(_defaultCollator->getSpec().toBSON());
    }

    std::unique_ptr<CanonicalQuery> cq =
        uassertStatusOK(CanonicalQuery::canonicalize(txn, std::move(qr), ExtensionsCallbackNoop()));

    // Query validation
    if (QueryPlannerCommon::hasNode(cq->root(), MatchExpression::GEO_NEAR)) {
        uasserted(13502, "use geoNear command rather than $near query");
    }

    // Fast path for targeting equalities on the shard key.
    auto shardKeyToFind = _shardKeyPattern.extractShardKeyFromQuery(*cq);
    if (!shardKeyToFind.isEmpty()) {
        try {
            auto chunk = findIntersectingChunk(shardKeyToFind, collation);
            shardIds->insert(chunk->getShardId());
            return;
        } catch (const DBException&) {
            // The query uses multiple shards
        }
    }

    // Transforms query into bounds for each field in the shard key
    // for example :
    //   Key { a: 1, b: 1 },
    //   Query { a : { $gte : 1, $lt : 2 },
    //            b : { $gte : 3, $lt : 4 } }
    //   => Bounds { a : [1, 2), b : [3, 4) }
    IndexBounds bounds = getIndexBoundsForQuery(_shardKeyPattern.toBSON(), *cq);
    //log() << "bounds = " << bounds.toString();

    // Transforms bounds for each shard key field into full shard key ranges
    // for example :
    //   Key { a : 1, b : 1 }
    //   Bounds { a : [1, 2), b : [3, 4) }
    //   => Ranges { a : 1, b : 3 } => { a : 2, b : 4 }
    BoundList ranges = _shardKeyPattern.flattenBounds(bounds);
    //log for query to shards

    // std::string str;
    // for (BoundList::const_iterator it = ranges.begin(); it != ranges.end(); ++it) {
    //     str += "first=" + it->first.toString() + ",second=" + it->second.toString() + ";";
    // }

    //log() << "ranges  = " << str;

    for (BoundList::const_iterator it = ranges.begin(); it != ranges.end(); ++it) {
        getShardIdsForRange(it->first /*min*/, it->second /*max*/, shardIds);

        // once we know we need to visit all shards no need to keep looping
        if (shardIds->size() == _shardVersionSize) {
            break;
        }
    }

    // SERVER-4914 Some clients of getShardIdsForQuery() assume at least one shard will be returned.
    // For now, we satisfy that assumption by adding a shard with no matches rather than returning
    // an empty set of shards.
    if (shardIds->empty()) {
        shardIds->insert(_shardVersions.begin()->first);
    }
}

void ChunkManagerEX::getShardIdsForRange(const BSONObj& min,
                                         const BSONObj& max,
                                         std::set<ShardId>* shardIds) const {
    const auto bounds = _overlappingTopRanges(min, max, true);
    for (auto it = bounds.first; it != bounds.second; ++it) {
        auto boundsInSecondaryIndex = _overlappingRanges(min, max, true, it->second);
        for (auto itSecond = boundsInSecondaryIndex.first;
             itSecond != boundsInSecondaryIndex.second;
             ++itSecond) {
            shardIds->insert(itSecond->second->getShardId());

            if (shardIds->size() == _shardVersionSize) {
                // No need to iterate through the rest of the ranges, because we already know we
                // need to use all shards.
                return;
            }
        }
    }
}

std::pair<ChunkMapEX::const_iterator, ChunkMapEX::const_iterator>
ChunkManagerEX::_overlappingRanges(const BSONObj& min,
                                   const BSONObj& max,
                                   bool isMaxInclusive,
                                   std::shared_ptr<ChunkMapEX> chunkMap) const {
    const auto itMin = chunkMap->upper_bound(_extractKeyString(min));
    const auto itMax = [this, &max, isMaxInclusive, chunkMap]() {
        auto it = isMaxInclusive ? chunkMap->upper_bound(_extractKeyString(max))
                                 : chunkMap->lower_bound(_extractKeyString(max));
        return it == chunkMap->end() ? it : ++it;
    }();

    return {itMin, itMax};
}


std::pair<TopIndexMap::const_iterator, TopIndexMap::const_iterator>
ChunkManagerEX::_overlappingTopRanges(const mongo::BSONObj& min,
                                      const mongo::BSONObj& max,
                                      bool isMaxInclusive) const {

    const auto itMin = _topIndexMap.upper_bound(_extractKeyString(min));
    const auto itMax = [this, &max, isMaxInclusive]() {
        auto it = isMaxInclusive ? _topIndexMap.upper_bound(_extractKeyString(max))
                                 : _topIndexMap.lower_bound(_extractKeyString(max));
        return it == _topIndexMap.end() ? it : ++it;
    }();

    return {itMin, itMax};
}


void ChunkManagerEX::getAllShardIds(std::set<ShardId>* all) const {
    std::transform(_shardVersions.begin(),
                   _shardVersions.end(),
                   std::inserter(*all, all->begin()),
                   [](const ShardVersionMapEX::value_type& pair) { return pair.first; });
}

IndexBounds ChunkManagerEX::getIndexBoundsForQuery(const BSONObj& key,
                                                   const CanonicalQuery& canonicalQuery) {
    // $text is not allowed in planning since we don't have text index on mongos.
    // TODO: Treat $text query as a no-op in planning on mongos. So with shard key {a: 1},
    //       the query { a: 2, $text: { ... } } will only target to {a: 2}.
    if (QueryPlannerCommon::hasNode(canonicalQuery.root(), MatchExpression::TEXT)) {
        IndexBounds bounds;
        IndexBoundsBuilder::allValuesBounds(key, &bounds);  // [minKey, maxKey]
        return bounds;
    }

    // Consider shard key as an index
    std::string accessMethod = IndexNames::findPluginName(key);
    dassert(accessMethod == IndexNames::BTREE || accessMethod == IndexNames::HASHED);

    // Use query framework to generate index bounds
    QueryPlannerParams plannerParams;
    // Must use "shard key" index
    plannerParams.options = QueryPlannerParams::NO_TABLE_SCAN;
    IndexEntry indexEntry(key,
                          accessMethod,
                          false /* multiKey */,
                          MultikeyPaths{},
                          false /* sparse */,
                          false /* unique */,
                          "shardkey",
                          NULL /* filterExpr */,
                          BSONObj(),
                          NULL /* collator */);
    plannerParams.indices.push_back(indexEntry);

    OwnedPointerVector<QuerySolution> solutions;
    Status status = QueryPlanner::plan(canonicalQuery, plannerParams, &solutions.mutableVector());
    uassert(status.code(), status.reason(), status.isOK());

    IndexBounds bounds;

    for (std::vector<QuerySolution*>::const_iterator it = solutions.begin();
         bounds.size() == 0 && it != solutions.end();
         it++) {
        // Try next solution if we failed to generate index bounds, i.e. bounds.size() == 0
        bounds = collapseQuerySolution((*it)->root.get());
    }

    if (bounds.size() == 0) {
        // We cannot plan the query without collection scan, so target to all shards.
        IndexBoundsBuilder::allValuesBounds(key, &bounds);  // [minKey, maxKey]
    }
    return bounds;
}

IndexBounds ChunkManagerEX::collapseQuerySolution(const QuerySolutionNode* node) {
    if (node->children.empty()) {
        invariant(node->getType() == STAGE_IXSCAN);

        const IndexScanNode* ixNode = static_cast<const IndexScanNode*>(node);
        return ixNode->bounds;
    }

    if (node->children.size() == 1) {
        // e.g. FETCH -> IXSCAN
        return collapseQuerySolution(node->children.front());
    }

    // children.size() > 1, assert it's OR / SORT_MERGE.
    if (node->getType() != STAGE_OR && node->getType() != STAGE_SORT_MERGE) {
        // Unexpected node. We should never reach here.
        error() << "could not generate index bounds on query solution tree: "
                << redact(node->toString());
        dassert(false);  // We'd like to know this error in testing.

        // Bail out with all shards in production, since this isn't a fatal error.
        return IndexBounds();
    }

    IndexBounds bounds;

    for (std::vector<QuerySolutionNode*>::const_iterator it = node->children.begin();
         it != node->children.end();
         it++) {
        // The first branch under OR
        if (it == node->children.begin()) {
            invariant(bounds.size() == 0);
            bounds = collapseQuerySolution(*it);
            if (bounds.size() == 0) {  // Got unexpected node in query solution tree
                return IndexBounds();
            }
            continue;
        }

        IndexBounds childBounds = collapseQuerySolution(*it);
        if (childBounds.size() == 0) {
            // Got unexpected node in query solution tree
            return IndexBounds();
        }

        invariant(childBounds.size() == bounds.size());

        for (size_t i = 0; i < bounds.size(); i++) {
            bounds.fields[i].intervals.insert(bounds.fields[i].intervals.end(),
                                              childBounds.fields[i].intervals.begin(),
                                              childBounds.fields[i].intervals.end());
        }
    }

    for (size_t i = 0; i < bounds.size(); i++) {
        IndexBoundsBuilder::unionize(&bounds.fields[i]);
    }

    return bounds;
}

bool ChunkManagerEX::compatibleWith(const ChunkManagerEX& other, const ShardId& shardName) const {
    // Return true if the shard version is the same in the two chunk managers
    // TODO: This doesn't need to be so strong, just major vs
    return other.getVersion(shardName).equals(getVersion(shardName));
}


ChunkVersion ChunkManagerEX::getVersion(const ShardId& shardName) const {
    auto it = _shardVersions.find(shardName);
    if (it == _shardVersions.end()) {
        // Shards without explicitly tracked shard versions (meaning they have no chunks) always
        // have a version of (0, 0, epoch)
        log() << "getVersion by shardname = " << shardName << ",not find";
        
        return ChunkVersion(0, 0, _collectionVersion.epoch());
    }
    return it->second;
}

std::string ChunkManagerEX::toString() const {
    StringBuilder sb;
    sb << "ChunkManager: " << _nss.ns() << " key: " << _shardKeyPattern.toString() << '\n';
    sb << "Chunks:\n";
    for(auto& topIndex: _topIndexMap){
        for(auto& itr: *topIndex.second){
            sb << "\t" << itr.second->toString() << '\n';
        }
    }

    sb << "Shard versions:\n";
    for (const auto& entry : _shardVersions) {
        sb << "\t" << entry.first << ": " << entry.second.toString() << '\n';
    }
    log() << sb.str();
    return sb.str();
}

//按start和limit获取内存中的chunks信息，用来校验mongos的内存路由和configsvr中是否一致，内部使用
std::shared_ptr<IteratorChunks> ChunkManagerEX::iteratorChunks(int start, int limit, bool print) const {
    //打印出路由信息，调试使用
    if(print){
        toString();
    }
    
    std::shared_ptr<IteratorChunks> result = std::make_shared<IteratorChunks>();
    int total = 0;
    int pre = 0;
    int needAdvance = 0;
    bool hasFindStartIndex = false;
    bool needBreak = false;
    for (auto& it : _topIndexMap) {
        auto itChunks = it.second->begin();
        if (!hasFindStartIndex) {
            total += it.second->size();
            pre = total - it.second->size();
            if (start > total) {
                continue;
            } else {
                hasFindStartIndex = true;
                needAdvance = start - pre;
                std::advance(itChunks, needAdvance);//指定的chunkMap上前进needAdvance
            }
        }

        for (; itChunks != it.second->end(); ++itChunks) {
            if (limit <= 0) {
                needBreak = true;
                break;
            }

            BSONObjBuilder bson;
            bson.append("min", itChunks->second->getMin());
            bson.append("max", itChunks->second->getMax());
            bson.append("shard", itChunks->second->getShardId().toString());
            auto obj = bson.obj();
            // log()<<"bson:"<<obj.toString();
            result->bson.append(obj);
            limit--;
        }

        if(needBreak){
            break;
        }
    }
    //总数
    int total_chunks = 0;
    for(auto& itStatic : _topIndexMap){
        total_chunks += itStatic.second->size();
    }
    result->chunksSize = total_chunks;

    return result;
}


ShardVersionMapEX ChunkManagerEX::_constructShardVersionMap(const OID& epoch,
                                                            const ChunkMapEX& chunkMap,
                                                            Ordering shardKeyOrdering) {

    ShardVersionMapEX shardVersions;
    Timer timer;
    int build_cnt = 0;
    boost::optional<BSONObj> firstMin = boost::none;
    boost::optional<BSONObj> lastMax = boost::none;

    log() << "chunkMap  size = " << chunkMap.size();
    ChunkMapEX::const_iterator current = chunkMap.cbegin();

    while (current != chunkMap.cend()) {
        build_cnt++;
        const auto& firstChunkInRange = current->second;

        // Tracks the max shard version for the shard on which the current range will reside

        auto shardVersionIt = shardVersions.find(firstChunkInRange->getShardId());
        if (shardVersionIt == shardVersions.end()) {
            log() << "push shardid=" << firstChunkInRange->getShardId() << " to shardVersions.";
            shardVersionIt =
                shardVersions.emplace(firstChunkInRange->getShardId(), ChunkVersion(0, 0, epoch))
                    .first;
        }

        auto& maxShardVersion = shardVersionIt->second;
        //找到下一个遍历的current，找寻条件为，下一个shardid！= currnet.shardid
        //如果shardid == currnet.shardid，更新maxShardVersion，保证shardVersions中每个shard的version是最新的chunks的version
        current = std::find_if(
            current,
            chunkMap.cend(),
            [&firstChunkInRange, &maxShardVersion](const ChunkMapEX::value_type& chunkMapEntry) {
                const auto& currentChunk = chunkMapEntry.second;

                if (currentChunk->getShardId() != firstChunkInRange->getShardId())
                    return true;

                if (currentChunk->getLastmod() > maxShardVersion)
                    maxShardVersion = currentChunk->getLastmod();

                return false;
            });

        const auto rangeLast = std::prev(current);

        const BSONObj rangeMin = firstChunkInRange->getMin();
        const BSONObj rangeMax = rangeLast->second->getMax();
        if (!firstMin)
            firstMin = rangeMin;

        lastMax = rangeMax;
    }
    log() << "build _constructShardVersionMap time=" << timer.millis()
          << "ms,build cnt=" << build_cnt;
    if (!chunkMap.empty()) {
        invariant(!shardVersions.empty());
        invariant(firstMin.is_initialized());
        invariant(lastMax.is_initialized());

        checkAllElementsAreOfType(MinKey, firstMin.get());
        checkAllElementsAreOfType(MaxKey, lastMax.get());
    }

    return shardVersions;
}


std::string ChunkManagerEX::_extractKeyString(const BSONObj& shardKeyValue) const {
    return extractKeyStringInternalWithLock(shardKeyValue, _shardKeyOrdering);
}

std::shared_ptr<ChunkManagerEX> ChunkManagerEX::makeNew(
    NamespaceString nss,
    KeyPattern shardKeyPattern,
    std::unique_ptr<CollatorInterface> defaultCollator,
    bool unique,
    OID epoch,
    const std::vector<ChunkType>& chunks) {
    log() << "chunk manager ex make new. chunks.size =" << chunks.size();
    ChunkVersion chunk(0, 0, epoch);
    auto ptr = std::make_shared<ChunkManagerEX>(std::move(nss),
                                                std::move(shardKeyPattern),
                                                std::move(defaultCollator),
                                                std::move(unique),
                                                std::move(chunk));
    return ptr->build(chunks);
}

//只有从无到有的构建chunkmap，这里的changeChunks应该是collection的全量chunks
//1.第一次collection获取路由
//2.collection被删，重新构建同名collection
std::shared_ptr<ChunkManagerEX> ChunkManagerEX::build(const std::vector<ChunkType>& changedChunks) {
    const auto startingCollectionVersion = getVersion();
    Timer timer;
    ChunkMapEX chunkMap;//临时chunkMap，因为changedChunks不是按key排序而是按lastmond，所以需要这个数据结果将chunks排序然后切割到_topIndexMap

    ChunkVersion collectionVersion = startingCollectionVersion;
    for (const auto& chunk : changedChunks) {
        const auto& chunkVersion = chunk.getVersion();

        uassert(ErrorCodes::ConflictingOperationInProgress,
                str::stream() << "Chunk " << chunk.genID(getns(), chunk.getMin())
                              << " has epoch different from that of the collection "
                              << chunkVersion.epoch(),
                collectionVersion.epoch() == chunkVersion.epoch());

        // Chunks must always come in incrementally sorted order
        invariant(chunkVersion >= collectionVersion);
        collectionVersion = chunkVersion;

        const auto chunkMinKeyString = _extractKeyString(chunk.getMin());
        const auto chunkMaxKeyString = _extractKeyString(chunk.getMax());

        // Returns the first chunk with a max key that is > min - implies that the chunk overlaps
        // min
        const auto low = chunkMap.upper_bound(chunkMinKeyString);

        // Returns the first chunk with a max key that is > max - implies that the next chunk cannot
        // not overlap max
        const auto high = chunkMap.upper_bound(chunkMaxKeyString);

        // Erase all chunks from the map, which overlap the chunk we got from the persistent store
        chunkMap.erase(low, high);

        // Insert only the chunk itself
        chunkMap.insert(std::make_pair(chunkMaxKeyString, std::make_shared<Chunk>(chunk)));
    }

    //构建_shardVdersionMap
    _shardVersions =
        _constructShardVersionMap(_collectionVersion.epoch(), chunkMap, _shardKeyOrdering);
    log() << "_shardVersions size = " << _shardVersions.size();

    std::shared_ptr<ChunkMapEX> chunksSecondary = std::make_shared<ChunkMapEX>();
    int si = _maxSizeSingleChunksMap;
    //_topIndexMap第一个分片可能不满_maxSizeSingleChunksMap.因为要用max做key，所以逆序遍历chunkMap
    for (auto it = chunkMap.rbegin(); it != chunkMap.rend(); ++it) {
        const auto chunkMaxKeyString = _extractKeyString(it->second->getMax());
        if (si == 0) {
            si = _maxSizeSingleChunksMap;  //填满了一个_maxSizeSingleChunksMap，开启新的
        }

        if (si == _maxSizeSingleChunksMap) {
            _topIndexMap[chunkMaxKeyString] = std::make_shared<ChunkMapEX>();
            chunksSecondary = _topIndexMap[chunkMaxKeyString];
        }

        chunksSecondary->insert(make_pair(chunkMaxKeyString, it->second));
        si--;
    }

    _collectionVersion = collectionVersion;

    log() << "topIndexMap size = " << _topIndexMap.size()
          << ",collectionVersion = " << _collectionVersion.toString();
    return shared_from_this();
}


std::shared_ptr<ChunkManagerEX> ChunkManagerEX::copyAndUpdate(
    std::shared_ptr<ChunkManagerEX> other,
    NamespaceString nss,
    KeyPattern shardKeyPattern,
    std::unique_ptr<CollatorInterface> defaultCollator,
    bool unique,
    OID epoch,
    const std::vector<ChunkType>& chunks) {
    log() << "chunk manager with  copy and update. chunks.size =" << chunks.size();
    auto ptr = std::make_shared<ChunkManagerEX>(other,
                                                std::move(nss),
                                                std::move(shardKeyPattern),
                                                std::move(defaultCollator),
                                                std::move(unique));
    return ptr->makeUpdated(chunks);
}


std::shared_ptr<ChunkManagerEX> ChunkManagerEX::makeUpdated(
    const std::vector<ChunkType>& changedChunks) {
    Timer timer;
    ChunkMapEX chunkMap;

    ChunkVersion collectionVersion = getVersion();
    std::map<std::string, std::shared_ptr<ChunkMapEX>> changeChunksMap;//记录所有因为changeChunks需要变更的ChunkMaps
    for (const auto& chunk : changedChunks) {
        const auto& chunkVersion = chunk.getVersion();

        uassert(ErrorCodes::ConflictingOperationInProgress,
                str::stream() << "Chunk " << chunk.genID(getns(), chunk.getMin())
                              << " has epoch different from that of the collection "
                              << chunkVersion.epoch(),
                collectionVersion.epoch() == chunkVersion.epoch());

        // Chunks must always come in incrementally sorted order
        invariant(chunkVersion >= collectionVersion);
        collectionVersion = chunkVersion;

        const auto chunkMinKeyString = _extractKeyString(chunk.getMin());
        const auto chunkMaxKeyString = _extractKeyString(chunk.getMax());

        auto topIndex = _topIndexMap.lower_bound(chunkMaxKeyString);//topIndxeMap中第一个不小于chunkMaxKeyString的

        if (topIndex == _topIndexMap.end()) {
            log() << __FILE__ << ":" << __LINE__
                  << " can't find the key chunkMinKeyString = " << chunkMinKeyString
                  << ",chunkMaxKeyString = " << chunkMaxKeyString;
            invariant(false);  //找不到覆盖key的chunk，程序直接异常退出
        }

        if (changeChunksMap.find(topIndex->first) == changeChunksMap.end()) {
            //copy 原chunkManager中这个changeChunksMap
            ChunkMapEX copyMap = *(topIndex->second.get());
            auto copyMapPtr = std::make_shared<ChunkMapEX>(copyMap);
            log() << "copy size = " << copyMapPtr->size();
            changeChunksMap.insert(std::make_pair(topIndex->first, copyMapPtr));
        }

        auto itUpdate = changeChunksMap.find(topIndex->first);
        // Returns the first chunk with a max key that is > min - implies that the chunk overlaps
        // min
        const auto low = itUpdate->second->upper_bound(chunkMinKeyString);

        // Returns the first chunk with a max key that is > max - implies that the next chunk cannot
        // not overlap max
        const auto high = itUpdate->second->upper_bound(chunkMaxKeyString);

        // Erase all chunks from the map, which overlap the chunk we got from the persistent store
        itUpdate->second->erase(low, high);

        // Insert only the chunk itself
        itUpdate->second->insert(std::make_pair(chunkMaxKeyString, std::make_shared<Chunk>(chunk)));


        auto shardVersionIt = _shardVersions.find(chunk.getShard());
        if (shardVersionIt == _shardVersions.end()) {
            log() << "push shardid=" << chunk.getShard() << " to shardVersions. UpdateChunksMap";
            _shardVersions.emplace(chunk.getShard(), chunk.getVersion());
        } else if (chunk.getVersion() > shardVersionIt->second) {
            shardVersionIt->second = chunk.getVersion();
        }
    }
    int change = 0;
    for (auto& it : changeChunksMap) {
        auto itIndex = _topIndexMap.find(it.first);
        if (itIndex == _topIndexMap.end()) {
            log() << __FILE__ << ":" << __LINE__
                  << " can't find the key  = " << it.first;
            invariant(false);  //找不到覆盖key的chunk，程序直接异常退出
        }

        itIndex->second = it.second;//替换chunksMap
        change++;
    }
    log() << "change cnt = " << change;
    
    _collectionVersion = collectionVersion;
    log() << "makeUpdated time=" << timer.millis() << "ms";
    return shared_from_this();
}


ChunkVersion ChunkManagerEX::getVersion() const {
    log() << "chunk_manager_version" << _collectionVersion.toString();
    return _collectionVersion;
}
}  // namespace mongo
