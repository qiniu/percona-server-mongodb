/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#pragma once

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <map>
#include <set>
#include <string>

#include "chunk_manager.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/s/chunk.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/util/concurrency/ticketholder.h"
#include <vector>
//#include "chunk_manager.h"

namespace mongo {

// Ordered map from the max for each chunk to an entry describing the chunk

using ChunkMapEX = std::map<std::string, std::shared_ptr<Chunk>>;

// Map from a shard is to the max chunk version on that shard
using ShardVersionMapEX = std::map<ShardId, ChunkVersion>;

using TopIndexMap = std::map<std::string, std::shared_ptr<ChunkMapEX>>;


class CanonicalQuery;
struct QuerySolutionNode;
class OperationContext;

struct IteratorChunks {
    int chunksSize;
    StringBuilder info;
    BSONArrayBuilder bson;
    bool hashErr;
    std::string errmsg;
};

struct ShardIDAndVersion {
    ShardId shardid;
    ChunkVersion version;
};

struct ChunkAndShardVersion {
    std::shared_ptr<Chunk> chunk;
    ChunkVersion version;
};

class ChunkManagerEX : public std::enable_shared_from_this<ChunkManagerEX> {
    MONGO_DISALLOW_COPYING(ChunkManagerEX);


public:
    ChunkManagerEX(NamespaceString nss,
                   KeyPattern shardKeyPattern,
                   std::unique_ptr<CollatorInterface> defaultCollator,
                   bool unique,
                   ChunkVersion collectionVersion);

    ChunkManagerEX(std::shared_ptr<ChunkManagerEX> other,
                   NamespaceString nss,
                   KeyPattern shardKeyPattern,
                   std::unique_ptr<CollatorInterface> defaultCollator,
                   bool unique
                   );

    ~ChunkManagerEX();


    /**
     * Makes an instance with a routing table for collection "nss", sharded on
     * "shardKeyPattern".
     *
     * "defaultCollator" is the default collation for the collection, "unique" indicates whether
     * or not the shard key for each document will be globally unique, and "epoch" is the globally
     * unique identifier for this version of the collection.
     *
     * The "chunks" vector must contain the chunk routing information sorted in ascending order by
     * chunk version, and adhere to the requirements of the routing table update algorithm.
     */
    static std::shared_ptr<ChunkManagerEX> makeNew(
        NamespaceString nss,
        KeyPattern shardKeyPattern,
        std::unique_ptr<CollatorInterface> defaultCollator,
        bool unique,
        OID epoch,
        const std::vector<ChunkType>& chunks);

    static std::shared_ptr<ChunkManagerEX> copyAndUpdate(
        std::shared_ptr<ChunkManagerEX> other,
        NamespaceString nss,
        KeyPattern shardKeyPattern,
        std::unique_ptr<CollatorInterface> defaultCollator,
        bool unique,
        OID epoch,
        const std::vector<ChunkType>& chunks);

    /**
     * Constructs a new instance with a routing table updated according to the changes described
     * in "changedChunks".
     *
     * The changes in "changedChunks" must be sorted in ascending order by chunk version, and adhere
     * to the requirements of the routing table update algorithm.
     */
    std::shared_ptr<ChunkManagerEX> makeUpdated(const std::vector<ChunkType>& changedChunks);


    std::shared_ptr<ChunkManagerEX> build(const std::vector<ChunkType>& changedChunks);


    /**
     * Returns an increasing number of the reload sequence number of this chunk manager.
     */
    unsigned long long getSequenceNumber() const {
        return _sequenceNumber;
    }

    const std::string& getns() const {
        return _nss.ns();
    }

    const ShardKeyPattern& getShardKeyPattern() const {
        return _shardKeyPattern;
    }

    //文档排序规则，默认ascii，也可以用中文，创建collection时传入
    const CollatorInterface* getDefaultCollator() const {
        return _defaultCollator.get();
    }

    bool isUnique() const {
        return _unique;
    }

    ChunkVersion getVersion() const;

    ChunkVersion getVersion(const ShardId& shardId) const;

    //const ChunkMap& chunkMap() const;


    int numChunks() const;

    TopIndexMap getTopIndexMap() const;

    ShardVersionMapEX getShardVersionMap(){return _shardVersions;}


    /**
     * Given a shard key (or a prefix) that has been extracted from a document, returns the chunk
     * that contains that key.
     *
     * Example: findIntersectingChunk({a : hash('foo')}) locates the chunk for document
     *          {a: 'foo', b: 'bar'} if the shard key is {a : 'hashed'}.
     *
     * If 'collation' is empty, we use the collection default collation for targeting.
     *
     * Throws a DBException with the ShardKeyNotFound code if unable to target a single shard due to
     * collation or due to the key not matching the shard key pattern.
     */
    std::shared_ptr<Chunk> findIntersectingChunk(const BSONObj& shardKey,
                                                 const BSONObj& collation) const;

    /**
     * Same as findIntersectingChunk, but assumes the simple collation.
     */
    std::shared_ptr<Chunk> findIntersectingChunkWithSimpleCollation(const BSONObj& shardKey) const;

    std::shared_ptr<ChunkAndShardVersion> findIntersectingChunkAndShardVersionWithSimpleCollation(
        const BSONObj& shardKey) const;
    std::shared_ptr<ChunkAndShardVersion> findIntersectingChunkAndShardVersion(
        const BSONObj& shardKey, const BSONObj& collation) const;


    /**
     * Finds the shard IDs for a given filter and collation. If collation is empty, we use the
     * collection default collation for targeting.
     */
    void getShardIdsForQuery(OperationContext* txn,
                             const BSONObj& query,
                             const BSONObj& collation,
                             std::set<ShardId>* shardIds) const;

    /**
     * Returns all shard ids which contain chunks overlapping the range [min, max]. Please note the
     * inclusive bounds on both sides (SERVER-20768).
     */
    void getShardIdsForRange(const BSONObj& min,
                             const BSONObj& max,
                             std::set<ShardId>* shardIds) const;


    void getShardIdsAndVersionForRange(const BSONObj& min,
                                       const BSONObj& max,
                                       std::set<ShardIDAndVersion>* shardIdAndVersions) const;

    /**
     * Returns the ids of all shards on which the collection has any chunks.
     */
    void getAllShardIds(std::set<ShardId>* all) const;

    // Transforms query into bounds for each field in the shard key
    // for example :
    //   Key { a: 1, b: 1 },
    //   Query { a : { $gte : 1, $lt : 2 },
    //            b : { $gte : 3, $lt : 4 } }
    //   => Bounds { a : [1, 2), b : [3, 4) }
    static IndexBounds getIndexBoundsForQuery(const BSONObj& key,
                                              const CanonicalQuery& canonicalQuery);

    // Collapse query solution tree.
    //
    // If it has OR node, the result could be a superset of the index bounds generated.
    // Since to give a single IndexBounds, this gives the union of bounds on each field.
    // for example:
    //   OR: { a: (0, 1), b: (0, 1) },
    //       { a: (2, 3), b: (2, 3) }
    //   =>  { a: (0, 1), (2, 3), b: (0, 1), (2, 3) }
    static IndexBounds collapseQuerySolution(const QuerySolutionNode* node);

    /**
     * Returns true if, for this shard, the chunks are identical in both chunk managers
     */
    bool compatibleWith(const ChunkManagerEX& other, const ShardId& shard) const;

    bool compatibleWith(const ChunkVersion& otherVersion, const ShardId& shardName) const;

    std::string toString() const;

    //按start和limit获取内存中的chunks信息，用来校验mongos的内存路由和configsvr中是否一致，内部使用
    std::shared_ptr<IteratorChunks> iteratorChunks(int start, int limit) const;

private:
    friend class CollectionRoutingDataLoader;

    /**
     * Does a single pass over the chunkMap and constructs the ShardVersionMap object.
     */
    static ShardVersionMapEX _constructShardVersionMap(const OID& epoch,
                                                     const ChunkMapEX& chunkMap,
                                                     Ordering shardKeyOrdering);

    std::string _extractKeyString(const BSONObj& shardKeyValue) const;

    //  ChunkMap::const_iterator  _rangeMapUpperBound(const BSONObj& key) const;

    std::pair<TopIndexMap::const_iterator, TopIndexMap::const_iterator> _overlappingTopRanges(
        const BSONObj& min, const BSONObj& max, bool isMaxInclusive) const;

    std::pair<ChunkMapEX::const_iterator, ChunkMapEX::const_iterator> _overlappingRanges(
        const BSONObj& min,
        const BSONObj& max,
        bool isMaxInclusive,
        std::shared_ptr<ChunkMapEX> chunkMap) const;


    // The shard versioning mechanism hinges on keeping track of the number of times we reload
    // ChunkManagers.
    unsigned long long _sequenceNumber;

    // Namespace to which this routing information corresponds
    const NamespaceString _nss;

    // The key pattern used to shard the collection
    const ShardKeyPattern _shardKeyPattern;

    const Ordering _shardKeyOrdering;

    // Default collation to use for routing data queries for this collection
    const std::unique_ptr<CollatorInterface> _defaultCollator;

    // Whether the sharding key is unique
    const bool _unique;

    // Map from the max for each chunk to an entry describing the chunk. The union of all chunks'
    // ranges must cover the complete space from [MinKey, MaxKey).
    //ChunkMap _chunkMap;

    TopIndexMap _topIndexMap;

    ShardVersionMapEX _shardVersions;

    std::atomic<uint64_t> _shardVersionSize;

    bool _init;


    // Max version across all chunks
    ChunkVersion _collectionVersion;


    // Auto-split throttling state (state mutable by write commands)
    struct AutoSplitThrottle {
    public:
        AutoSplitThrottle() : _splitTickets(maxParallelSplits) {}

        TicketHolder _splitTickets;

        // Maximum number of parallel threads requesting a split
        static const int maxParallelSplits = 5;

    } _autoSplitThrottle;

    // This function needs to be able to access the auto-split throttle
    friend void updateChunkWriteStatsAndSplitIfNeeded(OperationContext*,
                                                      ChunkManagerEX*,
                                                      Chunk*,
                                                      long);

    mutable boost::shared_mutex _mutex;
};
}  // namespace mongo
