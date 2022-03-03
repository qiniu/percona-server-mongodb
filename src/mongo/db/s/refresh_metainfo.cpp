#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/s/refresh_metainfo.h"
#include "mongo/db/client.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/grid.h"

#include "mongo/s/catalog/type_collection.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/timer.h"

namespace mongo {
using std::map;
using std::set;
using std::string;

constexpr auto kRefreshMetainfoJobName = "RefreshMetainfoJob";
RefreshMetainfoJob refreshMetaInfoJob;

std::string RefreshMetainfoJob::name() const {
    return kRefreshMetainfoJobName;
}

void RefreshMetainfoJob::initShardingMetaInfos(OperationContext& txn, ClusterRole role) {
    Timer t;
    ON_BLOCK_EXIT([&t] {
        LOG(1) << "RefreshMetainfoJob::initShardingMetaInfos() took " << t.millis() << "ms";
    });

    auto status = _getShardingCollections(txn);
    invariant(status.isOK());

    auto value = status.getValue();
    auto validCollections = value.at("undrop");

    log() << "[MongoStat] RefreshMetainfoJob::initShardingMetaInfos, validCollections size: "
          << validCollections.size() << ", role:" << role;
    auto catalogCache = Grid::get(txn.get())->catalogCache();
    invariant(catalogCache);
    for (const auto& collection : validCollections) {
        if (role == ClusterRole::None) {
            auto versionStatus = catalogCache->getShardedCollectionRoutingInfoWithRefresh(
                txn.get(), NamespaceString(collection));
            if (!versionStatus.isOK()) {
                log() << "[MongoStat] RefreshMetainfoJob::initShardingMetaInfos, "
                         "refreshMetadataNow failed, "
                      << "collection: " << collection << ", status: " << versionStatus;
            } else {
                auto cm = versionStatus.getValue().cm();
                if (cm) {
                    log() << "[MongoStat] RefreshMetainfoJob::initShardingMetaInfos, "
                             "refreshMetadataNow success, "
                          << "collection: " << collection << ", version: " << cm->getVersion();
                } else {
                    log() << "[MongoStat] RefreshMetainfoJob::initShardingMetaInfos, "
                             "refreshMetadataNow success, "
                          << "collection: " << collection << ", cm is null";
                }
            }
        } else if (role == ClusterRole::ShardServer) {
            auto shardingState = ShardingState::get(txn.get());
            invariant(shardingState);

            ChunkVersion version;
            auto versionStatus =
                shardingState->refreshMetadataNow(txn.get(), NamespaceString(collection), &version);

            if (!versionStatus.isOK()) {
                log() << "[MongoStat] RefreshMetainfoJob::initShardingMetaInfos, "
                         "refreshMetadataNow failed, "
                      << "collection: " << collection << ", status: " << versionStatus;
                invariant(versionStatus.isOK());
            } else {
                log() << "[MongoStat] RefreshMetainfoJob::initShardingMetaInfos, "
                         "refreshMetadataNow success, "
                      << "collection: " << collection << ", version: " << version;
            }
        } else {
            log() << "[MongoStat] RefreshMetainfoJob::initShardingMetaInfos, "
                     "unknown role: " << role;
        }
    }
}

StatusWith<map<string, set<string>>> RefreshMetainfoJob::_getShardingCollections(
    OperationContext& txn) {
    map<string, set<string>> collections;
    const auto catalogClient = Grid::get(txn.get())->catalogClient(txn.get());
    invariant(catalogClient);

    collections["drop"] = set<string>();
    collections["undrop"] = set<string>();

    do {
        // Load the sharded collections entries
        std::vector<CollectionType> collections;
        repl::OpTime collLoadConfigOptime;

        Timer t1;
        ON_BLOCK_EXIT([&t1] {
            auto cs = t1.millis();
            if (cs > 10) {
                log() << "[MongoStat] RefreshMetainfoJob::_getShardingCollections() get "
                         "collections  cost:"
                      << cs << "ms";
            }
        });
        Status status =
            catalogClient->getCollections(txn.get(), nullptr, &collections, &collLoadConfigOptime);
        if (!status.isOK()) {
            log() << "[MongoStat] RefreshMetainfoJob::_getShardingCollections() get collections "
                     "failed: "
                  << status;
            return StatusWith<map<string, set<string>>>(status);
        }

        for (const auto& coll : collections) {
            if (coll.getDropped()) {
                collections["drop"].insert(coll.getNs().ns());
            } else {
                collections["undrop"].insert(coll.getNs().ns());
            }
        }
    } while (false);
    return StatusWith<map<string, set<string>>>(collections);
}

void RefreshMetainfoJob::run() {
    Client::initThread(name().c_str());
    auto txnPtr = cc().makeOperationContext();
    OperationContext& opCtx = *txnPtr;

    while (!inShutdown()) {
        if (catalogClient == nullptr) {
            LOG(1) << "RefreshMetainfoJob::run() catalogClient is null";
            sleepsecs(60);
            continue;
        }

        do {
            auto status = _getShardingCollections(opCtx);
            if (!status.isOK()) {
                break;
            }

            std::shared_ptr<std::set<string>> tmp = std::make_shared<std::set<string>>();
            auto v = status.getValue();
            for (const auto& item : v) {
                for (const auto& coll : item.second) {
                    tmp->insert(coll);
                }
            }

            for (const auto& coll : *tmp) {
                log() << "[MongoStat] RefreshMetainfoJob::run() collection: " << coll;
            }

            Timer t;
            ON_BLOCK_EXIT([&t] {
                auto cs = t.millis();
                if (cs > 10) {
                    log() << "[MongoStat] RefreshMetainfoJob::_getShardingCollections() lock "
                             "cost:"
                          << cs << "ms";
                }
            });
            std::lock_guard<stdx::mutex> lk(_mutex);
            if (_sharedCollections == nullptr || tmp->size() >= _sharedCollections->size()) {
                _sharedCollections = tmp;
            }
        } while (false);

        auto waitSecs = 82800 + rand() % 3600;
        log() << "i will sleep " << waitSecs << " seconds";
        sleepsecs(waitSecs);
    }
}

std::shared_ptr<std::set<string>> RefreshMetainfoJob::getSharedCollections() {
    Timer t;
    ON_BLOCK_EXIT([&t] {
        auto cs = t.millis();
        if (cs > 10) {
            log() << "[MongoStat] RefreshMetainfoJob::getSharedCollections() lock cost:" << cs
                  << "ms";
        }
    });

    std::lock_guard<stdx::mutex> lk(_mutex);
    return _sharedCollections;
}
}  // namespace mongo