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
#include "mongo/db/operation_context.h"

namespace mongo {
using std::map;
using std::set;
using std::string;

constexpr auto kRefreshMetainfoJobName = "RefreshMetainfoJob";
RefreshMetainfoJob refreshMetaInfoJob;

std::string RefreshMetainfoJob::name() const {
    return kRefreshMetainfoJobName;
}


StatusWith<map<string, set<string>>> RefreshMetainfoJob::getShardingCollections(
    OperationContext* txn) {
    map<string, set<string>> collectionRes;
    const auto catalogClient = Grid::get(txn)->catalogClient(txn);
    invariant(catalogClient);

    collectionRes["drop"] = set<string>();
    collectionRes["undrop"] = set<string>();

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
            catalogClient->getCollections(txn, nullptr, &collections, &collLoadConfigOptime);
        if (!status.isOK()) {
            log() << "[MongoStat] RefreshMetainfoJob::_getShardingCollections() get collections "
                     "failed: "
                  << status;
            return StatusWith<map<string, set<string>>>(status);
        }

        for (const auto& coll : collections) {
            if (coll.getDropped()) {
                collectionRes["drop"].insert(coll.getNs().ns());
            } else {
                collectionRes["undrop"].insert(coll.getNs().ns());
            }
        }
    } while (false);
    return StatusWith<map<string, set<string>>>(collectionRes);
}

void RefreshMetainfoJob::run() {
    Client::initThread(name().c_str());
    auto txn = cc().makeOperationContext();

    while (!inShutdown()) {
        const auto catalogClient = Grid::get(txn.get())->catalogClient(txn.get());
        if (catalogClient == nullptr) {
            LOG(1) << "RefreshMetainfoJob::run() catalogClient is null";
            sleepsecs(60);
            continue;
        }

        do {
            auto status = getShardingCollections(txn.get());
            if (!status.isOK()) {
                break;
            }

            std::shared_ptr<std::set<string>> tmp = std::make_shared<std::set<string>>();
            map<string, set<string>> v = status.getValue();
            for (const auto& item : v) {
                for (const auto& coll : item.second) {
                    tmp->insert(coll);
                }
            }

            Timer t;
            ON_BLOCK_EXIT([&t] {
                auto cs = t.millis();
                if (cs > 10) {
                    log() << "[MongoStat] RefreshMetainfoJob::getShardingCollections() lock "
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