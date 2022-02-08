#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/s/refresh_metainfo.h"
#include "mongo/s/grid.h"
#include "mongo/db/client.h"
#include "mongo/s/catalog/sharding_catalog_client.h"

#include "mongo/util/log.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/timer.h"
#include "mongo/util/exit.h"

namespace mongo {
    using std::set;
    using std::string;

    constexpr auto kRefreshMetainfoJobName = "RefreshMetainfoJob";
    RefreshMetainfoJob refreshMetaInfoJob;

    std::string RefreshMetainfoJob::name() const {
        return kRefreshMetainfoJobName;
    }

    void RefreshMetainfoJob::run() {
        Client::initThread(name().c_str());
        auto txnPtr = cc().makeOperationContext();
        OperationContext& opCtx = *txnPtr;

        while (!inShutdown()) {
            const auto catalogClient = Grid::get(&opCtx)->catalogClient(&opCtx);

            do {
                if (catalogClient == nullptr) {
                    log() << "RefreshMetainfoJob::run() catalogClient is null";
                    break;
                }

                // Load the sharded collections entries
                std::vector<CollectionType> collections;
                repl::OpTime collLoadConfigOptime;

                try {
                    Timer t1;
                    ON_BLOCK_EXIT([&t1] {
                        auto cs = t1.millis();
                        if (cs > 10) {
                            log() << "[MongoStat] RefreshMetainfoJob::run() get collections  cost:" << cs << "ms";
                        }
                    });
                    uassertStatusOK(catalogClient->getCollections(
                        &opCtx, nullptr, &collections, &collLoadConfigOptime));
                } catch (...) {
                    break;
                }

                std::shared_ptr<std::set<string>> tmp = std::make_shared<std::set<string>>(); 
                for (const auto& coll : collections) {
                    if (coll.getDropped()) {
                        log() << "coll:" << coll.getNs().ns() << " is dropped";
                    }
                    tmp->insert(coll.getNs().ns());
                }

                {
                    Timer t;
                    ON_BLOCK_EXIT([&t] {
                        auto cs = t.millis();
                        if (cs > 10) {
                            log() << "[MongoStat] RefreshMetainfoJob::run() lock cost:" << cs << "ms";
                        }
                    });
                    std::lock_guard<stdx::mutex> lk(_mutex);
                    if (_sharedCollections == nullptr || tmp->size() >= _sharedCollections->size()) {
                        _sharedCollections = tmp;
                    }
                }
            } while (false);
            auto waitSecs = rand() % 60;
            log() << "i will sleep " << waitSecs << " seconds";
            sleepsecs(waitSecs);
        }
    }

    std::shared_ptr<std::set<string>> RefreshMetainfoJob::getSharedCollections() {
            Timer t;
            ON_BLOCK_EXIT([&t] {
                auto cs = t.millis();
                if (cs > 10) {
                    log() << "[MongoStat] RefreshMetainfoJob::getSharedCollections() lock cost:" << cs << "ms";
                }
            });

            std::lock_guard<stdx::mutex> lk(_mutex);
            for(const auto& item : *_sharedCollections) {
                log() << "collection:" << item;
            } 

            return _sharedCollections;
    }
}