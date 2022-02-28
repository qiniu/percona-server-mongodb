#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/s/refresh_secondary_routing.h"
#include "mongo/db/client.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/service_context.h"
#include "mongo/db/stats/apcounter.h"
#include "mongo/s/chunk_version.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/timer.h"
#include "mongo/util/exit.h"

namespace mongo {
using std::map;
using std::shared_ptr;
using std::string;
using stdx::mutex;

constexpr auto kRefreshSecondaryRoutingJobName = "RefreshSecondaryRoutingJob";

RefreshSecondaryRoutingJob refreshSecondaryRoutingJob;

std::string RefreshSecondaryRoutingJob::name() const {
    return kRefreshSecondaryRoutingJobName;
}

void RefreshSecondaryRoutingJob::putTask(const string& ns,
                                         const shared_ptr<ChunkVersion>& version) {
    if (ns.empty() || !version) {
        log() << "RefreshSecondaryRoutingJob::putTask() ns or version is empty";
        return;
    }

    Timer t;
    ON_BLOCK_EXIT([&t] {
        auto cs = t.millis();
        if (cs > 10) {
            log() << "[MongoStat] RefreshSecondaryRoutingJob::putTask() cost:" << cs << "ms";
        }
    });

    /**
     * 能被更新的条件
     * 1. 如果当前任务map中没有这个ns的任务，就直接insert
     * 2. 如果更新的版本的epoch不一致，说明进行了删除coll的操作，那就直接更新
     * 3. 如果epoch相同，那就保留版本比较大的;
     */
    stdx::lock_guard<mutex> lk(_mutex);
    auto value = _taskPool.find(ns);
    if (value == _taskPool.end()) {
        log() << "[MongoStat] insert task ns:" << ns << " version:" << version->toString();
        _taskPool[ns] = version;
        return;
    } 

    if (!version->hasEqualEpoch(*(value->second))) {
        log() << "[MongoStat] update task ns:" << ns << " new version:" << version->toString() << " old version:" << value->second->toString() << ",because epoch is not equal";

        globalApCounter.gotEpochNotEqual();
        _taskPool[ns] = version;
        return;
    }

    if (*version > *(value->second)) {
        log() << "[MongoStat] [Compatible] update task ns:" << ns << " version:" << version->toString() << " old:" << value->second->toString();
        _taskPool[ns] = version;
    }
}

void RefreshSecondaryRoutingJob::run() {
    Client::initThread(name().c_str());
    auto txnPtr = cc().makeOperationContext();
    OperationContext& txn = *txnPtr;

    while (!inShutdown()) {
        auto shardingState = ShardingState::get(&txn);

        if (shardingState && shardingState->enabled()) {
            repl::ReplicationCoordinator* replCoord = repl::getGlobalReplicationCoordinator();

            //只有secondary才会进行更新
            if (replCoord && replCoord->isReplEnabled() &&
                replCoord->getMemberState().secondary()) {
                map<string, shared_ptr<ChunkVersion>> copyMap;
                {
                    Timer t;
                    ON_BLOCK_EXIT([&t] {
                        auto cs = t.millis();
                        if (cs > 10) {
                            log() << "[MongoStat]RefreshSecondaryRoutingJob::run() copyMap took "
                                  << cs << "ms";
                        }
                    });

                    stdx::lock_guard<mutex> lk(_mutex);
                    copyMap = _taskPool;
                    _taskPool.clear();
                }

                {
                    Timer t;
                    ON_BLOCK_EXIT([&copyMap, &t] {
                        if (!copyMap.empty()) {
                            log() << "[MongoStat]RefreshSecondaryRoutingJob::run() task size:"
                                  << copyMap.size() << ",refresh routing took " << t.millis()
                                  << "ms";
                        }
                    });

                    for (auto& item : copyMap) {
                        auto& ns = item.first;
                        auto& version = item.second;

                        if (ns.empty()) {
                            continue;
                        }

                        log() << "[MongoStat]RefreshSecondaryRoutingJob::run() ns: " << ns
                              << " version: " << version->toString();
                        shardingState->onStaleShardVersion(&txn, NamespaceString(ns), *version);
                        globalApCounter.gotOnStaleShardVersion();
                    }
                }
                sleepmillis(500);
            } else {
                log() << "[MongoStat]RefreshSecondaryRoutingJob::run() not secondary, I wait 10 secondary";
                // 非secondary跳过
                sleepsecs(10);
            }
        } else {
            log() << "[MongoStat]RefreshSecondaryRoutingJob::run() shardingState->enabled() is false, I wait "
                     "10 secondary";
            sleepsecs(10);
        }
    }
}

}  // namespace mongo