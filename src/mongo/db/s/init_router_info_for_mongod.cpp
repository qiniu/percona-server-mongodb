#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/s/refresh_metainfo.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/s/chunk_version.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/timer.h"
#include "mongo/db/operation_context.h"
#include "mongo/s/grid.h"
#include <map>
#include <string>
#include <set>

namespace mongo {
    using std::map;
    using std::string;
    using std::set;

void initShardingMetaInfos(OperationContext* txn, ClusterRole role) {
    Timer t;
    ON_BLOCK_EXIT([&t] {
        log() << "initShardingMetaInfos() is complete, took " << t.millis() << "ms";
    });

    auto status = refreshMetaInfoJob.getShardingCollectionsForConfigsrv(txn);
    invariant(status.isOK());

    auto value = status.getValue();
    auto validCollections = value.at("undrop");

    log() << "[MongoStat] initShardingMetaInfos, validCollections size: "
          << validCollections.size() << ", role:" << static_cast<int>(role);
    auto catalogCache = Grid::get(txn)->catalogCache();
    invariant(catalogCache);
    for (const auto& collection : validCollections) {
        if (role == ClusterRole::ShardServer) {
            auto shardingState = ShardingState::get(txn);
            invariant(shardingState);

            ChunkVersion version;
            auto versionStatus =
                shardingState->refreshMetadataNow(txn, NamespaceString(collection), &version);

            if (!versionStatus.isOK()) {
                log() << "[MongoStat] initShardingMetaInfos, "
                         "refreshMetadataNow failed, "
                      << "collection: " << collection << ", status: " << versionStatus;
                invariant(versionStatus.isOK());
            } else {
                log() << "[MongoStat] initShardingMetaInfos, "
                         "refreshMetadataNow success, "
                      << "collection: " << collection << ", version: " << version;
            }
        } else {
            log() << "[MongoStat] initShardingMetaInfos, "
                     "unknown role: " << static_cast<int>(role);
        }
    }
}

}