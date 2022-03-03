#pragma once

#include <atomic>
#include <set>
#include <string>
#include <map>

#include "mongo/stdx/mutex.h"
#include "mongo/db/server_options.h"
#include "mongo/util/background.h"
#include "mongo/base/status_with.h"
#include <utility>

namespace mongo {

/**
 * 目前只是为了同步configsvr中的collections的信息
 */
class RefreshMetainfoJob final : public BackgroundJob {
public:
    std::string name() const final;
    void run() final;
    std::shared_ptr<std::set<std::string>> getSharedCollections();
    // 用来输出化当前进程的路由信息
    void initShardingMetaInfos(OperationContext& opCtx, ClusterRole role);
private:
    StatusWith<std::map<std::string, std::set<string>>> _getShardingCollections(OperationContext& opCtx);

    std::shared_ptr<std::set<std::string>> _sharedCollections;
    // 用来保护 map
    stdx::mutex _mutex;
};

extern RefreshMetainfoJob refreshMetaInfoJob;

}  // namespace mongo