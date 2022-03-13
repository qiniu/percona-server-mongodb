#pragma once

#include <atomic>
#include <map>
#include <set>
#include <string>

#include "mongo/stdx/mutex.h"
#include "mongo/util/background.h"
#include <utility>

namespace mongo {

class ChunkVersion;
/**
 * secondary会根据心跳的版本信息来更新本身的路由信息，也进一步解决readonly的mongos路由刷新的问题;
 */
class RefreshSecondaryRoutingJob final : public BackgroundJob {
public:
    std::string name() const final;
    void run() final;
    void putTask(const std::string& ns, const std::shared_ptr<ChunkVersion>& version);
    void putClearTask(const std::string& ns);
private:
    std::map<std::string, std::shared_ptr<ChunkVersion>> _taskPool;
    std::set<std::string> _clearPool;
    // 用来保护 map
    stdx::mutex _mutex;
};

extern RefreshSecondaryRoutingJob refreshSecondaryRoutingJob;

}  // namespace mongo