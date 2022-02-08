#pragma once

#include <atomic>
#include <set>
#include <string>

#include "mongo/stdx/mutex.h"
#include "mongo/util/background.h"
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
private:
    std::shared_ptr<std::set<std::string>> _sharedCollections;
    // 用来保护 map
    stdx::mutex _mutex;
};

extern RefreshMetainfoJob refreshMetaInfoJob;

}  // namespace mongo