#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/db/stats/detail_counter.h"
#include "mongo/util/log.h"

namespace mongo {
    DetailCmdCounterContainer G_D_C_CONTAINER;

    void DetailCmdCounterContainer::append(const BaseDetailCmdCounter* tmp) {
        if (tmp == nullptr || tmp->getName().empty()) {
            return;
        }

        stdx::lock_guard<mutex> lock(_mapLock);
        auto iter = _cmdMap.find(tmp->getName());

        if (iter != _cmdMap.end()) {
            LOG(logger::LogSeverity::Warning()) << "name:" << tmp->getName() << " has existed";
        } 

        _cmdMap[tmp->getName()] = tmp;
    }

    void DetailCmdCounterContainer::buildObj(BSONObjBuilder& builder) {
        stdx::lock_guard<mutex> lock(_mapLock);
        for(const auto& item : _cmdMap) {
            builder.append(item.first, item.second->getObj());
        }
    }

    const BaseDetailCmdCounter* DetailCmdCounterContainer::get(string name) {
        if (name.empty()) {
            return nullptr;
        }

        stdx::lock_guard<mutex> lock(_mapLock);
        auto iter = _cmdMap.find(name);

        if (iter == _cmdMap.end()) {
            return nullptr;
        } 
        return iter->second;
    }

    uint32_t DetailCmdCounterContainer::size() {
        stdx::lock_guard<mutex> lock(_mapLock);
        return _cmdMap.size();
    }

    void DetailCmdCounterContainer::remove(string name) {
        if (name.empty()) {
            return;
        }

        stdx::lock_guard<mutex> lock(_mapLock);
        _cmdMap.erase(name);
    }

}  // namespace mongo