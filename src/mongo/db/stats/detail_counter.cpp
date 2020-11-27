#include "mongo/db/stats/detail_counter.h"

namespace mongo {
    DetailCmdCounterContainer G_D_C_CONTAINER;

    void DetailCmdCounterContainer::append(const BaseDetailCmdCounter* tmp) {
        if (tmp == nullptr) {
            return;
        }

        stdx::lock_guard<mutex> lock(_mapLock);
        _cmdMap[tmp->getName()] = tmp;
    }
    void DetailCmdCounterContainer::buildObj(BSONObjBuilder& builder) {
        stdx::lock_guard<mutex> lock(_mapLock);
        for(const auto& item : _cmdMap) {
            builder.append(item.first, item.second->getObj());
        }
    }

}  // namespace mongo