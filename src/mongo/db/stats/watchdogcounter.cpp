#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "watchdogcounter.h"
#include "mongo/util/log.h"

namespace mongo {
    void WatchdogCounter::registerElement(const std::string& name, WatchdogElement* element) {
        invariant(!name.isEmpty());
        invariant(element);

        std::lock_guard<std::mutex> guard(_lock);
        if (this->_monitor.find(name) == this->_monitor.end()) {
            this->_monitor[name] = element;
        } else {
            log() << "name: " << name << " had existed";
        }
    }

    BSONObj WatchdogCounter::getObj() {
        BSONObjBuilder b;

        std::lock_guard<std::mutex> guard(_lock);
        for (const auto& element : this->_monitor) {
            b.append(element.first(), element.second()->getObj());
        }
        return b.obj();
    }

    WatchdogCounter globalWatchdogCounter;
}