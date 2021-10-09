#pragma once

#include "mongo/platform/atomic_word.h"
#include "mongo/platform/basic.h"
#include "mongo/db/jsobj.h"
#include <mutex>
#include <unordered_map>
#include <string>

namespace mongo {
    class WatchdogElement {
        public: 
        virtual BSONObj getObj() const = 0;
    };

    class WatchdogCounter {
        public:
            void registerElement(const std::string& name, WatchdogElement* element);
            BSONObj getObj() const;
        private:
            mutable std::mutex _lock;
            std::unordered_map<std::string, WatchdogElement* > _monitor;
    };
    extern WatchdogCounter globalWatchdogCounter;
}