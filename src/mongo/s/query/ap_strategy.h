#pragma once

#include "mongo/client/read_preference.h"

namespace mongo {
class ApStrategy {
    public:
        static bool useApTaskExecutorPool(ReadPreference readPref) {
            if (readPref == ReadPreference::SecondaryOnly || readPref == ReadPreference::SecondaryPreferred) {
                return true;
            }
            return false;
        }
};
}