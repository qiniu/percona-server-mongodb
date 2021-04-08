#pragma once

#include "mongo/platform/atomic_word.h"
#include "mongo/platform/basic.h"
#include "mongo/db/jsobj.h"

namespace mongo {

class ApCounter {
    public:
    ApCounter();
    void gotReadNotAp();
    void gotReadAp();
    void gotErrorGetApExecutorPool();

    BSONObj getObj() const;

    const AtomicUInt32* getReadNotAp() const{
        return &_readNotAp;
    }

    const AtomicUInt32* getReadAp() const {
        return &_readAp;
    }

    const AtomicUInt32* getErrorGetApExecutorPool() const {
        return &_readApExecutorPoolError;
    }

    private:
    
        void _checkWrap();
        AtomicUInt32 _readNotAp;
        AtomicUInt32 _readAp;
        AtomicUInt32 _readApExecutorPoolError;
};

extern ApCounter globalApCounter;
}