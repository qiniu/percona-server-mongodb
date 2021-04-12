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
    void gotReadSlowLog();
    void gotWriteSlowLog();
    void gotFamSlowLog();
    void gotCmdSlowLog();

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

    const AtomicUInt32* getReadSlowLog() const {
        return &_readSlowLog;
    }
    const AtomicUInt32* getWriteSlowLog() const {
        return &_writeSlowLog;
    }
    const AtomicUInt32* getFamSlowLog() const {
        return &_famSlowLog;
    }
    const AtomicUInt32* getCmdSlowLog() const {
        return &_cmdSlowLog;
    }

    private:
    
        void _checkWrap();
        AtomicUInt32 _readNotAp;
        AtomicUInt32 _readAp;
        AtomicUInt32 _readApExecutorPoolError;
        AtomicUInt32 _readSlowLog;
        AtomicUInt32 _writeSlowLog;
        AtomicUInt32 _famSlowLog;
        AtomicUInt32 _cmdSlowLog;
};

extern ApCounter globalApCounter;
}