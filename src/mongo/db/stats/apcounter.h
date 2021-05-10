#pragma once

#include "mongo/platform/atomic_word.h"
#include "mongo/platform/basic.h"
#include "mongo/db/jsobj.h"

namespace mongo {

class ApCounter {
    public:
    ApCounter();
    void gotReadTp();
    void gotReadAp();
    void gotErrorGetApExecutorPool();

    void gotReadApSlowLog();
    void gotReadApDSlowLog();
    void gotReadSlowLog();
    void gotReadDSlowLog();
    void gotReadUnSlowLog();
    void gotWriteSlowLog();
    void gotFamSlowLog();
    void gotCmdSlowLog();

    //limit counter
    void gotLegacyConnectionLimit();
    void gotAsioWaitReqQueueLimit();
    void gotShardHostLimit();

    BSONObj getObj() const;

    const AtomicUInt32* getReadTp() const{
        return &_readTp;
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

    const AtomicUInt32* getReadDSlowLog() const {
        return &_readDSlowLog;
    }

    const AtomicUInt32* getReadApSlowLog() const {
        return &_readApSlowLog;
    }
    
    const AtomicUInt32* getReadApDSlowLog() const {
        return &_readApDSlowLog;
    }

    const AtomicUInt32* getReadUnSlowLog() const {
        return &_readUnSlowLog;
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

    // for limiter
    const AtomicUInt32* getLegacyConnectionLimit() const {
        return &_legacyConnectionLimit;
    }
    const AtomicUInt32* getAsioWaitReqQueueLimit() const {
        return &_asioWaitReqQueueLimit;
    }
    const AtomicUInt32* getShardHostLimit() const {
        return &_shardHostLimit;
    }

    private:
    
        void _checkWrap();
        AtomicUInt32 _readTp;
        AtomicUInt32 _readAp;
        AtomicUInt32 _readApExecutorPoolError;

        //一个请求的慢日志计数
        AtomicUInt32 _readApSlowLog;
        AtomicUInt32 _readSlowLog;

        //往各个shard的慢日志计数
        AtomicUInt32 _readDSlowLog;
        AtomicUInt32 _readApDSlowLog;
        AtomicUInt32 _readUnSlowLog;

        AtomicUInt32 _writeSlowLog;
        AtomicUInt32 _famSlowLog;
        AtomicUInt32 _cmdSlowLog;

        // connection limit
        AtomicUInt32 _legacyConnectionLimit;
        AtomicUInt32 _asioWaitReqQueueLimit;
        AtomicUInt32 _shardHostLimit;
};

extern ApCounter globalApCounter;
}