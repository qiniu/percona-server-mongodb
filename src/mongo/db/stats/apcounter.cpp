#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/stats/apcounter.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/log.h"

namespace mongo {
using std::endl;

ApCounter::ApCounter() {}

void ApCounter::gotReadAp() {
    RARELY _checkWrap();
    _readAp.fetchAndAdd(1);
}

void ApCounter::gotReadTp() {
    RARELY _checkWrap();
    _readTp.fetchAndAdd(1);
}

void ApCounter::gotErrorGetApExecutorPool() {
    RARELY _checkWrap();
    _readApExecutorPoolError.fetchAndAdd(1);
}

void ApCounter::gotReadSlowLog() {
    RARELY _checkWrap();
    _readSlowLog.fetchAndAdd(1);
}

void ApCounter::gotReadDSlowLog() {
    RARELY _checkWrap();
    _readDSlowLog.fetchAndAdd(1);
}

void ApCounter::gotReadUnSlowLog() {
    RARELY _checkWrap();
    _readUnSlowLog.fetchAndAdd(1);
}

void ApCounter::gotReadApSlowLog() {
    RARELY _checkWrap();
    _readApSlowLog.fetchAndAdd(1);
}
void ApCounter::gotReadApDSlowLog() {
    RARELY _checkWrap();
    _readApDSlowLog.fetchAndAdd(1);
}

void ApCounter::gotCmdSlowLog() {
    RARELY _checkWrap();
    _cmdSlowLog.fetchAndAdd(1);
}

void ApCounter::gotFamSlowLog() {
    RARELY _checkWrap();
    _famSlowLog.fetchAndAdd(1);
}

void ApCounter::gotWriteSlowLog() {
    RARELY _checkWrap();
    _writeSlowLog.fetchAndAdd(1);
}

void ApCounter::gotLegacyConnectionLimit() {
    RARELY _checkWrap();
    _legacyConnectionLimit.fetchAndAdd(1);
}

void ApCounter::gotAsioWaitReqQueueLimit() {
    RARELY _checkWrap();
    _asioWaitReqQueueLimit.fetchAndAdd(1);
}

void ApCounter::gotShardHostLimit() {
    RARELY _checkWrap();
    _shardHostLimit.fetchAndAdd(1);
}

void ApCounter::_checkWrap() {
    const unsigned MAX = 1 << 30;

    bool wrap = _readAp.loadRelaxed() > MAX || _readTp.loadRelaxed() > MAX ||
        _readApExecutorPoolError.loadRelaxed() > MAX || _readSlowLog.loadRelaxed() > MAX ||
        _readDSlowLog.loadRelaxed() > MAX || _readApDSlowLog.loadRelaxed() > MAX ||
        _readApSlowLog.loadRelaxed() > MAX || _cmdSlowLog.loadRelaxed() > MAX ||
        _writeSlowLog.loadRelaxed() > MAX || _famSlowLog.loadRelaxed() > MAX ||
        _legacyConnectionLimit.loadRelaxed() > MAX || _asioWaitReqQueueLimit.loadRelaxed() > MAX ||
        _shardHostLimit.loadRelaxed() > MAX || _readUnSlowLog.loadRelaxed() > MAX;

    if (wrap) {
        _readAp.store(0);
        _readTp.store(0);
        _readApExecutorPoolError.store(0);

        _readSlowLog.store(0);
        _readDSlowLog.store(0);
        _readApSlowLog.store(0);
        _readApDSlowLog.store(0);
        _readUnSlowLog.store(0);

        _writeSlowLog.store(0);
        _famSlowLog.store(0);
        _cmdSlowLog.store(0);

        _legacyConnectionLimit.store(0);
        _asioWaitReqQueueLimit.store(0);
        _shardHostLimit.store(0);
    }
}

BSONObj ApCounter::getObj() const {
    BSONObjBuilder b;
    b.append("readAp", _readAp.loadRelaxed());
    b.append("readTp", _readTp.loadRelaxed());
    b.append("error_apexecutor_pool", _readApExecutorPoolError.loadRelaxed());

    b.append("read_slowlog", _readSlowLog.loadRelaxed());
    b.append("read_ap_slowlog", _readApSlowLog.loadRelaxed());
    b.append("read_d_slowlog", _readDSlowLog.loadRelaxed());
    b.append("read_ap_d_slowlog", _readApDSlowLog.loadRelaxed());
    b.append("read_un_slowlog", _readUnSlowLog.loadRelaxed());
    b.append("write_slowlog", _writeSlowLog.loadRelaxed());
    b.append("fam_slowlog", _famSlowLog.loadRelaxed());
    b.append("cmd_slowlog", _cmdSlowLog.loadRelaxed());

    b.append("limitForLegacy", _legacyConnectionLimit.loadRelaxed());
    b.append("limitForAsioReqQ", _asioWaitReqQueueLimit.loadRelaxed());
    b.append("limitForRefresh", _shardHostLimit.loadRelaxed());
    return b.obj();
}

ApCounter globalApCounter;
}  // namespace mongo