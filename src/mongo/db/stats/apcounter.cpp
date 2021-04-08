#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/stats/apcounter.h"
#include "mongo/util/log.h"
#include "mongo/util/debug_util.h"

namespace mongo {
    using std::endl;

    ApCounter::ApCounter(){}

    void ApCounter::gotReadAp() {
        RARELY _checkWrap();
        _readAp.fetchAndAdd(1);
    }

    void ApCounter::gotReadNotAp() {
        RARELY _checkWrap();
        _readNotAp.fetchAndAdd(1);
    }

    void ApCounter::gotErrorGetApExecutorPool() {
        RARELY _checkWrap();
        _readApExecutorPoolError.fetchAndAdd(1);
    }


    void ApCounter::_checkWrap() {
        const unsigned MAX = 1 << 30;

        bool wrap = _readAp.loadRelaxed() > MAX || _readNotAp.loadRelaxed() > MAX || _readApExecutorPoolError.loadRelaxed() > MAX;

        if (wrap) {
            _readAp.store(0);
            _readNotAp.store(0);
            _readApExecutorPoolError.store(0);
        }
    }

    BSONObj ApCounter::getObj() const {
        BSONObjBuilder b;
        b.append("readAp", _readAp.loadRelaxed());
        b.append("readNotAp", _readNotAp.loadRelaxed());
        b.append("error_apexecutor_pool", _readApExecutorPoolError.loadRelaxed());
        return b.obj();
    }

    ApCounter globalApCounter;
}