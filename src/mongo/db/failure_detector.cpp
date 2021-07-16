#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/failure_detector.h"
#include "mongo/client/connpool.h"
#include "mongo/db/server_options.h"
#include "mongo/util/net/sock.h"
#include "mongo/base/status.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/timer.h"
#include "mongo/util/log.h"

namespace mongo {
constexpr StringData FailureDetectorCheck::FDDBName;
constexpr StringData FailureDetectorCheck::FDCollName;
constexpr StringData FailureDetectorCheck::FDNS;

const int VALUE_SIZE = 200;

AtomicInt64 FailureDetectorCheck::s_prevElectionTime;

long FailureDetectorCheck::getSteadyMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

//每一周key会轮训回来
std::string FailureDetectorCheck::generateKey() {
    static string prefix = str::stream()
        << HostAndPort(getHostNameCached(), serverGlobalParams.port).toString() << ':';
    return prefix;
}

std::string FailureDetectorCheck::generateValue() {
    static std::string S_Value = []() {
        std::string tmp;
        tmp.resize(VALUE_SIZE);
        for (int i = 0; i < VALUE_SIZE; i++) {
            tmp[i] = 'a';
        }
        return tmp;
    }();
    return S_Value;
}

std::string watchdogReasonStr(WatchdogReason reason) {
    switch (reason) {
        case WatchdogReason::ReadCheckError:
            return "ReadCheckError";
        case WatchdogReason::WriteCheckError:
            return "WriteCheckError";
        default:
            return "Invalid Error";
    }
}

std::string FailureDetectorCheck::getMemberStateStr() {
    auto replicaCoord = repl::getGlobalReplicationCoordinator();
    invariant(replicaCoord);

    return replicaCoord->getMemberState().toString();
}

bool FailureDetectorCheck::isSecondary() {
    auto replicaCoord = repl::getGlobalReplicationCoordinator();
    invariant(replicaCoord);

    return replicaCoord->getMemberState().secondary();
}

std::tuple<bool, HostAndPort> FailureDetectorCheck::getPrimary() {
    auto replicaCoord = repl::getGlobalReplicationCoordinator();
    invariant(replicaCoord);

    return replicaCoord->getPrimary();
}

void FailureDetectorCheck::triggerElection(WatchdogReason reason) {
    std::string reasonStr = watchdogReasonStr(reason);
    log() << "Trigger Election, reason:" << reasonStr;

    switch (reason) {
        case WatchdogReason::ReadCheckError:
        case WatchdogReason::WriteCheckError: {
            long now = getSteadyMs();
            if ((now - s_prevElectionTime.load()) <= 60 * 1000) {
                log() << "the last election is in one minute, so jump it";
                return;
            }

            auto replicaCoord = repl::getGlobalReplicationCoordinator();
            invariant(replicaCoord);

            //通过
            Status status = replicaCoord->stepUpIfEligible();
            if (status.isOK()) {
                s_prevElectionTime.store(getSteadyMs());
                log() << "reason:" << reasonStr << ", result:success, time:"  << s_prevElectionTime.load();
            } else {
                log() << "reason:" << reasonStr << ", result:failure, time:"  << s_prevElectionTime.load() << ",reason:" << status.reason();
            }
            break;
        }
        default:
            return;
    }
}

FailureDetectorWriteCheck::FailureDetectorWriteCheck(int frequency,
                                                     long allowDelayTime,
                                                     WatchdogDeathCallback callback)
    : WatchdogCheck("WriteChecker", frequency, allowDelayTime, callback) {
    invariant(frequency > 0 && allowDelayTime > 0);

    log() << "FailureDectorWriteCheck: db:" << FailureDetectorCheck::FDDBName.toString() << ", Coll:" << FailureDetectorCheck::FDCollName.toString()
          << ", called frequency:" << this->getPeriod()
          << " allowDelayTime:" << this->getAllowDelayTime();
}

std::string FailureDetectorWriteCheck::getDescriptionForLogging() {
    return "WriterChecker";
}

bool FailureDetectorWriteCheck::isRunCurrentPeriod(long count) const {
    if (count < 0) {
        return true;
    }

    if (count % this->getPeriod() == 0) {
        return true;
    }
    return false;
}

/**
 * 1.判断当前节点是否是secondary
 * 2.get primary
 * 4.写请求能否正常
 */
void FailureDetectorWriteCheck::run(OperationContext* opCtx) {
    bool runResult = false;
    ON_BLOCK_EXIT([this, &runResult]() {
        if (runResult) {
            log() << "第" << this->_write_check_count << "次 Failure write check result:success";
            this->setRunSuccessTime(FailureDetectorCheck::getSteadyMs());
        } else {
            log() << "第" << this->_write_check_count << "次 Failure write check result:failure";
        }
        this->_write_check_count++;
        log() << "****************FailureDetectorWriteCheck End****************";
    });

    log() << "****************FailureDetectorWriteCheck Start****************";

    auto result = FailureDetectorCheck::getPrimary();
    if (!std::get<0>(result)) {
            log() << "get primary is failure, maybe no primary, this check classify to success";
            runResult = true;
            return;
    }

    HostAndPort primary = std::get<1>(result);
    log() << "WriterCheck primary:" << primary.toString();

    if (!FailureDetectorCheck::isSecondary()) {
        log() << "this node is not a secondary, this is "
              << FailureDetectorCheck::getMemberStateStr()
              << ", so this check classify to success";
        runResult = true;
        return;
    }

    auto key = FailureDetectorCheck::generateKey();
    auto primaryStr = primary.toString();

    if (writeHealthCheck(primaryStr, 1)) {
        runResult = true;
    }
}

bool FailureDetectorWriteCheck::writeHealthCheck(const std::string& primary, int timeoutSecs) {
    ScopedDbConnection conn(primary, timeoutSecs);
    Timer timer;
    bool result = false;

    ON_BLOCK_EXIT([&timer, &conn, &result]() {
        conn.done();  // return to pool on success.
        auto pingMicros = timer.micros();
        log() << (result ? "result:success," : "result: failure,")
              << "write health check consume:" << pingMicros << "us";
    });

    long nowSecs = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

    BSONObj response;
    BSONObjBuilder requestBuilder;
    requestBuilder.append("findAndModify", FailureDetectorCheck::FDCollName.toString());
    requestBuilder.append("query", BSON("_id" << FailureDetectorCheck::generateKey()));
    requestBuilder.append("upsert", true);
    requestBuilder.append("update", BSON("_id" << FailureDetectorCheck::generateKey() << "value" << FailureDetectorCheck::generateValue() << "time" << nowSecs));

    BSONObj request = requestBuilder.done();
    log() << "write health check request:" << request.toString();

    auto runResult = conn->runCommandWithTarget(FailureDetectorCheck::FDDBName.toString(), request, response);
    if (std::get<0>(runResult)) {
        result = true;
    } 
    return result;
}

FailureDetectorReadCheck::FailureDetectorReadCheck(int frequency,
                                                   long allowDelayTime,
                                                   WatchdogDeathCallback callback)
    : WatchdogCheck("ReaderChecker", frequency, allowDelayTime, callback) {
    invariant(frequency > 0 && allowDelayTime > 0);

    log() << "FailureDectorReadCheck: db:" << FailureDetectorCheck::FDDBName.toString() << ", Coll:" << FailureDetectorCheck::FDCollName.toString()
          << ", called frequency:" << this->getPeriod()
          << " allowDelayTime:" << this->getAllowDelayTime();
}

std::string FailureDetectorReadCheck::getDescriptionForLogging() {
    return "ReaderChecker";
}

bool FailureDetectorReadCheck::isRunCurrentPeriod(long count) const {
    if (count < 0) {
        return true;
    }

    if (count % this->getPeriod() == 0) {
        return true;
    }
    return false;
}

/**
 * 1.判断当前节点是否是secondary
 * 2.get primary
 * 4.写请求能否正常
 */
void FailureDetectorReadCheck::run(OperationContext* opCtx) {
    bool runResult = false;
    ON_BLOCK_EXIT([this, &runResult]() {
        if (runResult) {
            log() << "第" << this->_read_check_count << "次 Failure read check result:success";
            this->setRunSuccessTime(FailureDetectorCheck::getSteadyMs());
        } else {
            log() << "第" << this->_read_check_count << "次 Failure read check result:failure";
        }
        this->_read_check_count++;
        log() << "****************FailureDetectorReadCheck End****************";
    });

    log() << "****************FailureDetectorReadCheck Start****************";
    auto result = FailureDetectorCheck::getPrimary();

    if (!std::get<0>(result)) {
            log() << "get primary is failure, maybe no primary, this check classify to success";
            runResult = true;
            return;
        }

    HostAndPort primary = std::get<1>(result);
    log() << "primary:" << primary.toString();

    if (!FailureDetectorCheck::isSecondary()) {
        log() << "this node is not a secondary, this is "
              << FailureDetectorCheck::getMemberStateStr() 
              << ", this check classify to success";
        runResult = true;
        return;
    }

    auto key = FailureDetectorCheck::generateKey();
    auto primaryStr = primary.toString();

    if (readHealthCheck(primaryStr, 1)) {
        runResult = true;
    }
}

bool FailureDetectorReadCheck::readHealthCheck(const std::string& primary, int timeoutSecs) {
    ScopedDbConnection conn(primary, timeoutSecs);
    Timer timer;
    bool result = false;

    ON_BLOCK_EXIT([&timer, &conn, &result]() {
        conn.done();  // return to pool on success.
        auto pingMicros = timer.micros();
        log() << (result ? "result:success," : "result: failure,")
              << "read health check consume:" << pingMicros << "us";
    });

    Query query = QUERY("_id" << FailureDetectorCheck::generateKey());
    query.readPref(ReadPreference::PrimaryOnly, BSONArray());
    log() << "read request:" << query.toString();

    auto response = conn->query(FailureDetectorCheck::FDNS.toString(), query);
    if (response == nullptr) {
        log() << "failure detector read check is failure";
        return false;
    }

    log() << "failure detector read check is success";
    while (response->more()) {
        log() << "read content:" << response->next().toString();
    }
    return true;
}


}  // namespace mongo