#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/db/failure_detector.h"
#include "mongo/base/status.h"
#include "mongo/client/connpool.h"
#include "mongo/db/server_options.h"
#include "mongo/db/repl/repl_set_config.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/util/log.h"
#include "mongo/util/net/sock.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/timer.h"
#include <cstdlib>

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
    int idx = (rand()) % (26);
    return std::string(VALUE_SIZE, 'a' + idx);
}


std::string watchdogReasonStr(WatchdogReason reason) {
    switch (reason) {
        case WatchdogReason::HealthCheckError:
            return "HealthCheckError";
        default:
            return "Invalid Error";
    }
}

bool FailureDetectorCheck::enableBecomeCandidateWithCurrentState() {
    auto replicaCoord = repl::getGlobalReplicationCoordinator();
    invariant(replicaCoord);

    repl::ReplSetConfig config = replicaCoord->getConfig();
    if (!config.validate().isOK()) {
        log() << "replset config is invalid";
        return false;
    }

    int myId = replicaCoord->getMyId();
    auto tmpMember = config.findMemberByID(myId);
    if (!tmpMember) {
        log() << "id:" << myId << " don't find config";
        return false;
    } else {
        LOG(5) << "id:" << myId;
    }
    return tmpMember->isElectable();
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
        case WatchdogReason::HealthCheckError: {
            long now = getSteadyMs();
            if ((now - s_prevElectionTime.load()) <= 180 * 1000) {
                log() << "the last election is in three minute, so jump it";
                return;
            }

            bool result = false;
            Timer timer;
            ON_BLOCK_EXIT([&result, &timer, reasonStr]() {
                if (result) {
                    //变成Primary success;
                    log() << "reason:" << reasonStr << ", result:[success], previous election time:"
                          << s_prevElectionTime.load() << " => " << getSteadyMs() << "ms"
                          << ", consume:" << timer.micros() << "us";
                    s_prevElectionTime.store(getSteadyMs());
                } else {
                    // 没有变成Primary, 可能因为各种原因
                    log() << "reason:" << reasonStr << ", result:[failure], previous election time:"
                          << s_prevElectionTime.load() << ", consume:" << timer.micros() << "us";
                }
            });

            auto replicaCoord = repl::getGlobalReplicationCoordinator();
            invariant(replicaCoord);

            Status status = replicaCoord->stepUpIfEligible();
            if (status.isOK()) {
                result = true;
            }
            break;
        }
        default:
            return;
    }
}

FailureDetectorHealthCheck::FailureDetectorHealthCheck(Milliseconds frequency,
                                                       Milliseconds allowDelayTime,
                                                       WatchdogDeathCallback callback)
    : WatchdogCheck("HealthChecker", frequency, allowDelayTime, callback) {
    invariant(frequency.count() > 0 && allowDelayTime.count() > 0);

    log() << "FailureDetectorHealthCheck: db:" << FailureDetectorCheck::FDDBName.toString()
          << ", Coll:" << FailureDetectorCheck::FDCollName.toString()
          << ", called frequency:" << this->getPeriod()
          << " allowDelayTime:" << this->getAllowDelayTime();

    _monitor["runCount"] = std::make_unique<AtomicInt32>(0);
    _monitor["validRun"] = std::make_unique<AtomicInt32>(0);
    _monitor["runHealthSuc"] = std::make_unique<AtomicInt32>(0);

    _monitor["skipHealthCheck"] = std::make_unique<AtomicInt32>(0);

    _monitor["runCollSuc"] = std::make_unique<AtomicInt32>(0);
    _monitor["runCollFail"] = std::make_unique<AtomicInt32>(0);

    _monitor["runWriteSuc"] = std::make_unique<AtomicInt32>(0);
    _monitor["runWriteFail"] = std::make_unique<AtomicInt32>(0);

    _monitor["runReadSuc"] = std::make_unique<AtomicInt32>(0);
    _monitor["runReadFail"] = std::make_unique<AtomicInt32>(0);

    _monitor["noPrimary"] = std::make_unique<AtomicInt32>(0);
    _monitor["notSecond"] = std::make_unique<AtomicInt32>(0);
    _monitor["notBecomeCand"] = std::make_unique<AtomicInt32>(0);

    globalWatchdogCounter.registerElement(this->getName(), this);
}

std::string FailureDetectorHealthCheck::getDescriptionForLogging() {
    return "HealthChecker";
}

bool FailureDetectorHealthCheck::isRunCurrentPeriod(long count) const {
    if (count < 0) {
        return true;
    }

    if (count % this->getPeriod().count() == 0) {
        _monitor["runCount"]->fetchAndAdd(1);
        return true;
    }
    return false;
}

BSONObj FailureDetectorHealthCheck::getObj() const {
    BSONObjBuilder b;
    try {
        b.append("runCount", _monitor.at("runCount")->loadRelaxed());
        b.append("validRun", _monitor.at("validRun")->loadRelaxed());
        b.append("runHealthSuc", _monitor.at("runHealthSuc")->loadRelaxed());

        b.append("skipHealthCheck", _monitor.at("skipHealthCheck")->loadRelaxed());

        b.append("runCollSuc", _monitor.at("runCollSuc")->loadRelaxed());
        b.append("runCollFail", _monitor.at("runCollFail")->loadRelaxed());

        b.append("runReadSuc", _monitor.at("runReadSuc")->loadRelaxed());
        b.append("runReadFail", _monitor.at("runReadFail")->loadRelaxed());

        b.append("runWriteSuc", _monitor.at("runWriteSuc")->loadRelaxed());
        b.append("runWriteFail", _monitor.at("runWriteFail")->loadRelaxed());

        b.append("noPrimary", _monitor.at("noPrimary")->loadRelaxed());
        b.append("notSecond", _monitor.at("notSecond")->loadRelaxed());
        b.append("notBecomeCand", _monitor.at("notBecomeCand")->loadRelaxed());
    } catch (...) {
        log() << "healthChecker get obj is error";
        return BSONObjBuilder().obj();
    }
    return b.obj();
}

/**
 * 1.判断当前的primary
 * 2. 判断当前node是否为secondary
 * 3. 判断primary是否已经创建了health的表
 * 4. 读请求
 * 5. 写请求
 */
void FailureDetectorHealthCheck::run(OperationContext* opCtx) {
    bool runResult = false;
    Timer timer;

    ON_BLOCK_EXIT([this, &runResult, &timer]() {
        if (runResult) {
            LOG(5) << this->_check_count << ":HealthCheck result:[success], previous success time:"
                  << this->getTimePreRun() << " => " << FailureDetectorCheck::getSteadyMs()
                  << ", consume:" << timer.micros() << "us";

            _monitor["runHealthSuc"]->fetchAndAdd(1);
            this->setRunSuccessTime(FailureDetectorCheck::getSteadyMs());
        } else {
            log() << this->_check_count << ":HealthCheck result:[failure], previous success time:"
                  << this->getTimePreRun()
                  << ", delay time:" << FailureDetectorCheck::getSteadyMs() - this->getTimePreRun()
                  << "ms, allowDelayTime:" << this->getAllowDelayTime() << "ms";
        }
        this->_check_count++;
    });

    auto result = FailureDetectorCheck::getPrimary();
    if (!std::get<0>(result)) {
        _monitor["noPrimary"]->fetchAndAdd(1);
        log() << "get primary is failure, maybe no primary, this check classify to success";
        runResult = true;
        return;
    }

    HostAndPort primary = std::get<1>(result);
    LOG(5) << "HealthCheck primary:" << primary.toString();

    if (!FailureDetectorCheck::isSecondary()) {
        _monitor["notSecond"]->fetchAndAdd(1);
        LOG(5) << "this node is not a secondary, this is "
               << FailureDetectorCheck::getMemberStateStr()
               << ", so this check classify to success";
        runResult = true;
        return;
    }

    if (!FailureDetectorCheck::enableBecomeCandidateWithCurrentState()) {
        _monitor["notBecomeCand"]->fetchAndAdd(1);
        log() << "this node is no enable candidate, so this check classify to success";
        runResult = true;
        return;
    }

    _monitor["validRun"]->fetchAndAdd(1);

    auto key = FailureDetectorCheck::generateKey();
    auto primaryStr = primary.toString();

    do {
        auto collResult = _listCollectionsCheck(primaryStr);
        if (!std::get<0>(collResult)) {
            break;
        }

        if (!std::get<1>(collResult)) {
            _monitor["skipHealthCheck"]->fetchAndAdd(1);
        } else {
            if (!_writeHealthCheck(primaryStr)) {
                break;
            }
        }
        runResult = true;
    } while (false);
}

bool FailureDetectorHealthCheck::_writeHealthCheck(const std::string& primary, int timeoutSecs) {
    invariant(timeoutSecs > 0);

    try {
        ScopedDbConnection conn(primary, timeoutSecs);
        Timer timer;
        bool result = false;

        ON_BLOCK_EXIT([this, &timer, &conn, &result]() {
            conn.done();  // return to pool on success.
            auto elapsedMicros = timer.micros();

            if (result) {
                _monitor["runWriteSuc"]->fetchAndAdd(1);
                LOG(5) << "WriteHealthCheck result:[success]" << elapsedMicros << "us";
            } else {
                _monitor["runWriteFail"]->fetchAndAdd(1);
                log() << "WriteHealthCheck result:[failure]" << elapsedMicros << "us";
            }
        });

        long nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

        BSONObj response;
        BSONObjBuilder requestBuilder;
        requestBuilder.append("findAndModify", FailureDetectorCheck::FDCollName.toString());
        requestBuilder.append("query", BSON("_id" << FailureDetectorCheck::generateKey()));
        requestBuilder.append("upsert", true);
        requestBuilder.append("update",
                              BSON("_id" << FailureDetectorCheck::generateKey() << "value"
                                         << FailureDetectorCheck::generateValue() << "time"
                                         << Date_t::fromMillisSinceEpoch(nowMs)));

        BSONObj request = requestBuilder.done();
        LOG(5) << "write health check request:" << request.toString();

        auto runResult = conn->runCommandWithTarget(
            FailureDetectorCheck::FDDBName.toString(), request, response);
        if (std::get<0>(runResult)) {
            result = true;
        }
        return result;
    } catch (...) {
        log() << "run write check is exception";
        return false;
    }
}

bool FailureDetectorHealthCheck::isHealth(long time) {
    auto result = FailureDetectorCheck::getPrimary();
    if (!std::get<0>(result)) {
        log() << "get primary is failure, maybe no primary, maybe electing, so don't trigge stepup";
        return true;
    }

    HostAndPort primary = std::get<1>(result);
    //如果当前primary为空，大概率是因为第一次运行或者长时间没有primary, 不管是哪一种都不应该触发选举
    if (_currentPrimary.empty()) {
        _currentPrimary = primary.toString();
        _prevElectionTime.store(time);
        return true;
    }

    auto health = WatchdogCheck::isHealth(time);
    //说明最近最近有一次切换，那么就触发新选举了
    if (_currentPrimary != primary.toString()) {
        log() << "last switch, Old:" << _currentPrimary << " => New:" << primary
              << ",this checker will be skip";
        _currentPrimary = primary.toString();
        _prevElectionTime.store(time);
        return true;
    }

    if (!health) {
        // 在3分钟内的切换就不触发
        if ((time - _prevElectionTime.load()) <= 180 * 1000) {
            log() << "last switch, primary:" << _currentPrimary
                  << " and duration last election <= 3 minutes, actual:"
                  << (time - _prevElectionTime.load()) << "ms";
            return true;
        } else {
            log() << "last switch, current primary:" << _currentPrimary
                  << " and duration last election > 3 minutes, so could triggle election";
            return false;
        }
    }
    return true;
}

std::tuple<bool, bool> FailureDetectorHealthCheck::_listCollectionsCheck(const std::string& primary,
                                                                         int timeoutSecs) {
    invariant(timeoutSecs > 0);

    try {
        ScopedDbConnection conn(primary, timeoutSecs);
        Timer timer;
        bool result = false;

        ON_BLOCK_EXIT([this, &timer, &conn, &result]() {
            conn.done();  // return to pool on success.
            auto elapsedMicros = timer.micros();

            if (!result) {
                log() << "ListCollectionCheck result:[failure], consume:" << elapsedMicros << "us";
                _monitor["runCollFail"]->fetchAndAdd(1);
            } else {
                LOG(5) << "ListCollectionCheck result:[success], consume:" << elapsedMicros << "us";
                _monitor["runCollSuc"]->fetchAndAdd(1);
            }
        });

        BSONObjBuilder filter;
        filter.append("name", FailureDetectorCheck::FDCollName.toString());
        BSONObj filterObj = filter.done();

        auto runResult =
            conn->getCollectionInfos(FailureDetectorCheck::FDDBName.toString(), filterObj);
        result = true;
        if (!runResult.empty()) {
            return std::make_tuple(result, true);
        } else {
            log() << "don't find health database and coll";
            return std::make_tuple(result, false);
        }
    } catch (...) {
        log() << "get collectioninfo is exception";
        return std::make_tuple(false, false);
    }
}
}  // namespace mongo