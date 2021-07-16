/**
 * failure detector 
 *
 * 1. 只有secondary才会启动错误检测器;
 * 2. read/write是两种探测机制，任何一种发现有问题都会进行切换
 * 3.
 * 如果当前的副本集处于一种不健康状态就不进行检测，这里的不健康主要是no-primary，因为这种情况心跳能覆盖
 *
 */

#pragma once

#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/db/watchdog.h"
#include "mongo/base/string_data.h"
#include <string>
#include <tuple>

namespace mongo {

enum class WatchdogReason { 
    ReadCheckError = 1, 
    WriteCheckError = 2
};

class FailureDetectorCheck {
public:
    static constexpr StringData FDDBName = "health"_sd;
    static constexpr StringData FDCollName = "health_check"_sd;
    static constexpr StringData FDNS = "health.health_check"_sd;

    //判断当前节点是否是 secondary
    static bool isSecondary();
    static std::string getMemberStateStr();
    //获得 primary 的信息
    static std::tuple<bool, HostAndPort> getPrimary();

    //每次 read/write 的 key
    static std::string generateKey();
    //每次 read/write 的 value
    static std::string generateValue();
    //获得稳定的单调的时间;(ms)
    static long getSteadyMs();
    //触发一次选举
    static void triggerElection(WatchdogReason reason);

    private:
        static AtomicInt64 s_prevElectionTime;
};

class FailureDetectorWriteCheck : public WatchdogCheck {
public:
    FailureDetectorWriteCheck(
        int frequency, long allowDelayTime, WatchdogDeathCallback callback = []() {
            FailureDetectorCheck::triggerElection(WatchdogReason::WriteCheckError);
        });

    virtual void run(OperationContext* opCtx) final;
    virtual std::string getDescriptionForLogging() final;
    virtual bool isRunCurrentPeriod(long count) const;

private:
    bool writeHealthCheck(const std::string& primary, int timeoutSecs);

private:
    int _write_check_count;
};

class FailureDetectorReadCheck : public WatchdogCheck {
public:
    FailureDetectorReadCheck(
        int frequency, long allowDelayTime, WatchdogDeathCallback callback = []() {
            FailureDetectorCheck::triggerElection(WatchdogReason::ReadCheckError);
        });

    virtual void run(OperationContext* opCtx) final;
    virtual std::string getDescriptionForLogging() final;
    virtual bool isRunCurrentPeriod(long count) const;

private:
    bool readHealthCheck(const std::string& primary, int timeoutSecs);

private:
    int _read_check_count;
};
}  // namespace mongo