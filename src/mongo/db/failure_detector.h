/*
 * @Author: lixin lixin@qiniu.com
 * @Date: 2025-01-22 13:01:32
 * @LastEditors: lixin lixin@qiniu.com
 * @LastEditTime: 2025-04-17 15:57:03
 * @FilePath: /percona-server-mongodb/src/mongo/db/failure_detector.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
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
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/watchdog.h"
#include "mongo/base/string_data.h"
#include <string>
#include <tuple>
#include <deque>
#include <chrono>
#include <mutex>

namespace mongo {

enum class WatchdogReason { 
    HealthCheckError = 1, 
};



class SlidingWindow {
public:
    using clock = std::chrono::steady_clock;
    using time_point = clock::time_point;
    using duration = clock::duration;

    SlidingWindow(duration window_duration, int threshold)
        : window_duration_(window_duration), threshold_(threshold) {}

    /**
     * 添加一次错误记录，并返回当前是否触发异常处理
     * @return true 表示需要触发异常处理，false 表示未达到阈值
     */
    bool addError() {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto now = clock::now();
        
        // 清理窗口外的过期错误
        while (!error_times_.empty() && (now - error_times_.front() > window_duration_)) {
            error_times_.pop_front();
        }
        
        // 记录当前错误时间
        error_times_.push_back(now);
        
        // 检查当前窗口内错误次数
       
    }

    bool isErrorFull(){
        std::lock_guard<std::mutex> lock(mutex_);
        return error_times_.size() >= static_cast<size_t>(threshold_);
    }

private:
    const duration window_duration_;  // 时间窗口长度
    const int threshold_;             // 触发阈值
    std::deque<time_point> error_times_; // 错误时间队列
    std::mutex mutex_;                // 保证线程安全
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

    static bool enableBecomeCandidateWithCurrentState();

    //每次 read/write 的 key
    static std::string generateKey();
    //每次 read/write 的 value
    static std::string generateValue();
    //获得稳定的单调的时间;(ms)
    static long getSteadyMs();
    //触发一次选举
    static void triggerElection(WatchdogReason reason);
    // 判断健康度的表和 db 是否存在
    // 返回值：第一个 bool:表示是否有错误，true 表示没有，false 表示有，第二个 bool 表示是否存在对应的表，true:有，false：表示没有
    static std::tuple<bool, bool> existedHealthDBAndColl(const std::string& primary, int timeout_secs = 1);

    private:
        static AtomicInt64 s_prevElectionTime;
};

class FailureDetectorHealthCheck : public WatchdogCheck {
public:
    FailureDetectorHealthCheck(
        Milliseconds frequency, Milliseconds allowDelayTime, WatchdogDeathCallback callback = []() {
            FailureDetectorCheck::triggerElection(WatchdogReason::HealthCheckError);
        });

    virtual void run(OperationContext* opCtx) final;
    virtual std::string getDescriptionForLogging() final;
    virtual bool isRunCurrentPeriod(long count) const;
    virtual bool isHealth(long nowTime) override;
    BSONObj getObj() const override;

private:
    // first: run listcollection is ok, second: collection existed
    std::tuple<bool, bool> _listCollectionsCheck(const HostAndPort& primary, int timeoutSecs = 1);
    bool _writeHealthCheck(const HostAndPort& primary, int timeoutSecs = 1);
    std::tuple<bool, std::shared_ptr<DBClientConnection>> _getNewConnection(const HostAndPort& addr, int timeoutSecs = 1);

private:
    int _check_count{0};
    std::string _currentPrimary;
    AtomicInt64 _prevElectionTime{0};
    mutable std::unordered_map<std::string, std::unique_ptr<AtomicInt32> > _monitor;
    std::shared_ptr<SlidingWindow> _error_window;
};
}  // namespace mongo