/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "watchdog.h"
#include "failure_detector.h"

#include <boost/filesystem.hpp>
#include <algorithm>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "mongo/base/static_assert.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/log.h"
#include "mongo/platform/process_id.h"
#include "mongo/util/concurrency/idle_thread_block.h"
//#include "mongo/util/errno_util.h"
#include "mongo/util/exit.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/hex.h"
#include "mongo/util/timer.h"
#include "mongo/util/scopeguard.h"

namespace mongo {
int CHECK_FILE_COUNT = 0;

WatchdogCheck::WatchdogCheck(const std::string& name,
                             Milliseconds period,
                             Milliseconds allowDelayTime,
                             WatchdogDeathCallback callback)
    : _period(period), _allowDelayTime(allowDelayTime), _callback(callback), _name(name) {
    invariant(!name.empty());
    log() << "WatchDogCheck name:" << name << ",period:" << _period.count() << " allowDelayTime:" << _allowDelayTime.count();
}

WatchdogPeriodicThread::WatchdogPeriodicThread(Milliseconds period, StringData threadName)
    : _period(period), _enabled(true), _threadName(threadName.toString()) {}

void WatchdogPeriodicThread::start() {
    {
        //stdx::lock_guard<Latch> lock(_mutex);
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        invariant(_state == State::kNotStarted);
        _state = State::kStarted;

        // Start the thread.
        _thread = stdx::thread([this] { this->doLoop(); });
    }
}

void WatchdogPeriodicThread::shutdown() {

    stdx::thread thread;

    {
        //stdx::lock_guard<Latch> lock(_mutex);
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        bool started = (_state == State::kStarted);

        invariant(_state == State::kNotStarted || _state == State::kStarted);

        if (!started) {
            _state = State::kDone;
            return;
        }

        _state = State::kShutdownRequested;

        std::swap(thread, _thread);

        // Wake up the thread if sleeping so that it will check if we are done.
        _condvar.notify_one();
    }

    thread.join();

    _state = State::kDone;
}

void WatchdogPeriodicThread::setPeriod(Milliseconds period) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    bool wasEnabled = _enabled;

    if (period < Milliseconds::zero()) {
        _enabled = false;

        // Leave the thread running but very slowly. If we set this value too high, it would
        // overflow Duration.
        _period = Hours(1);
    } else {
        _period = period;
        _enabled = true;
    }

    if (!wasEnabled && _enabled) {
        resetState();
    }

    _condvar.notify_one();
}

void WatchdogPeriodicThread::doLoop() {
    Client::initThread(_threadName);
    Client* client = &cc();

    auto preciseClockSource = client->getServiceContext()->getPreciseClockSource();

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        resetState();
    }

    Date_t nextRunTime = preciseClockSource->now();

    while (true) {
        auto opCtx = client->makeOperationContext();
        Date_t startTime = preciseClockSource->now();

        // 执行run函数
        run(opCtx.get());

        // 计算下一次运行时间
        nextRunTime = startTime + _period;

        // 如果当前时间已经超过了下一次计划运行时间，立即开始下一次循环
        if (preciseClockSource->now() >= nextRunTime) {
            continue;
        }

        {
            stdx::unique_lock<stdx::mutex> lock(_mutex);
            MONGO_IDLE_THREAD_BLOCK;

            try {
                opCtx->waitForConditionOrInterruptUntil(_condvar, lock, nextRunTime, [&] {
                    return preciseClockSource->now() >= nextRunTime || _state == State::kShutdownRequested;
                });
            } catch (const DBException& e) {
                if (!opCtx->getServiceContext()->getKillAllOperations()) {
                    log() << "Watchdog was interrupted, shutting down, reason:" << e.toStatus();
                    exitCleanly(ExitCode::EXIT_ABRUPT);
                }
                return;
            }

            if (_state == State::kShutdownRequested) {
                return;
            }

            if (!_enabled) {
                continue;
            }
        }
    }
}


WatchdogCheckThread::WatchdogCheckThread(std::vector<std::unique_ptr<WatchdogCheck>> checks,
                                         Milliseconds period,
                                         const std::string& name
                                         )
    : WatchdogPeriodicThread(period, "watchdogCheck"), _checks(std::move(checks)) {

    // 所有的需要被检测的任务的周期必须能被最小周期整除
    long minPeriod = std::numeric_limits<long>::max();
    std::for_each(_checks.begin(), _checks.end(), [&minPeriod](const std::unique_ptr<WatchdogCheck>& item){
        if(item->getPeriod().count() < minPeriod) {
            minPeriod = item->getPeriod().count();
        }
    });

    log() << "min period:" << minPeriod << "ms";
    std::for_each(_checks.begin(), _checks.end(), [&minPeriod](const std::unique_ptr<WatchdogCheck>& item) {
        invariant((item->getPeriod().count() % minPeriod) == 0);
    });

    //设置监控数据
    std::for_each(_checks.begin(), _checks.end(), [&minPeriod, this](const std::unique_ptr<WatchdogCheck>& item) {
        _monitor[item->getName() + "_unHealth_cnt"] = std::make_unique<AtomicInt32>(0);
    });

    if (minPeriod != period.count()) {
        this->setPeriod(Milliseconds(minPeriod));
    }
    globalWatchdogCounter.registerElement(name, this);

    log() << "check job count:" << _checks.size() << ", period:" << minPeriod << "ms;";
}

void WatchdogCheckThread::checkHealths() {
    for(const auto& item : _checks) {
        if (item == nullptr) {
            continue;
        }
        auto now = FailureDetectorCheck::getSteadyMs();
        if (!item->isHealth(now)) {
            log() << "name:" << item->getName()
                  << ", period:" << item->getPeriod() 
                  << ", timePreRun:" << item->getTimePreRun()
                  << ", allowDelayTime:" << item->getAllowDelayTime()
                  << ", now:" << now 
                  << ",delayTime:" << (now - item->getTimePreRun());

            log() << "name:" << item->getName() << " i will callback function";
            _monitor[item->getName() + "_unHealth_cnt"]->fetchAndAdd(1);
            item->getCallback()();
            break;
        } else {
            LOG(5) << "name:" << item->getName() << " check is success";
        }
    }
}

BSONObj WatchdogCheckThread::getObj() const {
    BSONObjBuilder b;

    try {
        for (auto& item : _monitor) {
            b.append(item.first, (item.second)->loadRelaxed());
        } 
    } catch (...) {
        log() << "WatchdogCheckThread get obj is error";
        return BSONObjBuilder().obj();
    }
    return b.obj();
}

void WatchdogCheckThread::resetState() {
    _count.store(0);
}

void WatchdogCheckThread::run(OperationContext* opCtx) {
    ON_BLOCK_EXIT([this](){
        this->_count.addAndFetch(this->_period.count());
    });

    for (auto& check : _checks) {
        if (check->isRunCurrentPeriod(_count.load())) {
            //maybe blocks
            check->run(opCtx);
        }
    }
}

WatchdogMonitorThread::WatchdogMonitorThread(
    const std::shared_ptr<WatchdogCheckThread>& blocking,
    const std::shared_ptr<WatchdogCheckThread>& nonBlocking,
    Milliseconds period)
    : WatchdogPeriodicThread(period, "WatchdogMonitor"),
      _checkBlockingThread(blocking),
      _checkNonBlockingThread(nonBlocking) {

    if (_checkNonBlockingThread) {
        log() << "Non blocking check thread is not null";
    }

    if (_checkBlockingThread) {
        log() << "blocking check thread is not null";
    }

    log() << "monitor period:" << period.count() << "ms";
}

void WatchdogMonitorThread::resetState() {
}

void WatchdogMonitorThread::run(OperationContext* opCtx) {
    if (this->_checkNonBlockingThread) {
        this->_checkNonBlockingThread->checkHealths();
    }

    if (this->_checkBlockingThread) {
        this->_checkBlockingThread->checkHealths();
    }
}


WatchdogMonitor::WatchdogMonitor(std::vector<std::unique_ptr<WatchdogCheck>> checks,
                                 Milliseconds checkPeriod,
                                 Milliseconds monitorPeriod) {

    std::vector<std::unique_ptr<WatchdogCheck>> nonBlockCheck;
    std::vector<std::unique_ptr<WatchdogCheck>> blockCheck;
    for(auto& check : checks) {
        if (!check) {
            continue;
        }

        if (check->getIsBlocking()) {
            blockCheck.push_back(std::move(check));
        } else {
            nonBlockCheck.push_back(std::move(check));
        }
    }

    log() << "nonBlockCheck count:" << nonBlockCheck.size() << ", blockCheck count:" << blockCheck.size();
    if (!nonBlockCheck.empty()) {
        log() <<  "nonblocking";
        _watchdogNonBlockCheckThread = std::make_shared<WatchdogCheckThread>(std::move(nonBlockCheck), checkPeriod, "nonblocking_checker");
    }

    if (!blockCheck.empty()) {
        log() <<  "blocking";
        _watchdogBlockCheckThread = std::make_shared<WatchdogCheckThread>(std::move(blockCheck), checkPeriod, "blocking_checker");
    }
    
    _watchdogMonitorThread = std::make_shared<WatchdogMonitorThread>(_watchdogBlockCheckThread, _watchdogNonBlockCheckThread, monitorPeriod);

    invariant(_watchdogMonitorThread);
    invariant(checkPeriod < monitorPeriod);
}

void WatchdogMonitor::start() {
    log()<<"Starting Watchdog Monitor";
   // LOGV2(23408, "Starting Watchdog Monitor");

    // Start the threads.
    if (_watchdogBlockCheckThread) {
        _watchdogBlockCheckThread->start();
    }

    if (_watchdogNonBlockCheckThread) {
        _watchdogNonBlockCheckThread->start();
    }

    _watchdogMonitorThread->start();
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        invariant(_state == State::kNotStarted);
        _state = State::kStarted;
    }
}

void WatchdogMonitor::setPeriod(Milliseconds duration) {
    //不能动态修改调度的时间；原因是目前调度的周期是按照check来决定，目前也不准备动态调整
    return;
    // {
    //    // stdx::lock_guard<Latch> lock(_mutex);
    //     stdx::lock_guard<stdx::mutex> lock(_mutex);
    //     if (duration > Milliseconds(0)) {
    //         dassert(duration >= Milliseconds(1));

    //         // Make sure that we monitor runs more frequently then checks
    //         // 2 feels like an arbitrary good minimum.
    //         invariant(duration >= 2 * _checkPeriod);

    //         _watchdogCheckThread.setPeriod(_checkPeriod);
    //         _watchdogMonitorThread.setPeriod(duration);

    //         log()<<"WatchdogMonitor period changed to {"<<duration_cast<Seconds>(duration)<<"}";
    //         // LOGV2(23409,
    //         //       "WatchdogMonitor period changed to {duration_cast_Seconds_duration}",
    //         //       "duration_cast_Seconds_duration"_attr = duration_cast<Seconds>(duration));
    //     } else {
    //         _watchdogMonitorThread.setPeriod(duration);
    //         _watchdogCheckThread.setPeriod(duration);

    //         //LOGV2(23410, "WatchdogMonitor disabled");
    //         log()<<"WatchdogMonitor disabled";
    //     }
    // }
}

void WatchdogMonitor::shutdown() {
    {
        //stdx::lock_guard<Latch> lock(_mutex);
         stdx::lock_guard<stdx::mutex> lock(_mutex);
        bool started = (_state == State::kStarted);

        invariant(_state == State::kNotStarted || _state == State::kStarted);

        if (!started) {
            _state = State::kDone;
            return;
        }

        _state = State::kShutdownRequested;
    }

    _watchdogMonitorThread->shutdown();

    if (_watchdogNonBlockCheckThread) {
        _watchdogNonBlockCheckThread->shutdown();
    }

    if (_watchdogBlockCheckThread) {
        _watchdogBlockCheckThread->shutdown();
    }

    _state = State::kDone;
}

/**
 * Check a directory is ok
 * 1. Open up a direct_io to a new file
 * 2. Write to the file
 * 3. Read from the file
 * 4. Close file
 */
void checkFileAllOperator(OperationContext* opCtx, const boost::filesystem::path& file, int& fd) {
    Date_t now = opCtx->getServiceContext()->getPreciseClockSource()->now();
    std::string nowStr = now.toString();

    log()<<"operator file "<<file.generic_string();
    fd = open(file.generic_string().c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        auto err = errno;
        log()<<"open failed for "<<file.generic_string()<<" with error:{"<<errnoWithDescription(err)<<";";

        fassertNoTrace(4080, err == 0);
    }

    size_t bytesWrittenTotal = 0;
    while (bytesWrittenTotal < nowStr.size()) {
        ssize_t bytesWrittenInWrite =
            write(fd, nowStr.c_str() + bytesWrittenTotal, nowStr.size() - bytesWrittenTotal);
        if (bytesWrittenInWrite == -1) {
            auto err = errno;
            if (err == EINTR) {
                continue;
            }

            log()<<"write failed for "<<file.generic_string()<<" with error:{"<<errnoWithDescription(err)<<"};";

            fassertNoTrace(4081, err == 0);
        }

        // Warn if the write was incomplete
        if (bytesWrittenTotal == 0 && static_cast<size_t>(bytesWrittenInWrite) != nowStr.size()) {
            log()<<"write failed for " << file.generic_string() <<" expected "<< nowStr.size() << "bytes but wrote"
            << bytesWrittenInWrite <<" bytes";
        }

        bytesWrittenTotal += bytesWrittenInWrite;
    }

    if (fsync(fd)) {
        auto err = errno;
        log()<<"fsync failed for "<<file.generic_string()<<" with error:{"<<errnoWithDescription(err)<<"};";
        fassertNoTrace(4082, err == 0);
    }

}


void checkFileOnlyWrite(OperationContext* opCtx, const boost::filesystem::path& file, int& fd) {
    if(fd == -1){
        return;
    }

    char write_info = 'a';
    Timer timer;

    ssize_t bytesWrittenInWrite = write(fd, &write_info, 1);
    if (bytesWrittenInWrite == -1) {
        auto err = errno;

        log()<<"disk check  write failed for "<<file.generic_string()<<" with error:{"<<errnoWithDescription(err)<<"};";
        fassertNoTrace(4085, err == 0);
    }

    if (fsync(fd)) {
        auto err = errno;
        log()<<"disk check fsync failed for "<<file.generic_string()<<" with error:{"<<errnoWithDescription(err)<<"};";
        fassertNoTrace(4086, err == 0);
    }

    if (timer.millis() > 100){
        log()<<"check disk only write optime = "<<timer.millis()<<"ms";
    }
}

void watchdogTerminate() {
    // This calls the exit_group syscall on Linux
    invariant(false);

    // ::_exit(ExitCode::EXIT_WATCHDOG);
}


constexpr StringData DirectoryCheck::kProbeFileName;
constexpr StringData DirectoryCheck::kProbeFileNameExt;

bool DirectoryCheck::isRunCurrentPeriod(long count) const {
    if (count < 0) {
        return true;
    }
    if(count % this->getPeriod().count() == 0) {
        _monitor["runCount"]->fetchAndAdd(1);
        return true;
    }
    return false;
}

BSONObj DirectoryCheck::getObj() const {
    BSONObjBuilder b;

    try {
        b.append("runCount", _monitor.at("runCount")->loadRelaxed());
        b.append("runSuc", _monitor.at("runSuc")->loadRelaxed());
        b.append("runFail", _monitor.at("runFail")->loadRelaxed());
    } catch (...) {
        log() << "name:" << this->getName() << " get obj is error";
        return BSONObjBuilder().obj();
    }
    return b.obj();
}

void DirectoryCheck::run(OperationContext* opCtx) {
    bool result = false;
    Timer timer;
    ON_BLOCK_EXIT([this, &result, &timer]() {
        if (result) {
            LOG(5) << "DirectoryCheck result:[success], previous success time:"
                  << this->getTimePreRun() << " => " << FailureDetectorCheck::getSteadyMs()
                  << ", consume:" << timer.micros() << "us";

            _monitor["runSuc"]->fetchAndAdd(1);
            this->setRunSuccessTime(FailureDetectorCheck::getSteadyMs());
        } else {
            log() << "DirectoryCheck result:[failure], previous success time:"
                  << this->getTimePreRun()
                  << ", delay time:" << FailureDetectorCheck::getSteadyMs() - this->getTimePreRun()
                  << "ms, allowDelayTime:" << this->getAllowDelayTime() << "ms";
            _monitor["runFail"]->fetchAndAdd(1);
        }
    });

    // Ensure we have unique file names if multiple processes share the same logging directory
    boost::filesystem::path file = _directory;
    file /= kProbeFileName.toString();
    file += ProcessId::getCurrent().toString();
    file += kProbeFileNameExt.toString();
    if(_only_write_check_cnt >= 10){
        if(_fd != -1){
            if (close(_fd)) {
                auto err = errno;
                log() << "close failed for " << file.generic_string()
                      << " with error:" << errnoWithDescription(err);
                fassertNoTrace(4084, err == 0);
            }
        }
       
        boost::system::error_code ec;
        boost::filesystem::remove(file, ec);
        if (ec) {
            warning()<<"Failed to delete file \'"<<file.generic_string()<<"\' error: "<<ec.message();
        }
        checkFileAllOperator(opCtx, file,_fd);
        _only_write_check_cnt = 0;

    }else{
        checkFileOnlyWrite(opCtx, file, _fd);
        _only_write_check_cnt ++ ;
    }

    result = true;
}

std::string DirectoryCheck::getDescriptionForLogging() {
    return str::stream() << "checked directory '" << _directory.generic_string() << "'";
}

}  // namespace mongo
