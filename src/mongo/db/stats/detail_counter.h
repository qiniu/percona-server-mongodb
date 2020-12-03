// detail_counters.h

/*
 *    Copyright (C) 2010 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/jsobj.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/basic.h"
#include "mongo/util/concurrency/spin_lock.h"
#include "mongo/util/net/message.h"
#include "mongo/util/processinfo.h"
#include "mongo/db/stats/operation_latency_histogram.h"
#include <memory>
#include <unordered_map>
#include <string>

namespace mongo {

using std::unique_ptr;
using std::unordered_map;
using stdx::mutex; 
using std::string;

class BaseDetailCmdCounter;

//全局变量，用来存储detailcmd的对象

//找个地方把这些相关具体命令的监控信息能存起来；
class DetailCmdCounterContainer {
public:
    const BaseDetailCmdCounter* get(string name);
    uint32_t size();
    void buildObj(BSONObjBuilder& builder);

    void append(const BaseDetailCmdCounter* tmp); 
    void remove(string name);
private:
    mutex _mapLock;
    unordered_map<string, const BaseDetailCmdCounter*> _cmdMap;
};

extern DetailCmdCounterContainer G_D_C_CONTAINER;

class BaseDetailCmdCounter {
    public:
        BaseDetailCmdCounter(const string& name): _name(name) {
            G_D_C_CONTAINER.append(this);
        }

        const string& getName() const {
            return _name;
        }

        virtual BSONObj getObj() const = 0;
        virtual ~BaseDetailCmdCounter() {                
            G_D_C_CONTAINER.remove(_name);
        }
    private:
    string _name;
};


/**
 * for storing detail cmd's qps and latency
 * note: not thread saft, ok with that for speed
 */
class DetailCmdCounter : public BaseDetailCmdCounter {
public:
    DetailCmdCounter(string name) : BaseDetailCmdCounter(name), _latencyHistogramPtr(nullptr) {
        _latencyHistogramPtr = std::make_unique<OperationLatencyHistogram>();
        assert(_latencyHistogramPtr != nullptr);
        _failureCnt.store(0);
    }

    // read
    virtual BSONObj getObj() const {
        BSONObjBuilder b;
        _latencyHistogramPtr->append(true, &b);
        b.append("failureCnt", _failureCnt.loadRelaxed());

        //把直方图中的无关信息删除
        return b.obj().removeField("reads").removeField("writes");
    }

    uint32_t getFailure() const {
        return _failureCnt.loadRelaxed();
    }

    //microsecond
    void gotLatency(uint64_t latency) {
        _latencyHistogramPtr->increment(latency, Command::ReadWriteType::kCommand);
    }

    void gotFailure() {
        _checkWrap();
        _failureCnt.addAndFetch(1);
    }
private:
    void _checkWrap() {
        const unsigned MAX = 1 << 30;
        bool wrap = _failureCnt.loadRelaxed() > MAX;

        if (wrap) {
            _failureCnt.store(0);
        }
    }

private:
    unique_ptr<OperationLatencyHistogram> _latencyHistogramPtr;
    AtomicUInt32 _failureCnt;
};

}  // namespace mongo
