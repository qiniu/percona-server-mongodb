#pragma once

#include <string>
#include <sstream>
#include <vector>
#include "mongo/util/time_support.h"

namespace mongo {
    namespace trace {
        using std::string;

        enum class ConnPhase : uint8_t {
            C_PreTaskRun = 0,
            C_Resolver,
            C_ConnToLocal,
            C_GetNewFd_Header,
            C_GetNewFd_Body,
            C_SwitchToNewFd,
            C_Conn_Succ,
            C_Conn_Sum,
            C_Conn_Auth,
            C_Conn_RunMaster,
            C_Excp_End,
            C_Last_Conn,
            C_End
        };

        class TraceEntry {
            public:
                TraceEntry(ConnPhase p, bool res, int cs) : _phase(p), _res(res), _cs(cs) {}

                ConnPhase phase() {
                    return _phase;
                }

                bool res() {
                    return _res;
                }

                int cs() {
                    return _cs;
                }

                string toString() {
                    std::stringstream ss;
                    ss << "[" << getConnPhaseStr(_phase) << ":" << _res << ":" << _cs << "]";
                    return ss.str();
                }

                const string& getConnPhaseStr(ConnPhase p) {
                    static const string connPhaseStr[] = {
                        "PTR","RSL", "CTL", "GNF_H", "GNF_B", "STN", "CS", "CCS","CA", "CRM", "ECP_END","LED","END"
                    };

                    static const string EMPTY = "";
                    if (p > ConnPhase::C_End) {
                        return EMPTY;
                    }
                    return connPhaseStr[static_cast<uint8_t>(p)];
                }

            private:
                ConnPhase _phase;
                bool _res;
                int _cs;
        };

        class OneTrace {
            public:
                OneTrace() {
                    _trace.reserve(static_cast<uint8_t>(ConnPhase::C_End) + 1);
                    _uid = 0;
                    _fd = -1;
                    _newfd = -1;    
                }

                void addEntry(std::shared_ptr<TraceEntry> entry, int64_t now) {
                    if(entry != nullptr) {
                        _trace.push_back(entry);
                        _prePhase = now;
                    }
                }

                void addEntry(ConnPhase p, bool res, int64_t now) {
                    if (p > ConnPhase::C_End) {
                        return;
                    }
                    if (now < _prePhase) {
                        return;
                    }

                    auto tmp = std::make_shared<TraceEntry>(p, res, now - _prePhase);
                    _trace.push_back(tmp);
                    _prePhase = now;

                    if (p == ConnPhase::C_Excp_End) {
                        tmp = std::make_shared<TraceEntry>(ConnPhase::C_End, false, now - _startTime);
                        _trace.push_back(tmp);
                    }

                    if (p == ConnPhase::C_Conn_Succ) {
                        tmp = std::make_shared<TraceEntry>(ConnPhase::C_Conn_Sum, res, now - _startTime);
                        _trace.push_back(tmp);
                    }

                    if (p == ConnPhase::C_Last_Conn) {
                        tmp = std::make_shared<TraceEntry>(ConnPhase::C_End, res, now - _startTime);
                        _trace.push_back(tmp);
                    }
                }

                std::string toString() {
                    std::stringstream ss;
                    ss << "[uid:" << _uid << "][fd:" << _fd << "][newFd:" << _newfd << "][size:" << _trace.size() << "]";  
                    
                    for(auto &item : _trace) {
                        if (item == nullptr) {
                            continue;
                        }
                        ss << item->toString();
                    }

                    return ss.str();
                }

                void setUid(int uid) {
                    _uid = uid;
                }

                void setFd(int fd) {
                    _fd = fd;
                }

                void setNewFd(int newfd) {
                    _newfd = newfd;
                }

                void setStartTime(int64_t startTime) {
                    _startTime = startTime;
                }

                void setPrePhase(int64_t prePhase) {
                    _prePhase = prePhase;
                }

            private:
                uint64_t _uid;
                std::vector<std::shared_ptr<TraceEntry> > _trace;
                int64_t _prePhase;
                int64_t _startTime;
                int _fd;
                int _newfd;
        };
    }
}