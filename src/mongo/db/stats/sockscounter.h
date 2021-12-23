
#pragma once

#include "mongo/platform/atomic_word.h"
#include "mongo/platform/basic.h"
#include "mongo/db/jsobj.h"

namespace mongo {
class Socks5Counter {

public:
    Socks5Counter();

    void legacyConnect(uint64_t elapsedMs);
    void asioConnect(uint64_t elapsedMs);

    uint64_t getLegacyCount();
    double getLegacyAvgLatency();

    uint64_t getAsioCount();
    double getAsioAvgLatency();

    void incAsioConnectErrorCnt();
    void incLegacyConnectErrorCnt();

    uint32_t getAsioConnectErrorCnt();
    uint32_t getLeagcyConnectErrorCnt();

    void legacySwitch(uint64_t elapsedMs);
    void incLegacySwitchErrorCnt();

    void asioSwitch(uint64_t elapsedMs);
    void incAsioSwitchErrorCnt();

    BSONObj getObj() const; 
private:
    AtomicUInt64 _legacyConnectCnt;
    AtomicUInt64 _asioConnectCnt;

    AtomicUInt64 _legacyConnectLatency;
    AtomicUInt64 _asioConnectLatency;

    AtomicUInt64 _legacySwitchCnt;
    AtomicUInt64 _legacySwitchLatency;
    AtomicUInt64 _legacySwitchErrorCnt;

    AtomicUInt64 _asioSwitchCnt;
    AtomicUInt64 _asioSwitchLatency;
    AtomicUInt64 _asioSwitchErrorCnt;

    AtomicUInt32 _asioConnectErrorCnt;
    AtomicUInt32 _legacyConnectErrorCnt;
};
extern Socks5Counter globalSocksCounter;
}