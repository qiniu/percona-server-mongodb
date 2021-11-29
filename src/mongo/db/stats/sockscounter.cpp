#include "mongo/db/stats/sockscounter.h"
#include "mongo/util/debug_util.h"
#include "mongo/db/server_options.h"

namespace mongo {
    Socks5Counter::Socks5Counter(){
        _legacyConnectLatency.store(0);
        _legacyConnectCnt.store(0);

        _asioConnectCnt.store(0);
        _asioConnectLatency.store(0);

        _asioConnectErrorCnt.store(0);
        _legacyConnectErrorCnt.store(0);

    }

    void Socks5Counter::legacyConnect(uint64_t elapsedMs) {
        _legacyConnectCnt.fetchAndAdd(1);
        _legacyConnectLatency.fetchAndAdd(elapsedMs);
    }

    void Socks5Counter::asioConnect(uint64_t elapsedMs) {
        _asioConnectCnt.fetchAndAdd(1);
        _asioConnectLatency.fetchAndAdd(elapsedMs);
    }

    void Socks5Counter::incAsioConnectErrorCnt() {
        _asioConnectErrorCnt.fetchAndAdd(1);
    }

    void Socks5Counter::incLegacyConnectErrorCnt() {
        _legacyConnectErrorCnt.fetchAndAdd(1);
    }

    uint64_t Socks5Counter::getLegacyCount() {
        return _legacyConnectCnt.loadRelaxed();
    }

    uint64_t Socks5Counter::getAsioCount() {
        return _asioConnectCnt.loadRelaxed();
    }

    double Socks5Counter::getLegacyAvgLatency() {
        return  (_legacyConnectLatency.loadRelaxed() * 1.0) / _legacyConnectCnt.loadRelaxed();
    }

    double Socks5Counter::getAsioAvgLatency() {
        return  (_asioConnectLatency.loadRelaxed() * 1.0) / _asioSwitchCnt.loadRelaxed();
    }
    
    uint32_t Socks5Counter::getAsioConnectErrorCnt() {
        return _asioConnectErrorCnt.loadRelaxed();
    }

    uint32_t Socks5Counter::getLeagcyConnectErrorCnt() {
        return _legacyConnectErrorCnt.loadRelaxed();
    }

    void Socks5Counter::legacySwitch(uint64_t elapsedMs) {
        _legacySwitchCnt.fetchAndAdd(1);
        _legacySwitchLatency.fetchAndAdd(elapsedMs);
    }

    void Socks5Counter::asioSwitch(uint64_t elapsedMs) {
        _asioSwitchCnt.fetchAndAdd(1);
        _asioSwitchLatency.fetchAndAdd(elapsedMs);
    }

    void Socks5Counter::incAsioSwitchErrorCnt() {
        _asioSwitchErrorCnt.fetchAndAdd(1);
    }

    void Socks5Counter::incLegacySwitchErrorCnt() {
        _legacySwitchErrorCnt.fetchAndAdd(1);
    }

    BSONObj Socks5Counter::getObj() const {
        BSONObjBuilder b;

        b.append("legacy_connect_cnt", static_cast<int64_t>(_legacyConnectCnt.loadRelaxed()));
        b.append("legacy_connect_latency", static_cast<int64_t>(_legacyConnectLatency.loadRelaxed()));
        b.append("asio_connect_cnt", static_cast<int64_t>(_asioConnectCnt.loadRelaxed()));
        b.append("asio_connect_latency", static_cast<int64_t>(_asioConnectLatency.loadRelaxed()));

        b.append("leacy_connect_error", static_cast<int32_t>(_legacyConnectErrorCnt.loadRelaxed()));
        b.append("asio_connect_error", static_cast<int32_t>(_asioConnectErrorCnt.loadRelaxed()));

        b.append("legacy_switch_cnt", static_cast<int64_t>(_legacySwitchCnt.loadRelaxed()));
        b.append("legacy_switch_latency", static_cast<int64_t>(_legacySwitchLatency.loadRelaxed()));
        b.append("legacy_switch_error_cnt", static_cast<int64_t>(_legacySwitchErrorCnt.loadRelaxed()));

        b.append("asio_switch_cnt", static_cast<int64_t>(_asioSwitchCnt.loadRelaxed()));
        b.append("asio_switch_latency", static_cast<int64_t>(_asioSwitchLatency.loadRelaxed()));
        b.append("asio_switch_error_cnt", static_cast<int64_t>(_asioSwitchErrorCnt.loadRelaxed()));

        b.append("proxy_status", serverGlobalParams.authproxyModel);

        return b.obj();
    }

    Socks5Counter globalSocksCounter;
}