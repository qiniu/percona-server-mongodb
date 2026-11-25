(function() {
    'use strict';

    var conn = MongoRunner.runMongod({
        storageEngine: 'rocksdb',
    });
    var db = conn.getDB('test');

    var status = db.serverStatus();
    assert(status.storageEngine.name == 'rocksdb');

    // check number of counters and presence of legacy counter names
    var counters = status.rocksdb.counters;
    // RocksDB adds counters in newer releases; the legacy names below stay available.
    assert.gte(Object.keys(counters).length, 93);
    // legacy names
    assert(counters.hasOwnProperty('num-keys-written'));
    assert(counters.hasOwnProperty('num-keys-read'));
    assert(counters.hasOwnProperty('num-seeks'));
    assert(counters.hasOwnProperty('num-forward-iterations'));
    assert(counters.hasOwnProperty('num-backward-iterations'));
    assert(counters.hasOwnProperty('block-cache-misses'));
    assert(counters.hasOwnProperty('block-cache-hits'));
    assert(counters.hasOwnProperty('bloom-filter-useful'));
    assert(counters.hasOwnProperty('bytes-written'));
    assert(counters.hasOwnProperty('bytes-read-point-lookup'));
    assert(counters.hasOwnProperty('bytes-read-iteration'));
    assert(counters.hasOwnProperty('flush-bytes-written'));
    assert(counters.hasOwnProperty('compaction-bytes-read'));
    assert(counters.hasOwnProperty('compaction-bytes-written'));
    // other tickers
    assert(counters.hasOwnProperty('block-cache-add'));
    assert(counters.hasOwnProperty('block-cache-add-failures'));
    assert(counters.hasOwnProperty('block-cache-index-miss'));
    assert(counters.hasOwnProperty('block-cache-index-hit'));
    assert(counters.hasOwnProperty('block-cache-index-add'));
    assert(counters.hasOwnProperty('block-cache-index-bytes-insert'));
    assert(counters.hasOwnProperty('block-cache-index-bytes-evict'));
    assert(counters.hasOwnProperty('block-cache-filter-miss'));
    assert(counters.hasOwnProperty('block-cache-filter-hit'));
    assert(counters.hasOwnProperty('block-cache-filter-add'));
    assert(counters.hasOwnProperty('block-cache-filter-bytes-insert'));
    assert(counters.hasOwnProperty('block-cache-filter-bytes-evict'));
    assert(counters.hasOwnProperty('block-cache-data-miss'));
    assert(counters.hasOwnProperty('block-cache-data-hit'));
    assert(counters.hasOwnProperty('block-cache-data-add'));
    assert(counters.hasOwnProperty('block-cache-data-bytes-insert'));
    assert(counters.hasOwnProperty('block-cache-bytes-read'));
    assert(counters.hasOwnProperty('block-cache-bytes-write'));
    assert(counters.hasOwnProperty('persistent-cache-hit'));
    assert(counters.hasOwnProperty('persistent-cache-miss'));
    assert(counters.hasOwnProperty('sim-block-cache-hit'));
    assert(counters.hasOwnProperty('sim-block-cache-miss'));
    assert(counters.hasOwnProperty('memtable-hit'));
    assert(counters.hasOwnProperty('memtable-miss'));
    assert(counters.hasOwnProperty('l0-hit'));
    assert(counters.hasOwnProperty('l1-hit'));
    assert(counters.hasOwnProperty('l2andup-hit'));
    assert(counters.hasOwnProperty('compaction-key-drop-new'));
    assert(counters.hasOwnProperty('compaction-key-drop-obsolete'));
    assert(counters.hasOwnProperty('compaction-key-drop-range_del'));
    assert(counters.hasOwnProperty('compaction-key-drop-user'));
    assert(counters.hasOwnProperty('compaction-range_del-drop-obsolete'));
    assert(counters.hasOwnProperty('number-keys-updated'));
    assert(counters.hasOwnProperty('number-db-seek-found'));
    assert(counters.hasOwnProperty('number-db-next-found'));
    assert(counters.hasOwnProperty('number-db-prev-found'));
    assert(counters.hasOwnProperty('no-file-closes'));
    assert(counters.hasOwnProperty('no-file-opens'));
    assert(counters.hasOwnProperty('no-file-errors'));
    assert(counters.hasOwnProperty('l0-slowdown-micros'));
    assert(counters.hasOwnProperty('memtable-compaction-micros'));
    assert(counters.hasOwnProperty('l0-num-files-stall-micros'));
    assert(counters.hasOwnProperty('stall-micros'));
    assert(counters.hasOwnProperty('db-mutex-wait-micros'));
    assert(counters.hasOwnProperty('rate-limit-delay-millis'));
    assert(counters.hasOwnProperty('num-iterators'));
    assert(counters.hasOwnProperty('number-multiget-get'));
    assert(counters.hasOwnProperty('number-multiget-keys-read'));
    assert(counters.hasOwnProperty('number-multiget-bytes-read'));
    assert(counters.hasOwnProperty('number-deletes-filtered'));
    assert(counters.hasOwnProperty('number-merge-failures'));
    assert(counters.hasOwnProperty('bloom-filter-prefix-checked'));
    assert(counters.hasOwnProperty('bloom-filter-prefix-useful'));
    assert(counters.hasOwnProperty('number-reseeks-iteration'));
    assert(counters.hasOwnProperty('getupdatessince-calls'));
    assert(counters.hasOwnProperty('block-cachecompressed-miss'));
    assert(counters.hasOwnProperty('block-cachecompressed-hit'));
    assert(counters.hasOwnProperty('block-cachecompressed-add'));
    assert(counters.hasOwnProperty('block-cachecompressed-add-failures'));
    assert(counters.hasOwnProperty('wal-synced'));
    assert(counters.hasOwnProperty('wal-bytes'));
    assert(counters.hasOwnProperty('write-self'));
    assert(counters.hasOwnProperty('write-other'));
    assert(counters.hasOwnProperty('write-timeout'));
    assert(counters.hasOwnProperty('write-wal'));
    assert(counters.hasOwnProperty('number-direct-load-table-properties'));
    assert(counters.hasOwnProperty('number-superversion_acquires'));
    assert(counters.hasOwnProperty('number-superversion_releases'));
    assert(counters.hasOwnProperty('number-superversion_cleanups'));
    assert(counters.hasOwnProperty('number-block-compressed'));
    assert(counters.hasOwnProperty('number-block-decompressed'));
    assert(counters.hasOwnProperty('number-block-not_compressed'));
    assert(counters.hasOwnProperty('merge-operation-time-nanos'));
    assert(counters.hasOwnProperty('filter-operation-time-nanos'));
    assert(counters.hasOwnProperty('row-cache-hit'));
    assert(counters.hasOwnProperty('row-cache-miss'));
    assert(counters.hasOwnProperty('read-amp-estimate-useful-bytes'));
    assert(counters.hasOwnProperty('read-amp-total-read-bytes'));
    assert(counters.hasOwnProperty('number-rate_limiter-drains'));

    MongoRunner.stopMongod(conn);
})();
