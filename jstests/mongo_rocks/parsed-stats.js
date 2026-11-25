(function() {
    'use strict';

    var conn = MongoRunner.runMongod({
        storageEngine: 'rocksdb',
    });
    var db = conn.getDB('test');

    var status = db.serverStatus();
    assert(status.storageEngine.name == 'rocksdb');

    // Flush real data so the parser covers populated level rows as well as Sum/Int.
    var documents = [];
    for (var i = 0; i < 1000; ++i) {
        documents.push({_id: i, value: 'rocksdb-stats-' + i});
    }
    assert.writeOK(db.parsed_stats.insert(documents));
    assert.commandWorked(db.adminCommand({fsync: 1}));
    // Compaction after deletes can report negative Wnew, including -0.0.
    assert.writeOK(db.parsed_stats.remove({_id: {$lt: 500}}));
    assert.commandWorked(db.runCommand({compact: 'parsed_stats'}));
    assert.eq(500, db.parsed_stats.find().itcount());

    var rocksdb = db.serverStatus().rocksdb;

    assert(rocksdb.hasOwnProperty('compaction-stats'));
    assert(!rocksdb['compaction-stats'].hasOwnProperty('error'));
    assert.eq(32, Object.keys(rocksdb['compaction-stats']).length)

    var cstats = rocksdb['compaction-stats'];
    assert(cstats.hasOwnProperty('level-stats'));
    assert.gt(cstats['level-stats'].Sum['num-files'], 0);
    assert.gt(cstats['level-stats'].Sum['total-file-size-MB'], 0);
    assert(cstats.hasOwnProperty('uptime-total-sec'));
    assert(cstats.hasOwnProperty('uptime-interval-sec'));
    assert(cstats.hasOwnProperty('flush-cumulative-GB'));
    assert(cstats.hasOwnProperty('flush-interval-GB'));
    assert(cstats.hasOwnProperty('addfile-cumulative-GB'));
    assert(cstats.hasOwnProperty('addfile-interval-GB'));
    assert(cstats.hasOwnProperty('addfile-cumulative-cnt'));
    assert(cstats.hasOwnProperty('addfile-interval-cnt'));
    assert(cstats.hasOwnProperty('addfile-cumulative-l0-cnt'));
    assert(cstats.hasOwnProperty('addfile-interval-l0-cnt'));
    assert(cstats.hasOwnProperty('addfile-cumulative-key-cnt'));
    assert(cstats.hasOwnProperty('addfile-interval-key-cnt'));
    assert(cstats.hasOwnProperty('cumulative-written-GB'));
    assert(cstats.hasOwnProperty('cumulative-written-MB-s'));
    assert(cstats.hasOwnProperty('cumulative-read-GB'));
    assert(cstats.hasOwnProperty('cumulative-read-MB-s'));
    assert(cstats.hasOwnProperty('cumulative-seconds'));
    assert(cstats.hasOwnProperty('interval-written-GB'));
    assert(cstats.hasOwnProperty('interval-written-MB-s'));
    assert(cstats.hasOwnProperty('interval-read-GB'));
    assert(cstats.hasOwnProperty('interval-read-MB-s'));
    assert(cstats.hasOwnProperty('interval-seconds'));
    assert(cstats.hasOwnProperty('stalls-level0-slowdown'));
    assert(cstats.hasOwnProperty('stalls-level0-slowdown-with-compaction'));
    assert(cstats.hasOwnProperty('stalls-level0-numfiles'));
    assert(cstats.hasOwnProperty('stalls-level0-numfiles-with-compaction'));
    assert(cstats.hasOwnProperty('stalls-stop-for-pending-compaction-bytes'));
    assert(cstats.hasOwnProperty('stalls-slowdown-for-pending-compaction-bytes'));
    assert(cstats.hasOwnProperty('stalls-memtable-compaction'));
    assert(cstats.hasOwnProperty('stalls-memtable-slowdown'));
    assert(cstats.hasOwnProperty('stalls-interval-total-count'));

    assert(rocksdb.hasOwnProperty('db-stats'));
    assert(!rocksdb['db-stats'].hasOwnProperty('error'));
    assert.eq(28, Object.keys(rocksdb['db-stats']).length)

    var dbstats = rocksdb['db-stats'];
    assert(dbstats.hasOwnProperty('uptime-total-sec'));
    assert(dbstats.hasOwnProperty('uptime-interval-sec'));
    assert(dbstats.hasOwnProperty('cumulative-writes-cnt'));
    assert(dbstats.hasOwnProperty('cumulative-writes-keys'));
    assert(dbstats.hasOwnProperty('cumulative-writes-commit-groups'));
    assert(dbstats.hasOwnProperty('cumulative-writes-per-commit-group'));
    assert(dbstats.hasOwnProperty('cumulative-writes-ingest-GB'));
    assert(dbstats.hasOwnProperty('cumulative-writes-ingest-MB-s'));
    assert(dbstats.hasOwnProperty('cumulative-WAL-writes'));
    assert(dbstats.hasOwnProperty('cumulative-WAL-syncs'));
    assert(dbstats.hasOwnProperty('cumulative-WAL-writes-per-sync'));
    assert(dbstats.hasOwnProperty('cumulative-WAL-written-GB'));
    assert(dbstats.hasOwnProperty('cumulative-WAL-written-MB-s'));
    assert(dbstats.hasOwnProperty('cumulative-stall-sec'));
    assert(dbstats.hasOwnProperty('cumulative-stall-percent'));
    assert(dbstats.hasOwnProperty('interval-writes-cnt'));
    assert(dbstats.hasOwnProperty('interval-writes-keys'));
    assert(dbstats.hasOwnProperty('interval-writes-commit-groups'));
    assert(dbstats.hasOwnProperty('interval-writes-per-commit-group'));
    assert(dbstats.hasOwnProperty('interval-writes-ingest-MB'));
    assert(dbstats.hasOwnProperty('interval-writes-ingest-MB-s'));
    assert(dbstats.hasOwnProperty('interval-WAL-writes'));
    assert(dbstats.hasOwnProperty('interval-WAL-syncs'));
    assert(dbstats.hasOwnProperty('interval-WAL-writes-per-sync'));
    assert(dbstats.hasOwnProperty('interval-WAL-written-MB'));
    assert(dbstats.hasOwnProperty('interval-WAL-written-MB-s'));
    assert(dbstats.hasOwnProperty('interval-stall-sec'));
    assert(dbstats.hasOwnProperty('interval-stall-percent'));

    MongoRunner.stopMongod(conn);
})();
