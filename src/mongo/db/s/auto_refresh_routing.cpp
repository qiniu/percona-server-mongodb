/*
 * @Author: your name
 * @Date: 2020-09-03 17:28:23
 * @LastEditTime: 2020-12-25 17:21:57
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /percona-server-mongodb/src/mongo/util/net/ssl_expiration.cpp
 */
/*    Copyright 2014 10gen Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl
#include <random>
#include <string>
#include <vector>
#include "mongo/db/s/auto_refresh_routing.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/client.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"
#include "mongo/db/namespace_string.h"
#include "mongo/s/grid.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/mongod_options.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonelement.h"
namespace mongo {

AutoRefreshRouting::AutoRefreshRouting(uint64_t start){
        std::default_random_engine random(time(NULL));
        std::uniform_int_distribution<int> r1(60,240);//避免批量启动secondary mongod 同时拉取chunks configsvr压力大
        _nextRefreshTime = start + r1(random);
        log()<<__FILE__<<":"<<__LINE__<<" autoRefreshRoutingNameSpace";
    }

std::string AutoRefreshRouting::taskName() const {
    return "AutoRefreshRouting";
}

        
void AutoRefreshRouting::taskDoWork() {

    repl::ReplicationCoordinator* replCoord = repl::getGlobalReplicationCoordinator();
    if(!replCoord->getMemberState().secondary()){ // 非secondary跳过
        return;
    }

    auto now  = time(NULL);
        
    if((uint64_t)now > _nextRefreshTime){
        Client::initThreadIfNotAlready("auto-refresh-routing");
        auto txnPtr = cc().makeOperationContext();
        OperationContext& txn = *txnPtr;


        auto findStatus = Grid::get(&txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        &txn,
        ReadPreferenceSetting{ReadPreference::PrimaryOnly},
        repl::ReadConcernLevel::kMajorityReadConcern,
        NamespaceString(CollectionType::ConfigNS),
        BSONObj(),
        BSONObj(),
        boost::none /* no limit */);

        if (!findStatus.isOK()) {
            log()<<"query collecions from configsvr fail."<<findStatus.getStatus().reason();
            return;
        }

        std::vector<std::string> collectionNames;
        const auto& collectionDocsOpTimePair = findStatus.getValue();
        for (const BSONObj& obj : collectionDocsOpTimePair.docs) {
            BSONElement e = obj.getField("_id");
            auto nameSpace = e.str();
            collectionNames.push_back(nameSpace);
        }

        if(!replCoord->getMemberState().secondary()){ // 非secondary跳过,二次校验，避免拉collections过程中发送变更
            return;
        }
        
        for(auto itr : collectionNames){
            log()<<"need refresh collection = "<<itr;
            Grid::get(&txn)->catalogCache()->getShardedCollectionRoutingInfoWithRefresh(&txn, itr);
        }

         std::default_random_engine random(now);
         std::uniform_int_distribution<int> r1(80000,86400);
        _nextRefreshTime = now + r1(random); //避免多个secondary mongod同时拉取chunks 
        log()<<"next refreshtime = "<<_nextRefreshTime;
    }

}

}  // namespace mongo
