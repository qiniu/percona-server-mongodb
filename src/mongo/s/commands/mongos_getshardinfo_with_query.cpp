/**
 *    Copyright (C) 2015 MongoDB Inc.
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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding
#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/util/scopeguard.h"
#include <memory>
#include <ctime>
#include <ratio>
#include <chrono>

namespace mongo {

      using namespace std::chrono;

namespace {
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;

const string CMD_NAME = "getShardInfoWithQuery"; 

class MongosGetShardInfoWithQueryCmd : public Command {
public:
    MongosGetShardInfoWithQueryCmd()
        : Command("getShardInfoWithQuery", false, "getShardInfoWithQuery") {
            LOG(logger::LogSeverity::Info()) << "MongosGetShardINfoWithQueryCmd is create";
            _detailCmder = std::make_unique<DetailCmdCounter<CMD_NAME>>();
        }

    virtual bool slaveOk() const {
        return true;
    }

    virtual bool adminOnly() const {
        return false;
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    virtual void help(std::stringstream& help) const {
        help << " get shard info by query, similar to explain";
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::getShardInfoWithQuery);
        out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
    }

    /**
     * 输入协议
     * {
            getShardInfo:{
                find:"collection的名字",
                filter:{
                    k4:v4
                }
                ....
            }
        }

        输出协议
        {
           //表示多shard或者单shard
           "type": "SINGLE/MULTI",
           "shards":[
               {
                   "shardName":"shard0"
               },
               {
                   "shardName":"shard1"
               }
           ],
           "ok":1
        }
    */

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        bool success = true;
        auto startTime = std::chrono::steady_clock::now();
        ON_BLOCK_EXIT([this, success, startTime](){                
            if (!success) {                    
                this->_detailCmder->gotFailure();
            } else {
                auto endTime = std::chrono::steady_clock::now();
                this->_detailCmder->gotLatency(std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count());
            }
        });

        try {
            // This is the nested command which we are explaining.
            BSONObj explainObj = cmdObj.firstElement().Obj();

            const std::string cmdName = explainObj.firstElementFieldName();
            Command* commToExplain = Command::findCommand(cmdName);
            if (!commToExplain || commToExplain->getName() != "find") {
                appendCommandStatus(
                    result,
                    Status{ErrorCodes::CommandNotFound,
                           str::stream() << "Explain failed due to unknown command: " << cmdName});
                success = false;
                return false;
            }

            const NamespaceString nss(parseNs(dbname, explainObj));
            if (!nss.isValid()) {
                LOG(logger::LogSeverity::Error()) << "nss is invalid.nss name:" << nss.ns();
                success = false;
                return false;
            }

            LOG(3) << "getShardInfoWithQuery. cmdObj" << cmdObj.toString();

            auto status = QueryRequest::makeFromFindCommand(nss, explainObj, true);
            if (!status.isOK()) {
                LOG(logger::LogSeverity::Error())
                    << "cmdObj to QueryRequest is error, reason:" << status.getStatus().toString();
                success = false;
                return false;
            }

            const unique_ptr<QueryRequest>& queryRequest = status.getValue();
            if (queryRequest == nullptr || !queryRequest->validate().isOK()) {
                LOG(logger::LogSeverity::Error())
                    << "QueryRequest is invalid, reason: null or validate is false";
                success = false;
                return false;
            }

            bool print = false;
            if (cmdObj.hasField("print")) {
                print = cmdObj["print"].Bool();
            }

            if (print) {
                LOG(logger::LogSeverity::Info()) << status.getStatus().toString();
            }

            shared_ptr<ChunkManagerEX> manager;
            shared_ptr<Shard> primary;
            {
                auto routingInfoStatus =
                    Grid::get(txn)->catalogCache()->getCollectionRoutingInfo(txn, nss);
                if (routingInfoStatus != ErrorCodes::NamespaceNotFound) {
                    auto routingInfo = uassertStatusOK(std::move(routingInfoStatus));
                    manager = routingInfo.cm();
                    primary = routingInfo.primary();
                }
            }

            set<ShardId> shardIds;
            string vinfo;
            if (manager) {
                if (MONGO_unlikely(print)) {
                    vinfo = str::stream() << "[" << manager->getns() << " @ "
                                          << manager->getVersion().toString() << "]";
                }
                manager->getShardIdsForQuery(
                    txn, queryRequest->getFilter(), queryRequest->getCollation(), &shardIds);
            } else if (primary) {
                if (MONGO_unlikely(print)) {
                    vinfo = str::stream() << "[unsharded @ " << primary->toString() << "]";
                }
                shardIds.insert(primary->getId());
            }

            if (MONGO_unlikely(print)) {
                LOG(3) << vinfo;
            }

            BSONArrayBuilder bsonShardInfos(shardIds.size());
            for (const ShardId& entity : shardIds) {
                if (!entity.isValid()) {
                    continue;
                }
                BSONObjBuilder tmp(1);
                tmp.append("shardName", entity.toString());
                bsonShardInfos.append(tmp.done());
            }
            result.append("shards", bsonShardInfos.arr());
            return true;
        } catch (...) {
            LOG(logger::LogSeverity::Error())
                << "getShardInfoWithQuery unknown error, I catch exception";
                success = false;
            return false;
        }
    }
private:
    unique_ptr<DetailCmdCounter<CMD_NAME>> _detailCmder;
} getShardInfoWithQuery;

}  // namespace
}  // namespace mongo
