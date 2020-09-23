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
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

class MongosDumpChunksInfoCmd : public Command {
public:
    MongosDumpChunksInfoCmd() : Command("dumpchunks", false, "dumpchunks") {}

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
        help << " dump chunks in mongos's memory";
    }

    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::dumpChunks);
        out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
    }

    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
                         
        const NamespaceString nss(parseNs(dbname, cmdObj));
        log()<<"dump chunks. cmdObj"<<cmdObj.toString();
        const int start = cmdObj["start"].numberInt();
        const int limit = cmdObj["limit"].numberInt();
        bool print = false;
        if (cmdObj.hasField("print")){
            print = true;
        }
        std::shared_ptr<ChunkManagerEX>  cm;
        if(start == 0){ //从0开始遍历的默认刷一下路由
            auto routingInfo = uassertStatusOK(Grid::get(txn)->catalogCache()->getShardedCollectionRoutingInfoWithRefresh(txn, nss));
            cm = routingInfo.cm();
        }else{
            auto routingInfo = uassertStatusOK(Grid::get(txn)->catalogCache()->getCollectionRoutingInfo(txn, nss));
            cm = routingInfo.cm();
        }
        
        auto iterator_result = cm->iteratorChunks(start, limit, print);
        if (iterator_result->hashErr){
            errmsg = iterator_result->errmsg;
            return false;
        }else{
            result.append("chunks",iterator_result->bson.arr());
            result.append("chunksSize",iterator_result->chunksSize);
        }
       
        return true;
    }

} dumpChunks;

}  // namespace
}  // namespace mongo
