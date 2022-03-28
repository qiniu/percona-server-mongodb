/*
 * @Author: your name
 * @Date: 2022-03-11 16:18:11
 * @LastEditTime: 2022-03-11 17:00:50
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /percona-server-mongodb/src/mongo/db/bson/trace_additional_from_query.cpp
 */
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/bson/trace_additional_from_query.h"
namespace mongo {
namespace trace_query {
bool traceAdditionalInfoFromQuery(BSONObj& query, BSONObj& additional){
    bool ret = false;
    if (query.hasElement("@kodoMsg@")) {
        additional = query.getObjectField("@kodoMsg@");
        query = query.removeField("@kodoMsg@");
        ret = true;
    }
    return ret;
}
}
}