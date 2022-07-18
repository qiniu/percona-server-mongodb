/*
 * @Author: your name
 * @Date: 2022-03-11 16:17:45
 * @LastEditTime: 2022-03-28 16:15:49
 * @LastEditors: Please set LastEditors
 * @Description: 从query的bson中提取为kodo定制的附加信息bson
 * @FilePath: /percona-server-mongodb/src/mongo/db/bson/trace_additional_from_query.h
 */
#pragma once

#include <cstddef>
#include <set>

#include "mongo/bson/bsonelement_comparator_interface.h"
#include "mongo/bson/bsonobj.h"

namespace mongo {
namespace trace_query {
    bool traceAdditionalInfoFromQuery(BSONObj& query, BSONObj& additional);
}
}