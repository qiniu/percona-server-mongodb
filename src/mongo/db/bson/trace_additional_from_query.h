/*
 * @Author: your name
 * @Date: 2022-03-11 16:17:45
 * @LastEditTime: 2022-03-11 16:27:08
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
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