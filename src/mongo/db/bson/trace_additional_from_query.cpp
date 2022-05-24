#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/bson/trace_additional_from_query.h"
namespace mongo {
namespace trace_query {
const std::string KODOMSG = "@kodoMsg@";
bool traceAdditionalInfoFromQuery(BSONObj& query, BSONObj& additional){
    bool ret = false;
    if (query.hasElement(KODOMSG)) {
        additional = query.getObjectField(KODOMSG);
        query = query.removeField(KODOMSG);
        ret = true;
    }
    return ret;
}
}
}