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