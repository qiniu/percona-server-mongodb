#pragma once

#include "mongo/client/replica_set_monitor_manager.h"

namespace mongo {
/**
 * Maintains the replica set monitors associated with the global connection pool.
 */
extern ReplicaSetMonitorManager globalRSMonitorManager;

}  // namespace mongo
