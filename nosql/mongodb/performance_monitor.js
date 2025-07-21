// MongoDB Performance Monitoring Script
// Author: DBA Portfolio
// Purpose: Comprehensive MongoDB performance analysis and monitoring

// =======================================================
// DATABASE AND COLLECTION STATISTICS
// =======================================================

print("=".repeat(60));
print("MongoDB Performance Monitoring Report");
print("Generated: " + new Date().toISOString());
print("Server: " + db.runCommand("connectionStatus").authInfo.authenticatedUsers[0].db + " on " + db.serverStatus().host);
print("=".repeat(60));

// Server information
print("\n1. SERVER INFORMATION");
print("-".repeat(30));
var serverInfo = db.runCommand("buildInfo");
print("MongoDB Version: " + serverInfo.version);
print("Platform: " + serverInfo.buildEnvironment.target_arch + " " + serverInfo.buildEnvironment.target_os);

var serverStatus = db.serverStatus();
print("Uptime: " + Math.floor(serverStatus.uptime / 3600) + " hours");
print("Current Connections: " + serverStatus.connections.current + "/" + serverStatus.connections.available);

// =======================================================
// DATABASE SIZES AND STATISTICS  
// =======================================================

print("\n2. DATABASE STATISTICS");
print("-".repeat(30));

var adminDb = db.getSiblingDB("admin");
var databases = adminDb.runCommand("listDatabases").databases;

databases.forEach(function(database) {
    if (database.name !== "local" && database.name !== "admin" && database.name !== "config") {
        var dbStats = db.getSiblingDB(database.name).runCommand("dbStats");
        print("Database: " + database.name);
        print("  Size: " + (dbStats.dataSize / (1024*1024)).toFixed(2) + " MB");
        print("  Storage: " + (dbStats.storageSize / (1024*1024)).toFixed(2) + " MB");
        print("  Collections: " + dbStats.collections);
        print("  Indexes: " + dbStats.indexes);
        print("  Objects: " + dbStats.objects);
        print("");
    }
});

// =======================================================
// COLLECTION STATISTICS (for current database)
// =======================================================

print("\n3. COLLECTION STATISTICS (Current Database: " + db.getName() + ")");
print("-".repeat(50));

var collections = db.getCollectionNames();
var collectionStats = [];

collections.forEach(function(collName) {
    try {
        var stats = db[collName].stats();
        collectionStats.push({
            name: collName,
            count: stats.count || 0,
            size: stats.size || 0,
            storageSize: stats.storageSize || 0,
            avgObjSize: stats.avgObjSize || 0,
            indexSizes: stats.indexSizes || {},
            totalIndexSize: stats.totalIndexSize || 0
        });
    } catch (e) {
        print("Warning: Could not get stats for collection " + collName);
    }
});

// Sort by size
collectionStats.sort(function(a, b) { return b.size - a.size; });

print("Top Collections by Size:");
collectionStats.slice(0, 10).forEach(function(coll) {
    print("  " + coll.name + ":");
    print("    Documents: " + coll.count.toLocaleString());
    print("    Data Size: " + (coll.size / (1024*1024)).toFixed(2) + " MB");
    print("    Storage Size: " + (coll.storageSize / (1024*1024)).toFixed(2) + " MB");
    print("    Avg Doc Size: " + coll.avgObjSize.toFixed(2) + " bytes");
    print("    Index Size: " + (coll.totalIndexSize / (1024*1024)).toFixed(2) + " MB");
    print("");
});

// =======================================================
// CURRENT OPERATIONS
// =======================================================

print("\n4. CURRENT OPERATIONS");
print("-".repeat(30));

var currentOps = db.currentOp({"active": true});
if (currentOps.inprog.length > 0) {
    print("Active Operations: " + currentOps.inprog.length);
    currentOps.inprog.forEach(function(op, index) {
        if (op.op !== "none") {  // Skip idle connections
            print("  Operation " + (index + 1) + ":");
            print("    OpID: " + op.opid);
            print("    Type: " + op.op);
            print("    Namespace: " + op.ns);
            print("    Duration: " + op.secs_running + " seconds");
            print("    Client: " + (op.client || "N/A"));
            if (op.command) {
                print("    Command: " + JSON.stringify(op.command).substring(0, 100) + "...");
            }
            print("");
        }
    });
} else {
    print("No active operations");
}

// Long running operations (over 10 seconds)
var longOps = db.currentOp({"active": true, "secs_running": {"$gte": 10}});
if (longOps.inprog.length > 0) {
    print("\nLong Running Operations (>10 seconds): " + longOps.inprog.length);
    longOps.inprog.forEach(function(op) {
        print("  OpID " + op.opid + " - " + op.op + " on " + op.ns + " (" + op.secs_running + "s)");
    });
}

// =======================================================
// INDEX USAGE STATISTICS
// =======================================================

print("\n5. INDEX USAGE ANALYSIS");
print("-".repeat(30));

collections.forEach(function(collName) {
    var indexes = db[collName].getIndexes();
    if (indexes.length > 1) {  // More than just _id index
        print("Collection: " + collName);
        
        indexes.forEach(function(index) {
            var indexStats;
            try {
                indexStats = db[collName].aggregate([
                    {"$indexStats": {}}
                ]).toArray();
                
                var currentIndexStats = indexStats.find(function(stat) {
                    return stat.name === index.name;
                });
                
                if (currentIndexStats) {
                    print("  Index: " + index.name);
                    print("    Keys: " + JSON.stringify(index.key));
                    print("    Accesses: " + (currentIndexStats.accesses.ops || 0));
                    print("    Since: " + (currentIndexStats.accesses.since || "N/A"));
                    
                    if (currentIndexStats.accesses.ops === 0 && index.name !== "_id_") {
                        print("    *** UNUSED INDEX - Consider dropping ***");
                    }
                }
            } catch (e) {
                print("  Index: " + index.name + " (stats unavailable)");
            }
        });
        print("");
    }
});

// =======================================================
// QUERY PERFORMANCE ANALYSIS
// =======================================================

print("\n6. RECENT SLOW QUERIES");
print("-".repeat(30));

// Enable profiler temporarily to check for slow queries
var profilingStatus = db.getProfilingStatus();
print("Current Profiling Level: " + profilingStatus.level);
print("Slow Operation Threshold: " + profilingStatus.slowms + "ms");

// Check system.profile collection if profiling is enabled
if (db.system.profile.count() > 0) {
    print("Recent Slow Operations (Top 10):");
    
    db.system.profile.find().sort({ts: -1}).limit(10).forEach(function(op) {
        print("  Timestamp: " + op.ts);
        print("  Duration: " + op.millis + "ms");
        print("  Operation: " + op.op);
        print("  Namespace: " + op.ns);
        if (op.command) {
            print("  Command: " + JSON.stringify(op.command).substring(0, 100) + "...");
        }
        print("  Docs Examined: " + (op.docsExamined || 0));
        print("  Docs Returned: " + (op.nreturned || 0));
        print("");
    });
} else {
    print("No profiling data available. Enable profiling to see slow queries:");
    print("  db.setProfilingLevel(1, {slowms: 100})");
}

// =======================================================
// REPLICATION STATUS
// =======================================================

print("\n7. REPLICATION STATUS");
print("-".repeat(30));

try {
    var replStatus = db.runCommand("replSetGetStatus");
    if (replStatus.ok) {
        print("Replica Set: " + replStatus.set);
        print("Members: " + replStatus.members.length);
        
        replStatus.members.forEach(function(member) {
            print("  " + member.name + " - " + member.stateStr);
            if (member.optimeDate) {
                print("    Last Optime: " + member.optimeDate);
            }
            if (member.lastHeartbeat) {
                print("    Last Heartbeat: " + member.lastHeartbeat);
            }
            if (member.pingMs !== undefined) {
                print("    Ping: " + member.pingMs + "ms");
            }
        });
        
        // Check replication lag
        var primary = replStatus.members.find(function(m) { return m.stateStr === "PRIMARY"; });
        var secondaries = replStatus.members.filter(function(m) { return m.stateStr === "SECONDARY"; });
        
        if (primary && secondaries.length > 0) {
            print("\nReplication Lag:");
            secondaries.forEach(function(secondary) {
                if (secondary.optimeDate && primary.optimeDate) {
                    var lag = (primary.optimeDate.getTime() - secondary.optimeDate.getTime()) / 1000;
                    print("  " + secondary.name + ": " + lag.toFixed(2) + " seconds");
                }
            });
        }
    } else {
        print("Not running in replica set mode");
    }
} catch (e) {
    print("Not running in replica set mode");
}

// =======================================================
// SHARDING STATUS
// =======================================================

print("\n8. SHARDING STATUS");
print("-".repeat(30));

try {
    var shardingEnabled = db.runCommand("ismaster").msg === "isdbgrid";
    if (shardingEnabled) {
        var shardStatus = db.getSiblingDB("config").runCommand("listShards");
        print("Sharded Cluster - Shards: " + shardStatus.shards.length);
        
        shardStatus.shards.forEach(function(shard) {
            print("  " + shard._id + ": " + shard.host);
        });
        
        // Check for unbalanced shards
        print("\nShard Distribution:");
        databases.forEach(function(database) {
            if (database.name !== "local" && database.name !== "admin" && database.name !== "config") {
                var dbName = database.name;
                var collections = db.getSiblingDB(dbName).getCollectionNames();
                
                collections.forEach(function(collName) {
                    try {
                        var shardDist = db.getSiblingDB("config").chunks.aggregate([
                            {"$match": {"ns": dbName + "." + collName}},
                            {"$group": {"_id": "$shard", "chunks": {"$sum": 1}}}
                        ]).toArray();
                        
                        if (shardDist.length > 1) {
                            print("  " + dbName + "." + collName + ":");
                            shardDist.forEach(function(dist) {
                                print("    " + dist._id + ": " + dist.chunks + " chunks");
                            });
                        }
                    } catch (e) {
                        // Collection not sharded
                    }
                });
            }
        });
    } else {
        print("Not running in sharded mode");
    }
} catch (e) {
    print("Not running in sharded mode");
}

// =======================================================
// MEMORY AND CONNECTION USAGE
// =======================================================

print("\n9. MEMORY AND RESOURCE USAGE");
print("-".repeat(40));

var memInfo = serverStatus.mem;
print("Memory Usage:");
print("  Resident: " + memInfo.resident + " MB");
print("  Virtual: " + memInfo.virtual + " MB");
print("  Mapped: " + (memInfo.mapped || 0) + " MB");

var connInfo = serverStatus.connections;
print("\nConnection Usage:");
print("  Current: " + connInfo.current);
print("  Available: " + connInfo.available);
print("  Total Created: " + connInfo.totalCreated);
print("  Utilization: " + ((connInfo.current / (connInfo.current + connInfo.available)) * 100).toFixed(1) + "%");

// =======================================================
// OPCOUNTERS (Operations per second)
// =======================================================

print("\n10. OPERATION COUNTERS");
print("-".repeat(30));

var opCounters = serverStatus.opcounters;
print("Total Operations:");
print("  Insert: " + opCounters.insert.toLocaleString());
print("  Query: " + opCounters.query.toLocaleString());
print("  Update: " + opCounters.update.toLocaleString());
print("  Delete: " + opCounters.delete.toLocaleString());
print("  GetMore: " + opCounters.getmore.toLocaleString());
print("  Command: " + opCounters.command.toLocaleString());

// =======================================================
// LOCK STATISTICS
// =======================================================

if (serverStatus.locks) {
    print("\n11. LOCK STATISTICS");
    print("-".repeat(30));
    
    Object.keys(serverStatus.locks).forEach(function(lockType) {
        var lockData = serverStatus.locks[lockType];
        if (lockData.acquireCount) {
            print(lockType + " locks:");
            Object.keys(lockData.acquireCount).forEach(function(mode) {
                print("  " + mode + ": " + lockData.acquireCount[mode].toLocaleString());
            });
        }
    });
}

// =======================================================
// STORAGE ENGINE STATISTICS
// =======================================================

if (serverStatus.storageEngine) {
    print("\n12. STORAGE ENGINE: " + serverStatus.storageEngine.name);
    print("-".repeat(40));
    
    if (serverStatus.wiredTiger) {
        var wt = serverStatus.wiredTiger;
        print("WiredTiger Statistics:");
        print("  Cache Size: " + (wt.cache["maximum bytes configured"] / (1024*1024)).toFixed(0) + " MB");
        print("  Cache Used: " + (wt.cache["bytes currently in the cache"] / (1024*1024)).toFixed(0) + " MB");
        print("  Cache Hit Ratio: " + ((wt.cache["pages read into cache"] / (wt.cache["pages read into cache"] + wt.cache["pages requested from the cache"])) * 100).toFixed(2) + "%");
        print("  Checkpoint Time: " + (wt.transaction["transaction checkpoint most recent time (msecs)"] || 0) + "ms");
    }
}

// =======================================================
// RECOMMENDATIONS
// =======================================================

print("\n13. RECOMMENDATIONS");
print("-".repeat(30));

var recommendations = [];

// Check connection usage
var connUtilization = (connInfo.current / (connInfo.current + connInfo.available)) * 100;
if (connUtilization > 80) {
    recommendations.push("High connection usage (" + connUtilization.toFixed(1) + "%). Consider connection pooling.");
}

// Check for unused indexes
collections.forEach(function(collName) {
    var indexes = db[collName].getIndexes();
    if (indexes.length > 5) {
        recommendations.push("Collection '" + collName + "' has " + indexes.length + " indexes. Review for unused indexes.");
    }
});

// Check large collections without indexes
collectionStats.forEach(function(coll) {
    if (coll.count > 100000 && Object.keys(coll.indexSizes).length <= 1) {
        recommendations.push("Large collection '" + coll.name + "' (" + coll.count + " docs) has minimal indexing.");
    }
});

// Check profiling
if (profilingStatus.level === 0) {
    recommendations.push("Database profiling is disabled. Enable with db.setProfilingLevel(1, {slowms: 100}).");
}

if (recommendations.length > 0) {
    recommendations.forEach(function(rec, index) {
        print("  " + (index + 1) + ". " + rec);
    });
} else {
    print("No immediate recommendations.");
}

print("\n" + "=".repeat(60));
print("MongoDB Performance Report Complete");
print("=".repeat(60));