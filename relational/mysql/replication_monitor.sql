-- MySQL Replication Monitoring Script
-- Author: DBA Portfolio
-- Purpose: Comprehensive MySQL replication health monitoring

-- ==================================================
-- MASTER SERVER REPLICATION STATUS
-- ==================================================

SELECT 'MASTER STATUS' as Info;
SELECT '===============================================' as Separator;

SHOW MASTER STATUS\G

-- Binary log files and sizes
SELECT 'BINARY LOG FILES' as Info;
SELECT '===============================================' as Separator;

SHOW BINARY LOGS;

-- Master variables
SELECT 'MASTER CONFIGURATION VARIABLES' as Info;
SELECT '===============================================' as Separator;

SELECT 
    variable_name,
    variable_value
FROM performance_schema.global_variables 
WHERE variable_name IN (
    'server_id',
    'log_bin',
    'log_bin_basename',
    'binlog_format',
    'binlog_row_image',
    'sync_binlog',
    'binlog_cache_size',
    'max_binlog_size',
    'expire_logs_days',
    'binlog_expire_logs_seconds',
    'gtid_mode',
    'enforce_gtid_consistency'
)
ORDER BY variable_name;

-- ==================================================
-- SLAVE SERVER REPLICATION STATUS
-- ==================================================

SELECT 'SLAVE STATUS' as Info;
SELECT '===============================================' as Separator;

SHOW SLAVE STATUS\G

-- Detailed slave status analysis
SELECT 'SLAVE STATUS ANALYSIS' as Info;
SELECT '===============================================' as Separator;

SELECT 
    'Slave_IO_State' as Metric,
    CASE 
        WHEN Slave_IO_State = 'Waiting for master to send event' THEN 'HEALTHY'
        WHEN Slave_IO_State = 'Connecting to master' THEN 'CONNECTING'
        WHEN Slave_IO_State = '' THEN 'STOPPED'
        ELSE 'CHECK_REQUIRED'
    END as Status,
    Slave_IO_State as Details
FROM information_schema.replica_host_status
WHERE Channel_name = ''
UNION ALL
SELECT 
    'Slave_IO_Running' as Metric,
    CASE Slave_IO_Running 
        WHEN 'Yes' THEN 'HEALTHY'
        WHEN 'No' THEN 'CRITICAL'
        ELSE 'WARNING'
    END as Status,
    Slave_IO_Running as Details
FROM information_schema.replica_host_status
WHERE Channel_name = ''
UNION ALL
SELECT 
    'Slave_SQL_Running' as Metric,
    CASE Slave_SQL_Running 
        WHEN 'Yes' THEN 'HEALTHY'
        WHEN 'No' THEN 'CRITICAL' 
        ELSE 'WARNING'
    END as Status,
    Slave_SQL_Running as Details
FROM information_schema.replica_host_status
WHERE Channel_name = '';

-- Replication lag calculation
SELECT 'REPLICATION LAG ANALYSIS' as Info;
SELECT '===============================================' as Separator;

SELECT 
    'Seconds_Behind_Master' as Metric,
    CASE 
        WHEN Seconds_Behind_Master IS NULL THEN 'REPLICATION_STOPPED'
        WHEN Seconds_Behind_Master = 0 THEN 'SYNCHRONIZED'
        WHEN Seconds_Behind_Master < 60 THEN 'ACCEPTABLE'
        WHEN Seconds_Behind_Master < 300 THEN 'WARNING'
        ELSE 'CRITICAL'
    END as Status,
    COALESCE(Seconds_Behind_Master, -1) as Lag_Seconds,
    CASE 
        WHEN Seconds_Behind_Master > 0 THEN 
            CONCAT(
                FLOOR(Seconds_Behind_Master / 3600), 'h ',
                FLOOR((Seconds_Behind_Master % 3600) / 60), 'm ',
                (Seconds_Behind_Master % 60), 's'
            )
        ELSE 'N/A'
    END as Lag_Formatted
FROM information_schema.replica_host_status
WHERE Channel_name = '';

-- ==================================================
-- REPLICATION ERRORS AND ISSUES
-- ==================================================

SELECT 'REPLICATION ERRORS' as Info;
SELECT '===============================================' as Separator;

SELECT 
    'Last_IO_Error' as Error_Type,
    CASE 
        WHEN Last_IO_Errno = 0 THEN 'NO_ERROR'
        ELSE 'ERROR_PRESENT'
    END as Status,
    Last_IO_Errno as Error_Number,
    LEFT(Last_IO_Error, 200) as Error_Message
FROM information_schema.replica_host_status
WHERE Channel_name = ''
UNION ALL
SELECT 
    'Last_SQL_Error' as Error_Type,
    CASE 
        WHEN Last_SQL_Errno = 0 THEN 'NO_ERROR'
        ELSE 'ERROR_PRESENT'
    END as Status,
    Last_SQL_Errno as Error_Number,
    LEFT(Last_SQL_Error, 200) as Error_Message
FROM information_schema.replica_host_status
WHERE Channel_name = '';

-- ==================================================
-- GTID STATUS (if enabled)
-- ==================================================

SELECT 'GTID REPLICATION STATUS' as Info;
SELECT '===============================================' as Separator;

SELECT 
    variable_name,
    variable_value
FROM performance_schema.global_variables 
WHERE variable_name IN (
    'gtid_mode',
    'enforce_gtid_consistency',
    'gtid_executed',
    'gtid_purged',
    'gtid_owned'
)
ORDER BY variable_name;

-- GTID gaps analysis (if GTID is enabled)
SELECT 'GTID EXECUTED SETS' as Info;
SELECT '===============================================' as Separator;

SELECT @@global.gtid_executed as Master_GTID_Executed;

-- For slave servers, show retrieved and executed GTIDs
SELECT 
    Retrieved_Gtid_Set as Retrieved_GTID,
    Executed_Gtid_Set as Executed_GTID
FROM information_schema.replica_host_status
WHERE Channel_name = '';

-- ==================================================
-- REPLICATION PERFORMANCE METRICS
-- ==================================================

SELECT 'REPLICATION PERFORMANCE METRICS' as Info;
SELECT '===============================================' as Separator;

-- Binary log cache usage
SELECT 
    'Binlog_cache_use' as Metric,
    variable_value as Value,
    'Total number of transactions that used the binary log cache' as Description
FROM performance_schema.global_status 
WHERE variable_name = 'Binlog_cache_use'
UNION ALL
SELECT 
    'Binlog_cache_disk_use' as Metric,
    variable_value as Value,
    'Number of transactions that exceeded binlog_cache_size' as Description
FROM performance_schema.global_status 
WHERE variable_name = 'Binlog_cache_disk_use'
UNION ALL
SELECT 
    'Binlog_stmt_cache_use' as Metric,
    variable_value as Value,
    'Total number of statements that used the binary log statement cache' as Description
FROM performance_schema.global_status 
WHERE variable_name = 'Binlog_stmt_cache_use'
UNION ALL
SELECT 
    'Binlog_stmt_cache_disk_use' as Metric,
    variable_value as Value,
    'Number of statements that exceeded binlog_stmt_cache_size' as Description
FROM performance_schema.global_status 
WHERE variable_name = 'Binlog_stmt_cache_disk_use';

-- Slave performance metrics
SELECT 'SLAVE PERFORMANCE METRICS' as Info;
SELECT '===============================================' as Separator;

SELECT 
    'Slave_open_temp_tables' as Metric,
    variable_value as Value,
    'Number of temp tables currently open by the slave SQL thread' as Description
FROM performance_schema.global_status 
WHERE variable_name = 'Slave_open_temp_tables'
UNION ALL
SELECT 
    'Slave_retried_transactions' as Metric,
    variable_value as Value,
    'Total number of times slave SQL thread retried transactions' as Description
FROM performance_schema.global_status 
WHERE variable_name = 'Slave_retried_transactions'
UNION ALL
SELECT 
    'Slave_running' as Metric,
    variable_value as Value,
    'ON if this is a slave that is connected to a master' as Description
FROM performance_schema.global_status 
WHERE variable_name = 'Slave_running';

-- ==================================================
-- REPLICATION TOPOLOGY AND CONNECTIONS
-- ==================================================

SELECT 'REPLICATION TOPOLOGY' as Info;
SELECT '===============================================' as Separator;

-- Master information (from slave perspective)
SELECT 
    Master_Host as Master_Host,
    Master_Port as Master_Port,
    Master_User as Master_User,
    Master_Log_File as Current_Master_Log_File,
    Read_Master_Log_Pos as Current_Master_Log_Position
FROM information_schema.replica_host_status
WHERE Channel_name = '';

-- Connection status
SELECT 'CONNECTION STATUS' as Info;
SELECT '===============================================' as Separator;

SELECT 
    Master_Server_Id as Master_Server_ID,
    Connect_Retry as Connection_Retry_Interval,
    Master_Retry_Count as Master_Retry_Count,
    CASE 
        WHEN Master_SSL_Allowed = 'Yes' THEN 'SSL_ENABLED'
        ELSE 'SSL_DISABLED'
    END as SSL_Status
FROM information_schema.replica_host_status
WHERE Channel_name = '';

-- ==================================================
-- RELAY LOG INFORMATION
-- ==================================================

SELECT 'RELAY LOG STATUS' as Info;
SELECT '===============================================' as Separator;

SELECT 
    Relay_Log_File as Current_Relay_Log_File,
    Relay_Log_Pos as Current_Relay_Log_Position,
    Relay_Master_Log_File as Master_Log_File_Being_Read,
    Exec_Master_Log_Pos as Master_Log_Position_Executed
FROM information_schema.replica_host_status
WHERE Channel_name = '';

-- Relay log files
SHOW RELAYLOG EVENTS LIMIT 5;

-- ==================================================
-- REPLICATION FILTERS
-- ==================================================

SELECT 'REPLICATION FILTERS' as Info;
SELECT '===============================================' as Separator;

-- Show replication filters
SELECT 
    COALESCE(Replicate_Do_DB, 'NOT_SET') as Replicate_Do_DB,
    COALESCE(Replicate_Ignore_DB, 'NOT_SET') as Replicate_Ignore_DB,
    COALESCE(Replicate_Do_Table, 'NOT_SET') as Replicate_Do_Table,
    COALESCE(Replicate_Ignore_Table, 'NOT_SET') as Replicate_Ignore_Table,
    COALESCE(Replicate_Wild_Do_Table, 'NOT_SET') as Replicate_Wild_Do_Table,
    COALESCE(Replicate_Wild_Ignore_Table, 'NOT_SET') as Replicate_Wild_Ignore_Table
FROM information_schema.replica_host_status
WHERE Channel_name = '';

-- ==================================================
-- MULTI-SOURCE REPLICATION (MySQL 5.7+)
-- ==================================================

SELECT 'MULTI-SOURCE REPLICATION CHANNELS' as Info;
SELECT '===============================================' as Separator;

SELECT 
    Channel_name as Channel_Name,
    Host as Master_Host,
    Port as Master_Port,
    User as Master_User,
    CASE 
        WHEN Service_State = 'ON' THEN 'RUNNING'
        ELSE 'STOPPED'
    END as Channel_Status
FROM performance_schema.replication_connection_configuration
WHERE Channel_name != '';

-- Channel status details
SELECT 
    Channel_name as Channel_Name,
    Thread_Id,
    Service_State as IO_Thread_State,
    Last_Error_Number as Last_IO_Error_Number,
    LEFT(Last_Error_Message, 100) as Last_IO_Error_Message,
    Last_Error_Timestamp as Last_Error_Time
FROM performance_schema.replication_connection_status
WHERE Channel_name != '';

-- ==================================================
-- REPLICATION HEALTH SUMMARY
-- ==================================================

SELECT 'REPLICATION HEALTH SUMMARY' as Info;
SELECT '===============================================' as Separator;

SELECT 
    'Overall_Replication_Health' as Check_Name,
    CASE 
        WHEN (SELECT COUNT(*) FROM information_schema.replica_host_status 
              WHERE Channel_name = '' AND Slave_IO_Running = 'Yes' AND Slave_SQL_Running = 'Yes') > 0
        THEN 'HEALTHY'
        WHEN (SELECT COUNT(*) FROM information_schema.replica_host_status 
              WHERE Channel_name = '' AND (Slave_IO_Running != 'Yes' OR Slave_SQL_Running != 'Yes')) > 0
        THEN 'UNHEALTHY'
        ELSE 'NOT_A_SLAVE'
    END as Status,
    CASE 
        WHEN (SELECT COUNT(*) FROM information_schema.replica_host_status WHERE Channel_name = '') = 0
        THEN 'This server is not configured as a replication slave'
        ELSE 'Check individual metrics above for details'
    END as Details
UNION ALL
SELECT 
    'Replication_Lag_Status' as Check_Name,
    CASE 
        WHEN (SELECT Seconds_Behind_Master FROM information_schema.replica_host_status WHERE Channel_name = '') IS NULL
        THEN 'REPLICATION_STOPPED'
        WHEN (SELECT Seconds_Behind_Master FROM information_schema.replica_host_status WHERE Channel_name = '') = 0
        THEN 'SYNCHRONIZED'
        WHEN (SELECT Seconds_Behind_Master FROM information_schema.replica_host_status WHERE Channel_name = '') < 60
        THEN 'ACCEPTABLE_LAG'
        ELSE 'HIGH_LAG'
    END as Status,
    CONCAT(
        COALESCE((SELECT Seconds_Behind_Master FROM information_schema.replica_host_status WHERE Channel_name = ''), -1),
        ' seconds behind master'
    ) as Details
UNION ALL
SELECT 
    'Replication_Errors' as Check_Name,
    CASE 
        WHEN (SELECT COUNT(*) FROM information_schema.replica_host_status 
              WHERE Channel_name = '' AND (Last_IO_Errno != 0 OR Last_SQL_Errno != 0)) > 0
        THEN 'ERRORS_PRESENT'
        WHEN (SELECT COUNT(*) FROM information_schema.replica_host_status WHERE Channel_name = '') = 0
        THEN 'NOT_APPLICABLE'
        ELSE 'NO_ERRORS'
    END as Status,
    CASE 
        WHEN (SELECT COUNT(*) FROM information_schema.replica_host_status WHERE Channel_name = '') = 0
        THEN 'Not a slave server'
        ELSE 'Check error details above'
    END as Details;

-- ==================================================
-- RECOMMENDATIONS
-- ==================================================

SELECT 'REPLICATION RECOMMENDATIONS' as Info;  
SELECT '===============================================' as Separator;

-- Check binlog format
SELECT 
    'Binlog_Format_Check' as Recommendation_Type,
    CASE 
        WHEN (SELECT variable_value FROM performance_schema.global_variables WHERE variable_name = 'binlog_format') = 'ROW'
        THEN 'OPTIMAL - Using ROW format for better consistency'
        WHEN (SELECT variable_value FROM performance_schema.global_variables WHERE variable_name = 'binlog_format') = 'MIXED'  
        THEN 'ACCEPTABLE - Consider ROW format for better consistency'
        ELSE 'SUBOPTIMAL - Consider using ROW or MIXED format'
    END as Assessment,
    (SELECT variable_value FROM performance_schema.global_variables WHERE variable_name = 'binlog_format') as Current_Value

UNION ALL

-- Check sync_binlog
SELECT 
    'Sync_Binlog_Check' as Recommendation_Type,
    CASE 
        WHEN (SELECT variable_value FROM performance_schema.global_variables WHERE variable_name = 'sync_binlog') = '1'
        THEN 'OPTIMAL - Maximum durability enabled'
        WHEN (SELECT variable_value FROM performance_schema.global_variables WHERE variable_name = 'sync_binlog') = '0'
        THEN 'RISKY - Consider setting sync_binlog=1 for durability'
        ELSE 'SUBOPTIMAL - Consider sync_binlog=1 for maximum durability'
    END as Assessment,
    (SELECT variable_value FROM performance_schema.global_variables WHERE variable_name = 'sync_binlog') as Current_Value

UNION ALL

-- Check GTID mode
SELECT 
    'GTID_Mode_Check' as Recommendation_Type,
    CASE 
        WHEN (SELECT variable_value FROM performance_schema.global_variables WHERE variable_name = 'gtid_mode') = 'ON'
        THEN 'EXCELLENT - GTID enabled for easier failover management'
        ELSE 'RECOMMENDED - Consider enabling GTID mode for easier management'
    END as Assessment,
    (SELECT variable_value FROM performance_schema.global_variables WHERE variable_name = 'gtid_mode') as Current_Value;

-- End of replication monitoring script
SELECT 'REPLICATION MONITORING COMPLETE' as Status;
SELECT CONCAT('Report generated at: ', NOW()) as Timestamp;