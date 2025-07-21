-- PostgreSQL Performance Monitoring Queries
-- Author: DBA Portfolio
-- Purpose: Comprehensive performance monitoring and analysis

-- =====================================================
-- ACTIVE CONNECTIONS AND QUERIES
-- =====================================================

-- Current active connections
SELECT 
    pid,
    usename as username,
    application_name,
    client_addr,
    backend_start,
    state,
    query_start,
    NOW() - query_start AS query_duration,
    LEFT(query, 100) as current_query
FROM pg_stat_activity 
WHERE state = 'active'
  AND pid != pg_backend_pid()
ORDER BY query_duration DESC;

-- Long running queries (over 5 minutes)
SELECT 
    pid,
    usename,
    client_addr,
    query_start,
    NOW() - query_start AS duration,
    LEFT(query, 200) as query_text
FROM pg_stat_activity 
WHERE state = 'active'
  AND NOW() - query_start > INTERVAL '5 minutes'
  AND pid != pg_backend_pid()
ORDER BY duration DESC;

-- =====================================================
-- DATABASE SIZE AND GROWTH
-- =====================================================

-- Database sizes
SELECT 
    d.datname as database_name,
    pg_size_pretty(pg_database_size(d.datname)) as size,
    pg_database_size(d.datname) as size_bytes
FROM pg_database d
WHERE d.datistemplate = false
ORDER BY pg_database_size(d.datname) DESC;

-- Table sizes in current database
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size,
    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
FROM pg_tables 
WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 20;

-- =====================================================
-- INDEX USAGE AND EFFICIENCY
-- =====================================================

-- Index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    CASE 
        WHEN idx_tup_read = 0 THEN 0
        ELSE ROUND((idx_tup_fetch::numeric / idx_tup_read::numeric) * 100, 2)
    END as fetch_ratio_percent
FROM pg_stat_user_indexes
ORDER BY idx_tup_read DESC;

-- Unused indexes (potential candidates for removal)
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE idx_scan = 0
  AND pg_relation_size(indexrelid) > 1024*1024  -- Indexes larger than 1MB
ORDER BY pg_relation_size(indexrelid) DESC;

-- Missing indexes (tables with sequential scans)
SELECT 
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    n_tup_ins + n_tup_upd + n_tup_del as total_writes,
    CASE 
        WHEN seq_scan = 0 THEN 0
        ELSE ROUND((seq_tup_read::numeric / seq_scan::numeric), 2)
    END as avg_seq_read_per_scan
FROM pg_stat_user_tables 
WHERE seq_scan > 100
  AND seq_tup_read > 1000
ORDER BY seq_tup_read DESC;

-- =====================================================
-- QUERY PERFORMANCE STATISTICS
-- =====================================================

-- Top queries by execution time (requires pg_stat_statements extension)
SELECT 
    LEFT(query, 100) as query_snippet,
    calls,
    total_time,
    mean_time,
    rows,
    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements 
ORDER BY total_time DESC 
LIMIT 10;

-- Top queries by calls
SELECT 
    LEFT(query, 100) as query_snippet,
    calls,
    total_time,
    mean_time,
    rows
FROM pg_stat_statements 
ORDER BY calls DESC 
LIMIT 10;

-- =====================================================
-- CACHE HIT RATIOS
-- =====================================================

-- Buffer cache hit ratio (should be > 95%)
SELECT 
    ROUND(
        100.0 * sum(blks_hit) / (sum(blks_hit) + sum(blks_read)), 2
    ) AS cache_hit_ratio
FROM pg_stat_database;

-- Individual database cache hit ratios
SELECT 
    datname,
    ROUND(
        100.0 * blks_hit / (blks_hit + blks_read), 2
    ) AS cache_hit_ratio,
    blks_hit,
    blks_read
FROM pg_stat_database 
WHERE blks_read > 0
ORDER BY cache_hit_ratio ASC;

-- =====================================================
-- LOCKS AND BLOCKING
-- =====================================================

-- Current locks
SELECT 
    t.schemaname,
    t.tablename,
    l.locktype,
    l.mode,
    l.granted,
    a.usename,
    a.query,
    a.query_start,
    age(now(), a.query_start) AS "age"
FROM pg_stat_all_tables t
LEFT JOIN pg_locks l ON l.relation = t.relid
LEFT JOIN pg_stat_activity a ON l.pid = a.pid
WHERE l.granted = 'f'  -- Only show waiting locks
ORDER BY a.query_start;

-- Blocking queries
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS blocking_statement
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.GRANTED;

-- =====================================================
-- VACUUM AND ANALYZE STATISTICS
-- =====================================================

-- Tables that need vacuum
SELECT 
    schemaname,
    tablename,
    n_dead_tup,
    n_live_tup,
    ROUND((n_dead_tup::float / NULLIF(n_live_tup::float, 0)) * 100, 2) AS dead_tuple_percent,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables 
WHERE n_dead_tup > 1000
ORDER BY dead_tuple_percent DESC;

-- Tables that need analyze
SELECT 
    schemaname,
    tablename,
    n_mod_since_analyze,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables 
WHERE n_mod_since_analyze > 1000
ORDER BY n_mod_since_analyze DESC;

-- =====================================================
-- REPLICATION STATUS (if applicable)
-- =====================================================

-- Replication lag (for streaming replication)
SELECT 
    client_addr,
    client_hostname,
    client_port,
    state,
    sent_location,
    write_location,
    flush_location,
    replay_location,
    write_lag,
    flush_lag,
    replay_lag
FROM pg_stat_replication;

-- =====================================================
-- CHECKPOINT AND WAL STATISTICS
-- =====================================================

-- Checkpoint statistics
SELECT 
    checkpoints_timed,
    checkpoints_req,
    checkpoint_write_time,
    checkpoint_sync_time,
    buffers_checkpoint,
    buffers_clean,
    maxwritten_clean,
    buffers_backend,
    buffers_backend_fsync,
    buffers_alloc,
    stats_reset
FROM pg_stat_bgwriter;

-- =====================================================
-- CONNECTION STATISTICS
-- =====================================================

-- Connection counts by database
SELECT 
    datname,
    numbackends as current_connections,
    datconnlimit as max_connections
FROM pg_stat_database
ORDER BY numbackends DESC;

-- Connection counts by user
SELECT 
    usename,
    COUNT(*) as connection_count
FROM pg_stat_activity
GROUP BY usename
ORDER BY connection_count DESC;