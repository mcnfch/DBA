#!/bin/bash
# InfluxDB Backup Script (Shell)
# Author: DBA Portfolio
# Purpose: Comprehensive InfluxDB backup automation for time-series data

set -euo pipefail

# Default configuration
INFLUX_HOST="${INFLUX_HOST:-localhost}"
INFLUX_PORT="${INFLUX_PORT:-8086}"
INFLUX_USERNAME="${INFLUX_USERNAME:-admin}"
INFLUX_PASSWORD="${INFLUX_PASSWORD:-}"
INFLUX_TOKEN="${INFLUX_TOKEN:-}"
INFLUX_ORG="${INFLUX_ORG:-}"
INFLUX_VERSION="${INFLUX_VERSION:-2}"  # 1 or 2
BACKUP_DIR="${BACKUP_DIR:-/var/backups/influxdb}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
COMPRESS="${COMPRESS:-true}"
PARALLEL="${PARALLEL:-true}"
LOG_FILE="${LOG_FILE:-$BACKUP_DIR/influxdb_backup.log}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging function
log() {
    local level=$1
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        ERROR)   echo -e "${RED}[$timestamp] ERROR: $message${NC}" | tee -a "$LOG_FILE" ;;
        WARN)    echo -e "${YELLOW}[$timestamp] WARN: $message${NC}" | tee -a "$LOG_FILE" ;;
        INFO)    echo -e "${GREEN}[$timestamp] INFO: $message${NC}" | tee -a "$LOG_FILE" ;;
        DEBUG)   echo -e "${BLUE}[$timestamp] DEBUG: $message${NC}" | tee -a "$LOG_FILE" ;;
        *)       echo "[$timestamp] $message" | tee -a "$LOG_FILE" ;;
    esac
}

# Usage function
usage() {
    cat << EOF
InfluxDB Backup Script

Usage: $0 [OPTIONS]

Options:
    -h, --host         InfluxDB host (default: localhost)
    -p, --port         InfluxDB port (default: 8086)
    -u, --username     Username (v1.x)
    -w, --password     Password (v1.x)
    -t, --token        Authentication token (v2.x)
    -o, --org          Organization (v2.x)
    -d, --database     Database name (v1.x) or bucket name (v2.x)
    -v, --version      InfluxDB version: 1 or 2 (default: 2)
    -b, --backup-dir   Backup directory (default: /var/backups/influxdb)
    -r, --retention    Retention days (default: 7)
    --compress         Enable compression (default: true)
    --parallel         Enable parallel backup (default: true)
    --help            Show this help

Environment Variables:
    INFLUX_HOST       InfluxDB host
    INFLUX_PORT       InfluxDB port  
    INFLUX_USERNAME   Username (v1.x)
    INFLUX_PASSWORD   Password (v1.x)
    INFLUX_TOKEN      Authentication token (v2.x)
    INFLUX_ORG        Organization (v2.x)
    BACKUP_DIR        Backup directory
    RETENTION_DAYS    Backup retention days

Examples:
    # InfluxDB v1.x
    $0 -v 1 -u admin -w password -d mydb
    
    # InfluxDB v2.x
    $0 -v 2 -t mytoken -o myorg
    
    # Backup specific bucket in v2.x
    $0 -v 2 -t mytoken -o myorg -d mybucket
EOF
}

# Check dependencies
check_dependencies() {
    local deps=()
    
    if [[ "$INFLUX_VERSION" == "1" ]]; then
        deps=("influxd" "influx_inspect")
    else
        deps=("influx")
    fi
    
    if [[ "$COMPRESS" == "true" ]]; then
        deps+=("gzip" "tar")
    fi
    
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            log ERROR "Required dependency '$dep' not found"
            exit 1
        fi
    done
    
    log INFO "All dependencies satisfied"
}

# Test InfluxDB connection
test_connection() {
    log INFO "Testing InfluxDB connection to $INFLUX_HOST:$INFLUX_PORT"
    
    if [[ "$INFLUX_VERSION" == "1" ]]; then
        # InfluxDB 1.x connection test
        local auth_params=""
        if [[ -n "$INFLUX_USERNAME" && -n "$INFLUX_PASSWORD" ]]; then
            auth_params="-username $INFLUX_USERNAME -password $INFLUX_PASSWORD"
        fi
        
        if echo "SHOW DATABASES" | influx -host "$INFLUX_HOST" -port "$INFLUX_PORT" $auth_params &>/dev/null; then
            log INFO "InfluxDB v1.x connection successful"
            return 0
        else
            log ERROR "InfluxDB v1.x connection failed"
            return 1
        fi
    else
        # InfluxDB 2.x connection test
        if [[ -z "$INFLUX_TOKEN" ]]; then
            log ERROR "Token required for InfluxDB v2.x"
            return 1
        fi
        
        export INFLUX_HOST_URL="http://$INFLUX_HOST:$INFLUX_PORT"
        export INFLUX_TOKEN="$INFLUX_TOKEN"
        
        if influx ping &>/dev/null; then
            log INFO "InfluxDB v2.x connection successful"
            return 0
        else
            log ERROR "InfluxDB v2.x connection failed"
            return 1
        fi
    fi
}

# Get InfluxDB information
get_influxdb_info() {
    log INFO "Gathering InfluxDB information"
    
    if [[ "$INFLUX_VERSION" == "1" ]]; then
        # InfluxDB 1.x info
        local auth_params=""
        if [[ -n "$INFLUX_USERNAME" && -n "$INFLUX_PASSWORD" ]]; then
            auth_params="-username $INFLUX_USERNAME -password $INFLUX_PASSWORD"
        fi
        
        log INFO "InfluxDB v1.x Server Information:"
        echo "SHOW DIAGNOSTICS" | influx -host "$INFLUX_HOST" -port "$INFLUX_PORT" $auth_params | head -20
        
        log INFO "Available Databases:"
        echo "SHOW DATABASES" | influx -host "$INFLUX_HOST" -port "$INFLUX_PORT" $auth_params
        
    else
        # InfluxDB 2.x info
        log INFO "InfluxDB v2.x Server Information:"
        influx server-config 2>/dev/null || log WARN "Could not retrieve server config"
        
        log INFO "Available Buckets:"
        influx bucket list 2>/dev/null || log WARN "Could not list buckets"
    fi
}

# Create backup directory
create_backup_dir() {
    if [[ ! -d "$BACKUP_DIR" ]]; then
        log INFO "Creating backup directory: $BACKUP_DIR"
        mkdir -p "$BACKUP_DIR"
    fi
    
    # Create timestamped subdirectory
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    BACKUP_SUBDIR="$BACKUP_DIR/influxdb_backup_$timestamp"
    mkdir -p "$BACKUP_SUBDIR"
    
    log INFO "Backup will be stored in: $BACKUP_SUBDIR"
}

# Backup InfluxDB v1.x
backup_influxdb_v1() {
    local database="$1"
    
    log INFO "Starting InfluxDB v1.x backup"
    
    # Prepare authentication
    local auth_params=""
    if [[ -n "$INFLUX_USERNAME" && -n "$INFLUX_PASSWORD" ]]; then
        auth_params="-username $INFLUX_USERNAME -password $INFLUX_PASSWORD"
    fi
    
    # Create backup using influxd backup command
    local backup_cmd="influxd backup"
    backup_cmd+=" -host $INFLUX_HOST:$INFLUX_PORT"
    
    if [[ -n "$database" ]]; then
        backup_cmd+=" -database $database"
    fi
    
    if [[ "$PARALLEL" == "true" ]]; then
        backup_cmd+=" -parallel"
    fi
    
    backup_cmd+=" $BACKUP_SUBDIR"
    
    log INFO "Executing backup command: $backup_cmd"
    
    local start_time=$(date +%s)
    
    if eval "$backup_cmd"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log INFO "Backup completed successfully in ${duration} seconds"
        
        # Backup metadata
        backup_metadata_v1 "$database"
        
        return 0
    else
        log ERROR "Backup failed"
        return 1
    fi
}

# Backup metadata for v1.x
backup_metadata_v1() {
    local database="$1"
    local metadata_file="$BACKUP_SUBDIR/metadata.txt"
    
    log INFO "Backing up metadata to: $metadata_file"
    
    local auth_params=""
    if [[ -n "$INFLUX_USERNAME" && -n "$INFLUX_PASSWORD" ]]; then
        auth_params="-username $INFLUX_USERNAME -password $INFLUX_PASSWORD"
    fi
    
    {
        echo "InfluxDB v1.x Backup Metadata"
        echo "Generated: $(date)"
        echo "Host: $INFLUX_HOST:$INFLUX_PORT"
        echo "Database: ${database:-ALL}"
        echo ""
        
        echo "=== DATABASES ==="
        echo "SHOW DATABASES" | influx -host "$INFLUX_HOST" -port "$INFLUX_PORT" $auth_params
        echo ""
        
        if [[ -n "$database" ]]; then
            echo "=== RETENTION POLICIES for $database ==="
            echo "SHOW RETENTION POLICIES ON $database" | influx -host "$INFLUX_HOST" -port "$INFLUX_PORT" $auth_params
            echo ""
            
            echo "=== MEASUREMENTS for $database ==="
            echo "SHOW MEASUREMENTS ON $database" | influx -host "$INFLUX_HOST" -port "$INFLUX_PORT" $auth_params | head -50
            echo ""
            
            echo "=== SERIES CARDINALITY for $database ==="
            echo "SHOW SERIES CARDINALITY ON $database" | influx -host "$INFLUX_HOST" -port "$INFLUX_PORT" $auth_params
        fi
        
        echo "=== USERS ==="
        echo "SHOW USERS" | influx -host "$INFLUX_HOST" -port "$INFLUX_PORT" $auth_params
        
    } > "$metadata_file" 2>&1
}

# Backup InfluxDB v2.x
backup_influxdb_v2() {
    local bucket="$1"
    
    log INFO "Starting InfluxDB v2.x backup"
    
    # Prepare backup command
    local backup_cmd="influx backup"
    backup_cmd+=" --host http://$INFLUX_HOST:$INFLUX_PORT"
    backup_cmd+=" --token $INFLUX_TOKEN"
    
    if [[ -n "$INFLUX_ORG" ]]; then
        backup_cmd+=" --org $INFLUX_ORG"
    fi
    
    if [[ -n "$bucket" ]]; then
        backup_cmd+=" --bucket $bucket"
    fi
    
    backup_cmd+=" $BACKUP_SUBDIR"
    
    log INFO "Executing backup command: $backup_cmd"
    
    local start_time=$(date +%s)
    
    if eval "$backup_cmd"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log INFO "Backup completed successfully in ${duration} seconds"
        
        # Backup metadata
        backup_metadata_v2 "$bucket"
        
        return 0
    else
        log ERROR "Backup failed"
        return 1
    fi
}

# Backup metadata for v2.x
backup_metadata_v2() {
    local bucket="$1"
    local metadata_file="$BACKUP_SUBDIR/metadata.txt"
    
    log INFO "Backing up metadata to: $metadata_file"
    
    {
        echo "InfluxDB v2.x Backup Metadata"
        echo "Generated: $(date)"
        echo "Host: $INFLUX_HOST:$INFLUX_PORT"
        echo "Organization: ${INFLUX_ORG:-ALL}"
        echo "Bucket: ${bucket:-ALL}"
        echo ""
        
        echo "=== SERVER INFO ==="
        influx server-config 2>/dev/null || echo "Server config not available"
        echo ""
        
        echo "=== ORGANIZATIONS ==="
        influx org list 2>/dev/null || echo "Organizations not available"
        echo ""
        
        echo "=== BUCKETS ==="
        influx bucket list 2>/dev/null || echo "Buckets not available"
        echo ""
        
        echo "=== USERS ==="
        influx user list 2>/dev/null || echo "Users not available"
        echo ""
        
        if [[ -n "$bucket" ]]; then
            echo "=== BUCKET SCHEMA for $bucket ==="
            influx bucket list --name "$bucket" 2>/dev/null || echo "Bucket schema not available"
        fi
        
    } > "$metadata_file" 2>&1
}

# Compress backup
compress_backup() {
    if [[ "$COMPRESS" != "true" ]]; then
        return 0
    fi
    
    log INFO "Compressing backup directory..."
    
    local compressed_file="$BACKUP_SUBDIR.tar.gz"
    local start_time=$(date +%s)
    
    if tar -czf "$compressed_file" -C "$(dirname "$BACKUP_SUBDIR")" "$(basename "$BACKUP_SUBDIR")"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log INFO "Compression completed in ${duration} seconds"
        
        # Remove uncompressed directory
        rm -rf "$BACKUP_SUBDIR"
        
        # Update BACKUP_SUBDIR to compressed file for further operations
        BACKUP_SUBDIR="$compressed_file"
        
        return 0
    else
        log ERROR "Compression failed"
        return 1
    fi
}

# Verify backup
verify_backup() {
    log INFO "Verifying backup integrity..."
    
    if [[ "$BACKUP_SUBDIR" == *.tar.gz ]]; then
        # Verify compressed backup
        if tar -tzf "$BACKUP_SUBDIR" >/dev/null 2>&1; then
            log INFO "Compressed backup verification successful"
            return 0
        else
            log ERROR "Compressed backup verification failed"
            return 1
        fi
    else
        # Verify directory backup
        if [[ -d "$BACKUP_SUBDIR" ]]; then
            local file_count=$(find "$BACKUP_SUBDIR" -type f | wc -l)
            if [[ $file_count -gt 0 ]]; then
                log INFO "Backup verification successful ($file_count files found)"
                return 0
            else
                log ERROR "Backup verification failed (no files found)"
                return 1
            fi
        else
            log ERROR "Backup directory not found"
            return 1
        fi
    fi
}

# Get backup size
get_backup_size() {
    if [[ -f "$BACKUP_SUBDIR" ]]; then
        # File size
        du -h "$BACKUP_SUBDIR" | cut -f1
    elif [[ -d "$BACKUP_SUBDIR" ]]; then
        # Directory size
        du -sh "$BACKUP_SUBDIR" | cut -f1
    else
        echo "0B"
    fi
}

# Cleanup old backups
cleanup_old_backups() {
    log INFO "Cleaning up backups older than $RETENTION_DAYS days"
    
    local deleted_count=0
    
    # Find and remove old backup directories and files
    while IFS= read -r -d '' item; do
        rm -rf "$item"
        log INFO "Removed old backup: $(basename "$item")"
        ((deleted_count++))
    done < <(find "$BACKUP_DIR" -maxdepth 1 \( -name "influxdb_backup_*" -o -name "influxdb_backup_*.tar.gz" \) -mtime +$RETENTION_DAYS -print0 2>/dev/null)
    
    if [[ $deleted_count -gt 0 ]]; then
        log INFO "Removed $deleted_count old backup(s)"
    else
        log INFO "No old backups to remove"
    fi
}

# Generate backup report
generate_backup_report() {
    local database_or_bucket="$1"
    local status="$2"
    local backup_size="$3"
    local duration="$4"
    
    local report_file="$BACKUP_DIR/influxdb_backup_report.csv"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Create header if file doesn't exist
    if [[ ! -f "$report_file" ]]; then
        echo "Timestamp,Host,Version,Database_Bucket,Status,Size,Duration_sec,BackupPath" > "$report_file"
    fi
    
    echo "$timestamp,$INFLUX_HOST:$INFLUX_PORT,v$INFLUX_VERSION,$database_or_bucket,$status,$backup_size,$duration,$BACKUP_SUBDIR" >> "$report_file"
    
    log INFO "Backup report updated: $report_file"
}

# Main function
main() {
    local database_or_bucket=""
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--host)
                INFLUX_HOST="$2"
                shift 2
                ;;
            -p|--port)
                INFLUX_PORT="$2"
                shift 2
                ;;
            -u|--username)
                INFLUX_USERNAME="$2"
                shift 2
                ;;
            -w|--password)
                INFLUX_PASSWORD="$2"
                shift 2
                ;;
            -t|--token)
                INFLUX_TOKEN="$2"
                shift 2
                ;;
            -o|--org)
                INFLUX_ORG="$2"
                shift 2
                ;;
            -d|--database)
                database_or_bucket="$2"
                shift 2
                ;;
            -v|--version)
                INFLUX_VERSION="$2"
                shift 2
                ;;
            -b|--backup-dir)
                BACKUP_DIR="$2"
                shift 2
                ;;
            -r|--retention)
                RETENTION_DAYS="$2"
                shift 2
                ;;
            --compress)
                COMPRESS="true"
                shift
                ;;
            --parallel)
                PARALLEL="true"
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                log ERROR "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    # Validate version
    if [[ ! "$INFLUX_VERSION" =~ ^[12]$ ]]; then
        log ERROR "Invalid InfluxDB version. Must be 1 or 2"
        exit 1
    fi
    
    # Update LOG_FILE path after BACKUP_DIR is potentially changed
    LOG_FILE="$BACKUP_DIR/influxdb_backup.log"
    
    log INFO "Starting InfluxDB v$INFLUX_VERSION backup process"
    log INFO "Host: $INFLUX_HOST:$INFLUX_PORT"
    log INFO "Backup Directory: $BACKUP_DIR"
    log INFO "Retention: $RETENTION_DAYS days"
    log INFO "Compression: $COMPRESS"
    log INFO "Parallel: $PARALLEL"
    
    if [[ -n "$database_or_bucket" ]]; then
        if [[ "$INFLUX_VERSION" == "1" ]]; then
            log INFO "Database: $database_or_bucket"
        else
            log INFO "Bucket: $database_or_bucket"
        fi
    else
        log INFO "Backing up all databases/buckets"
    fi
    
    # Check dependencies
    check_dependencies
    
    # Test connection
    if ! test_connection; then
        generate_backup_report "$database_or_bucket" "Connection Failed" "0B" "0"
        exit 1
    fi
    
    # Get InfluxDB information
    get_influxdb_info
    
    # Create backup directory
    create_backup_dir
    
    # Perform backup based on version
    local start_time=$(date +%s)
    local backup_success=false
    
    if [[ "$INFLUX_VERSION" == "1" ]]; then
        if backup_influxdb_v1 "$database_or_bucket"; then
            backup_success=true
        fi
    else
        if backup_influxdb_v2 "$database_or_bucket"; then
            backup_success=true
        fi
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [[ "$backup_success" == "true" ]]; then
        # Compress backup if enabled
        if ! compress_backup; then
            log WARN "Compression failed, but backup is still valid"
        fi
        
        # Verify backup
        if verify_backup; then
            local backup_size=$(get_backup_size)
            
            log INFO "Backup size: $backup_size"
            log INFO "Backup completed successfully in ${duration} seconds"
            
            # Cleanup old backups
            cleanup_old_backups
            
            # Generate success report
            generate_backup_report "$database_or_bucket" "Success" "$backup_size" "$duration"
            
            exit 0
        else
            log ERROR "Backup verification failed"
            generate_backup_report "$database_or_bucket" "Verification Failed" "0B" "$duration"
            exit 1
        fi
    else
        log ERROR "Backup process failed after ${duration} seconds"
        generate_backup_report "$database_or_bucket" "Failed" "0B" "$duration"
        exit 1
    fi
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi