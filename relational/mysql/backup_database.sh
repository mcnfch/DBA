#!/bin/bash
# MySQL Database Backup Script (Shell)
# Author: DBA Portfolio
# Purpose: Comprehensive MySQL backup with replication and performance optimization

set -euo pipefail

# Default configuration
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/mysql}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
COMPRESS="${COMPRESS:-true}"
PARALLEL_JOBS="${PARALLEL_JOBS:-4}"
SINGLE_TRANSACTION="${SINGLE_TRANSACTION:-true}"
ROUTINES="${ROUTINES:-true}"
TRIGGERS="${TRIGGERS:-true}"
LOG_FILE="${LOG_FILE:-$BACKUP_DIR/mysql_backup.log}"

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
Usage: $0 [OPTIONS]

Options:
    -d, --database      Database name (optional - backup all if not specified)
    -h, --host         MySQL host (default: localhost)
    -P, --port         MySQL port (default: 3306)
    -u, --user         MySQL user (default: root)
    -p, --password     MySQL password (use MYSQL_PWD env var for security)
    -b, --backup-dir   Backup directory (default: /var/backups/mysql)
    -r, --retention    Retention days (default: 7)
    -c, --compress     Compress backups (default: true)
    -j, --jobs         Parallel jobs for mysqldump (default: 4)
    --no-single-transaction  Disable single transaction mode
    --no-routines      Don't backup stored procedures/functions
    --no-triggers      Don't backup triggers
    --help            Show this help

Environment Variables:
    MYSQL_PWD         MySQL password (more secure than command line)
    DB_HOST           Database host
    DB_PORT           Database port
    DB_USER           Database user
    BACKUP_DIR        Backup directory
    RETENTION_DAYS    Backup retention days

Examples:
    $0                              # Backup all databases
    $0 -d myapp                     # Backup specific database
    MYSQL_PWD=secret $0 -d prod     # Using environment variable for password
    $0 -h db.example.com -u admin   # Remote backup
EOF
}

# Check dependencies
check_dependencies() {
    local deps=("mysql" "mysqldump" "mysqladmin")
    
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            log ERROR "Required dependency '$dep' not found"
            exit 1
        fi
    done
    
    if [[ "$COMPRESS" == "true" ]] && ! command -v "gzip" &> /dev/null; then
        log WARN "gzip not found, compression disabled"
        COMPRESS="false"
    fi
    
    if ! command -v "pv" &> /dev/null; then
        log DEBUG "pv (pipe viewer) not found - progress monitoring disabled"
    fi
}

# Test MySQL connection
test_connection() {
    log INFO "Testing MySQL connection to $DB_HOST:$DB_PORT"
    
    local connection_params=(
        "--host=$DB_HOST"
        "--port=$DB_PORT"
        "--user=$DB_USER"
    )
    
    if [[ -n "${MYSQL_PWD:-$DB_PASSWORD}" ]]; then
        export MYSQL_PWD="${MYSQL_PWD:-$DB_PASSWORD}"
    fi
    
    if mysqladmin "${connection_params[@]}" ping --silent 2>/dev/null; then
        log INFO "MySQL connection successful"
        return 0
    else
        log ERROR "MySQL connection failed"
        return 1
    fi
}

# Get MySQL server information
get_server_info() {
    local connection_params=(
        "--host=$DB_HOST"
        "--port=$DB_PORT" 
        "--user=$DB_USER"
        "--skip-column-names"
        "--silent"
    )
    
    log INFO "MySQL Server Information:"
    
    # Version
    local version=$(mysql "${connection_params[@]}" -e "SELECT VERSION();" 2>/dev/null || echo "Unknown")
    log INFO "Version: $version"
    
    # Uptime
    local uptime=$(mysql "${connection_params[@]}" -e "SHOW STATUS LIKE 'Uptime';" 2>/dev/null | cut -f2 || echo "0")
    local uptime_hours=$((uptime / 3600))
    log INFO "Uptime: $uptime_hours hours"
    
    # Check replication status
    local slave_status=$(mysql "${connection_params[@]}" -e "SHOW SLAVE STATUS\G" 2>/dev/null || true)
    if [[ -n "$slave_status" ]]; then
        local io_running=$(echo "$slave_status" | grep "Slave_IO_Running:" | awk '{print $2}')
        local sql_running=$(echo "$slave_status" | grep "Slave_SQL_Running:" | awk '{print $2}')
        log INFO "Replication Status - IO: $io_running, SQL: $sql_running"
        
        if [[ "$io_running" != "Yes" ]] || [[ "$sql_running" != "Yes" ]]; then
            log WARN "Replication may not be running properly"
        fi
    fi
}

# Get database list
get_database_list() {
    local database_name="$1"
    local connection_params=(
        "--host=$DB_HOST"
        "--port=$DB_PORT"
        "--user=$DB_USER"
        "--skip-column-names"
        "--silent"
    )
    
    if [[ -n "$database_name" ]]; then
        echo "$database_name"
    else
        mysql "${connection_params[@]}" -e "SHOW DATABASES;" | grep -v -E '^(information_schema|performance_schema|mysql|sys)$'
    fi
}

# Get database size
get_database_size() {
    local db_name="$1"
    local connection_params=(
        "--host=$DB_HOST"
        "--port=$DB_PORT"
        "--user=$DB_USER"
        "--skip-column-names"
        "--silent"
    )
    
    local size_query="SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'DB Size in MB'
                     FROM information_schema.tables 
                     WHERE table_schema='$db_name';"
    
    mysql "${connection_params[@]}" -e "$size_query" 2>/dev/null | head -1 || echo "0"
}

# Create backup directory
create_backup_dir() {
    if [[ ! -d "$BACKUP_DIR" ]]; then
        log INFO "Creating backup directory: $BACKUP_DIR"
        mkdir -p "$BACKUP_DIR"
    fi
}

# Backup single database
backup_database() {
    local db_name="$1"
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local backup_file="$BACKUP_DIR/${db_name}_backup_${timestamp}.sql"
    local final_file="$backup_file"
    
    log INFO "Starting backup of database: $db_name"
    
    # Get database size
    local db_size=$(get_database_size "$db_name")
    log INFO "Database size: ${db_size} MB"
    
    # Build mysqldump command
    local dump_params=(
        "--host=$DB_HOST"
        "--port=$DB_PORT"
        "--user=$DB_USER"
        "--verbose"
        "--add-drop-database"
        "--create-options"
        "--disable-keys"
        "--extended-insert"
        "--quick"
        "--lock-tables=false"
    )
    
    # Add conditional parameters
    if [[ "$SINGLE_TRANSACTION" == "true" ]]; then
        dump_params+=("--single-transaction")
        log DEBUG "Using single-transaction mode"
    fi
    
    if [[ "$ROUTINES" == "true" ]]; then
        dump_params+=("--routines")
        log DEBUG "Including stored procedures and functions"
    fi
    
    if [[ "$TRIGGERS" == "true" ]]; then
        dump_params+=("--triggers")
        log DEBUG "Including triggers"
    fi
    
    # Add parallel processing if supported
    if mysqldump --help | grep -q "parallel" 2>/dev/null; then
        dump_params+=("--parallel=$PARALLEL_JOBS")
        log DEBUG "Using parallel processing with $PARALLEL_JOBS jobs"
    fi
    
    dump_params+=("--databases" "$db_name")
    
    # Execute backup with progress monitoring if available
    local start_time=$(date +%s)
    
    if command -v pv &> /dev/null && [[ "$db_size" != "0" ]] && [[ $(echo "$db_size > 10" | bc -l 2>/dev/null || echo 0) -eq 1 ]]; then
        # Use pv for progress monitoring on larger databases
        log INFO "Starting backup with progress monitoring..."
        if mysqldump "${dump_params[@]}" 2>>"$LOG_FILE" | pv -s "${db_size}M" > "$backup_file"; then
            local backup_result=0
        else
            local backup_result=1
        fi
    else
        log INFO "Starting backup..."
        if mysqldump "${dump_params[@]}" > "$backup_file" 2>>"$LOG_FILE"; then
            local backup_result=0
        else
            local backup_result=1
        fi
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [[ $backup_result -eq 0 ]]; then
        log INFO "Backup completed in ${duration} seconds"
        
        # Verify backup file
        if verify_backup "$backup_file" "$db_name"; then
            log INFO "Backup verification successful"
            
            # Compress if enabled
            if [[ "$COMPRESS" == "true" ]]; then
                log INFO "Compressing backup..."
                if gzip "$backup_file"; then
                    final_file="${backup_file}.gz"
                    log INFO "Backup compressed: $final_file"
                else
                    log WARN "Compression failed, keeping uncompressed backup"
                fi
            fi
            
            # Log backup stats
            local backup_size=$(get_file_size "$final_file")
            log INFO "Final backup size: $backup_size"
            
            # Log to CSV report
            log_backup_report "$db_name" "Success" "$final_file" "$backup_size" "$duration"
            
            return 0
        else
            log ERROR "Backup verification failed for $db_name"
            return 1
        fi
    else
        log ERROR "Backup failed for database: $db_name"
        return 1
    fi
}

# Verify backup file
verify_backup() {
    local backup_file="$1"
    local db_name="$2"
    
    log DEBUG "Verifying backup file: $backup_file"
    
    # Check if file exists and has content
    if [[ ! -f "$backup_file" ]] || [[ ! -s "$backup_file" ]]; then
        log ERROR "Backup file is empty or doesn't exist"
        return 1
    fi
    
    # Check file content based on compression
    if [[ "$backup_file" == *.gz ]]; then
        # Check compressed file
        if zcat "$backup_file" | head -n 10 | grep -q "CREATE DATABASE.*$db_name\|USE.*$db_name"; then
            return 0
        fi
    else
        # Check uncompressed file
        if head -n 10 "$backup_file" | grep -q "CREATE DATABASE.*$db_name\|USE.*$db_name"; then
            return 0
        fi
    fi
    
    log ERROR "Backup file verification failed - invalid content"
    return 1
}

# Get file size in human readable format
get_file_size() {
    local file="$1"
    
    if command -v numfmt &> /dev/null; then
        numfmt --to=iec-i --suffix=B "$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file" 2>/dev/null || echo 0)"
    else
        ls -lh "$file" 2>/dev/null | awk '{print $5}' || echo "0B"
    fi
}

# Cleanup old backups
cleanup_old_backups() {
    local database_pattern="$1"
    
    log INFO "Cleaning up backups older than $RETENTION_DAYS days"
    
    local pattern="$BACKUP_DIR/${database_pattern}_backup_*.sql*"
    local deleted_count=0
    
    while IFS= read -r -d '' file; do
        rm -f "$file"
        log INFO "Removed old backup: $(basename "$file")"
        ((deleted_count++))
    done < <(find "$BACKUP_DIR" -name "${database_pattern}_backup_*.sql*" -type f -mtime +$RETENTION_DAYS -print0 2>/dev/null)
    
    if [[ $deleted_count -gt 0 ]]; then
        log INFO "Removed $deleted_count old backup(s) for $database_pattern"
    else
        log DEBUG "No old backups to remove for $database_pattern"
    fi
}

# Log backup report to CSV
log_backup_report() {
    local database="$1"
    local status="$2"  
    local backup_file="$3"
    local size="$4"
    local duration="$5"
    
    local report_file="$BACKUP_DIR/mysql_backup_report.csv"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Create header if file doesn't exist
    if [[ ! -f "$report_file" ]]; then
        echo "Timestamp,Database,Status,BackupFile,Size,Duration(s),Host" > "$report_file"
    fi
    
    echo "$timestamp,$database,$status,$backup_file,$size,$duration,$DB_HOST:$DB_PORT" >> "$report_file"
}

# Get backup statistics
show_backup_statistics() {
    log INFO "Backup Statistics Summary:"
    
    local report_file="$BACKUP_DIR/mysql_backup_report.csv"
    if [[ -f "$report_file" ]]; then
        # Show recent backups
        log INFO "Recent backup history:"
        tail -n 10 "$report_file" | column -t -s ','
        
        # Show success rate
        local total_backups=$(tail -n +2 "$report_file" | wc -l)
        local successful_backups=$(tail -n +2 "$report_file" | grep -c "Success" || echo 0)
        
        if [[ $total_backups -gt 0 ]]; then
            local success_rate=$((successful_backups * 100 / total_backups))
            log INFO "Success rate: $success_rate% ($successful_backups/$total_backups)"
        fi
    fi
}

# Signal handlers
cleanup_on_exit() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        log ERROR "Script exited with error code: $exit_code"
    fi
}

trap cleanup_on_exit EXIT

# Main function
main() {
    local database=""
    local backup_all=true
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--database)
                database="$2"
                backup_all=false
                shift 2
                ;;
            -h|--host)
                DB_HOST="$2"
                shift 2
                ;;
            -P|--port)
                DB_PORT="$2"
                shift 2
                ;;
            -u|--user)
                DB_USER="$2"
                shift 2
                ;;
            -p|--password)
                DB_PASSWORD="$2"
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
            -c|--compress)
                COMPRESS="$2"
                shift 2
                ;;
            -j|--jobs)
                PARALLEL_JOBS="$2"
                shift 2
                ;;
            --no-single-transaction)
                SINGLE_TRANSACTION="false"
                shift
                ;;
            --no-routines)
                ROUTINES="false"
                shift
                ;;
            --no-triggers)
                TRIGGERS="false"
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
    
    # Set password from environment if not provided
    if [[ -z "${MYSQL_PWD:-$DB_PASSWORD}" ]]; then
        log ERROR "MySQL password must be provided via -p option or MYSQL_PWD environment variable"
        exit 1
    fi
    
    export MYSQL_PWD="${MYSQL_PWD:-$DB_PASSWORD}"
    
    # Update LOG_FILE path after BACKUP_DIR is potentially changed
    LOG_FILE="$BACKUP_DIR/mysql_backup.log"
    
    log INFO "Starting MySQL backup process"
    log INFO "Host: $DB_HOST:$DB_PORT"
    log INFO "User: $DB_USER"
    log INFO "Backup Directory: $BACKUP_DIR"
    log INFO "Retention: $RETENTION_DAYS days"
    log INFO "Compression: $COMPRESS"
    
    # Check dependencies
    check_dependencies
    
    # Create backup directory
    create_backup_dir
    
    # Test connection
    if ! test_connection; then
        exit 1
    fi
    
    # Get server information
    get_server_info
    
    # Get list of databases to backup
    local databases
    if [[ "$backup_all" == "true" ]]; then
        log INFO "Backing up all user databases"
        databases=$(get_database_list "")
    else
        log INFO "Backing up specific database: $database"
        databases="$database"
    fi
    
    if [[ -z "$databases" ]]; then
        log ERROR "No databases found to backup"
        exit 1
    fi
    
    # Backup each database
    local total_databases=0
    local successful_backups=0
    local failed_backups=0
    
    for db in $databases; do
        ((total_databases++))
        log INFO "Processing database $total_databases: $db"
        
        if backup_database "$db"; then
            ((successful_backups++))
            
            # Cleanup old backups for this database
            cleanup_old_backups "$db"
        else
            ((failed_backups++))
            log_backup_report "$db" "Failed" "" "0B" "0"
        fi
    done
    
    # Final summary
    log INFO "Backup process completed"
    log INFO "Total databases: $total_databases"
    log INFO "Successful: $successful_backups"
    log INFO "Failed: $failed_backups"
    
    # Show statistics
    show_backup_statistics
    
    if [[ $failed_backups -eq 0 ]]; then
        log INFO "All backups completed successfully"
        exit 0
    else
        log ERROR "$failed_backups backup(s) failed"
        exit 1
    fi
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi