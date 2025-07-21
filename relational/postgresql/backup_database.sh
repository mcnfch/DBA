#!/bin/bash
# PostgreSQL Database Backup Script (Shell)
# Author: DBA Portfolio  
# Purpose: Automated PostgreSQL backup with logging and monitoring

set -euo pipefail

# Default configuration
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/postgresql}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
COMPRESS="${COMPRESS:-true}"
LOG_FILE="${LOG_FILE:-$BACKUP_DIR/backup.log}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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
        *)       echo "[$timestamp] $message" | tee -a "$LOG_FILE" ;;
    esac
}

# Usage function
usage() {
    cat << EOF
Usage: $0 -d DATABASE_NAME [OPTIONS]

Options:
    -d, --database      Database name (required)
    -h, --host         Database host (default: localhost)
    -p, --port         Database port (default: 5432)
    -u, --user         Database user (default: postgres)
    -b, --backup-dir   Backup directory (default: /var/backups/postgresql)
    -r, --retention    Retention days (default: 7)
    -c, --compress     Compress backup (default: true)
    --help            Show this help message

Environment Variables:
    PGPASSWORD        Database password
    DB_HOST           Database host
    DB_PORT           Database port
    DB_USER           Database user
    BACKUP_DIR        Backup directory
    RETENTION_DAYS    Backup retention days
    COMPRESS          Compress backups (true/false)

Examples:
    $0 -d mydb
    $0 -d mydb -h db.example.com -u admin
    PGPASSWORD=secret $0 -d production_db -r 30
EOF
}

# Check dependencies
check_dependencies() {
    local deps=("pg_dump" "psql")
    
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
}

# Test database connection
test_connection() {
    local database=$1
    
    log INFO "Testing connection to database: $database"
    
    if PGPASSWORD="$PGPASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$database" -c "SELECT 1;" &>/dev/null; then
        log INFO "Connection successful"
        return 0
    else
        log ERROR "Connection failed"
        return 1
    fi
}

# Get database size
get_database_size() {
    local database=$1
    
    PGPASSWORD="$PGPASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$database" -t -c "SELECT pg_size_pretty(pg_database_size('$database'));" 2>/dev/null | xargs || echo "Unknown"
}

# Create backup directory
create_backup_dir() {
    if [[ ! -d "$BACKUP_DIR" ]]; then
        log INFO "Creating backup directory: $BACKUP_DIR"
        mkdir -p "$BACKUP_DIR"
    fi
}

# Perform backup
perform_backup() {
    local database=$1
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local backup_file="$BACKUP_DIR/${database}_backup_${timestamp}.sql"
    local final_file="$backup_file"
    
    log INFO "Starting backup of database: $database"
    log INFO "Database size: $(get_database_size "$database")"
    
    # Create backup
    if PGPASSWORD="$PGPASSWORD" pg_dump \
        --host="$DB_HOST" \
        --port="$DB_PORT" \
        --username="$DB_USER" \
        --dbname="$database" \
        --verbose \
        --clean \
        --create \
        --if-exists \
        --file="$backup_file" 2>> "$LOG_FILE"; then
        
        log INFO "Backup completed: $backup_file"
        
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
        
        # Verify backup
        if verify_backup "$final_file"; then
            log INFO "Backup verification successful"
            
            # Log backup stats
            local backup_size=$(get_file_size "$final_file")
            log INFO "Backup size: $backup_size"
            
            return 0
        else
            log ERROR "Backup verification failed"
            return 1
        fi
    else
        log ERROR "pg_dump failed"
        return 1
    fi
}

# Verify backup file
verify_backup() {
    local backup_file=$1
    
    if [[ "$backup_file" == *.gz ]]; then
        # Check compressed file
        if zcat "$backup_file" | head -n 5 | grep -q "PostgreSQL database dump\|CREATE DATABASE"; then
            return 0
        fi
    else
        # Check uncompressed file
        if head -n 5 "$backup_file" | grep -q "PostgreSQL database dump\|CREATE DATABASE"; then
            return 0
        fi
    fi
    
    return 1
}

# Get file size in human readable format
get_file_size() {
    local file=$1
    
    if command -v numfmt &> /dev/null; then
        numfmt --to=iec-i --suffix=B $(stat -c%s "$file" 2>/dev/null || stat -f%z "$file" 2>/dev/null || echo 0)
    else
        ls -lh "$file" | awk '{print $5}'
    fi
}

# Cleanup old backups
cleanup_old_backups() {
    local database=$1
    
    log INFO "Cleaning up backups older than $RETENTION_DAYS days"
    
    # Find and remove old backups
    local deleted_count=0
    while IFS= read -r -d '' file; do
        rm -f "$file"
        log INFO "Removed old backup: $(basename "$file")"
        ((deleted_count++))
    done < <(find "$BACKUP_DIR" -name "${database}_backup_*.sql*" -type f -mtime +$RETENTION_DAYS -print0 2>/dev/null)
    
    if [[ $deleted_count -gt 0 ]]; then
        log INFO "Removed $deleted_count old backup(s)"
    else
        log INFO "No old backups to remove"
    fi
}

# Generate backup report
generate_report() {
    local database=$1
    local status=$2
    local backup_file=$3
    
    local report_file="$BACKUP_DIR/backup_report.csv"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Create header if file doesn't exist
    if [[ ! -f "$report_file" ]]; then
        echo "Timestamp,Database,Status,BackupFile,Size,Duration" > "$report_file"
    fi
    
    local size="0"
    if [[ -n "$backup_file" && -f "$backup_file" ]]; then
        size=$(get_file_size "$backup_file")
    fi
    
    echo "$timestamp,$database,$status,$backup_file,$size,N/A" >> "$report_file"
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
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--database)
                database="$2"
                shift 2
                ;;
            -h|--host)
                DB_HOST="$2"
                shift 2
                ;;
            -p|--port)
                DB_PORT="$2"
                shift 2
                ;;
            -u|--user)
                DB_USER="$2"
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
    
    # Validate required parameters
    if [[ -z "$database" ]]; then
        log ERROR "Database name is required"
        usage
        exit 1
    fi
    
    if [[ -z "${PGPASSWORD:-}" ]]; then
        log ERROR "PGPASSWORD environment variable must be set"
        exit 1
    fi
    
    # Update LOG_FILE path after BACKUP_DIR is set
    LOG_FILE="$BACKUP_DIR/backup.log"
    
    log INFO "Starting PostgreSQL backup process"
    log INFO "Database: $database"
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
    if ! test_connection "$database"; then
        generate_report "$database" "Connection Failed" ""
        exit 1
    fi
    
    # Perform backup
    local backup_file=""
    if perform_backup "$database"; then
        # Get the most recent backup file
        backup_file=$(ls -t "$BACKUP_DIR"/${database}_backup_*.sql* 2>/dev/null | head -n 1 || echo "")
        
        # Cleanup old backups
        cleanup_old_backups "$database"
        
        generate_report "$database" "Success" "$backup_file"
        log INFO "Backup process completed successfully"
    else
        generate_report "$database" "Failed" ""
        log ERROR "Backup process failed"
        exit 1
    fi
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi