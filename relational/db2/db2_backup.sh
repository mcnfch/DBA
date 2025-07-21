#!/bin/bash
# IBM DB2 Backup Script (Shell)
# Author: DBA Portfolio
# Purpose: Comprehensive IBM DB2 backup automation with online/offline support

set -euo pipefail

# Default configuration
DB2_INSTANCE="${DB2_INSTANCE:-db2inst1}"
DB2_HOME="${DB2_HOME:-/opt/ibm/db2/V11.5}"
DATABASE_NAME="${DATABASE_NAME:-}"
BACKUP_TYPE="${BACKUP_TYPE:-ONLINE}"  # ONLINE or OFFLINE
BACKUP_DIR="${BACKUP_DIR:-/db2backup}"
COMPRESS="${COMPRESS:-true}"
INCLUDE_LOGS="${INCLUDE_LOGS:-true}"
PARALLELISM="${PARALLELISM:-1}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
LOG_FILE="${LOG_FILE:-$BACKUP_DIR/db2_backup.log}"

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
IBM DB2 Backup Script

Usage: $0 [OPTIONS]

Options:
    -d, --database     Database name (required)
    -i, --instance     DB2 instance name (default: db2inst1)
    -h, --db2-home     DB2 home directory (default: /opt/ibm/db2/V11.5)
    -t, --type         Backup type: ONLINE or OFFLINE (default: ONLINE)
    -b, --backup-dir   Backup directory (default: /db2backup)
    -p, --parallelism  Number of parallel processes (default: 1)
    -r, --retention    Retention days (default: 7)
    --compress         Enable compression (default: true)
    --include-logs     Include transaction logs (default: true)
    --help            Show this help

Environment Variables:
    DB2_INSTANCE      DB2 instance name
    DB2_HOME          DB2 installation directory
    BACKUP_DIR        Backup destination directory
    RETENTION_DAYS    Backup retention in days

Examples:
    $0 -d MYDB                          # Online backup with defaults
    $0 -d MYDB -t OFFLINE               # Offline backup
    $0 -d MYDB -p 4 --compress          # Parallel compressed backup
    $0 -d MYDB -b /backup/db2           # Custom backup directory
EOF
}

# Check DB2 environment
check_db2_environment() {
    log INFO "Checking DB2 environment..."
    
    # Check if DB2 home exists
    if [[ ! -d "$DB2_HOME" ]]; then
        log ERROR "DB2 home directory not found: $DB2_HOME"
        exit 1
    fi
    
    # Set DB2 environment
    export DB2_HOME="$DB2_HOME"
    export PATH="$DB2_HOME/bin:$PATH"
    export LD_LIBRARY_PATH="$DB2_HOME/lib64:$DB2_HOME/lib32:${LD_LIBRARY_PATH:-}"
    
    # Check if DB2 commands are available
    local commands=("db2" "db2level" "db2pd")
    for cmd in "${commands[@]}"; do
        if ! command -v "$cmd" &> /dev/null; then
            log ERROR "DB2 command not found: $cmd"
            exit 1
        fi
    done
    
    log INFO "DB2 environment configured successfully"
    log INFO "DB2 Instance: $DB2_INSTANCE"
    log INFO "DB2 Home: $DB2_HOME"
}

# Test database connection
test_database_connection() {
    log INFO "Testing database connection..."
    
    # Set instance environment
    export DB2INSTANCE="$DB2_INSTANCE"
    
    # Test connection
    if db2 "connect to $DATABASE_NAME" >/dev/null 2>&1; then
        log INFO "Database connection successful"
        db2 "connect reset" >/dev/null 2>&1
        return 0
    else
        log ERROR "Database connection failed"
        return 1
    fi
}

# Get database information
get_database_info() {
    log INFO "Gathering database information..."
    
    export DB2INSTANCE="$DB2_INSTANCE"
    
    # Connect to database
    db2 "connect to $DATABASE_NAME" >/dev/null 2>&1
    
    # Get database configuration
    log INFO "Database Configuration:"
    db2 "get db cfg for $DATABASE_NAME" | grep -E "(Database name|Database path|Log retain for recovery|User exit for logging|Automatic maintenance)"
    
    # Get tablespace information
    log INFO "Tablespace Information:"
    db2 "SELECT TBSP_NAME, TBSP_TYPE, TBSP_STATE, TOTAL_PAGES, USABLE_PAGES, USED_PAGES FROM SYSIBMADM.TBSP_UTILIZATION" 2>/dev/null || true
    
    # Get database size
    log INFO "Database Size Information:"
    db2 "CALL SYSPROC.DB_SIZE(?, ?, ?, 0)" 2>/dev/null || {
        log INFO "Database size procedure not available, using alternative method"
        db2 "SELECT SUM(POOL_DATA_L_READS + POOL_INDEX_L_READS) as LOGICAL_READS FROM SYSIBMADM.SNAPDB" 2>/dev/null || true
    }
    
    # Get active log information
    log INFO "Active Log Information:"
    db2 "SELECT LOG_NAME, LOG_PATH FROM SYSIBMADM.LOG_UTILIZATION" 2>/dev/null || true
    
    # Disconnect
    db2 "connect reset" >/dev/null 2>&1
}

# Create backup directory
create_backup_directory() {
    if [[ ! -d "$BACKUP_DIR" ]]; then
        log INFO "Creating backup directory: $BACKUP_DIR"
        mkdir -p "$BACKUP_DIR"
        
        # Set appropriate permissions for DB2 instance user
        chmod 755 "$BACKUP_DIR"
        
        # Change ownership if running as root
        if [[ $(id -u) -eq 0 ]] && id "$DB2_INSTANCE" &>/dev/null; then
            chown "$DB2_INSTANCE:$DB2_INSTANCE" "$BACKUP_DIR"
        fi
    fi
}

# Perform database backup
perform_backup() {
    local database="$1"
    local backup_type="$2"
    
    log INFO "Starting $backup_type backup of database: $database"
    
    export DB2INSTANCE="$DB2_INSTANCE"
    
    # Generate timestamp for backup
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local backup_path="$BACKUP_DIR/${database}_${backup_type}_${timestamp}"
    
    # Build backup command
    local backup_cmd="db2 \"BACKUP DATABASE $database"
    
    if [[ "$backup_type" == "ONLINE" ]]; then
        backup_cmd+=" ONLINE"
    fi
    
    backup_cmd+=" TO $backup_path"
    
    # Add compression if enabled
    if [[ "$COMPRESS" == "true" ]]; then
        backup_cmd+=" COMPRESS"
        log DEBUG "Compression enabled"
    fi
    
    # Add parallelism if specified
    if [[ "$PARALLELISM" -gt 1 ]]; then
        backup_cmd+=" PARALLELISM $PARALLELISM"
        log DEBUG "Parallelism set to: $PARALLELISM"
    fi
    
    # Add log inclusion for online backups
    if [[ "$INCLUDE_LOGS" == "true" && "$backup_type" == "ONLINE" ]]; then
        backup_cmd+=" INCLUDE LOGS"
        log DEBUG "Including transaction logs"
    fi
    
    backup_cmd+="\""
    
    log INFO "Executing backup command..."
    log DEBUG "Command: $backup_cmd"
    
    # Execute backup
    local start_time=$(date +%s)
    
    if [[ "$backup_type" == "OFFLINE" ]]; then
        # Force applications off for offline backup
        log INFO "Forcing applications off for offline backup..."
        db2 "force applications all" || log WARN "Could not force applications off"
        sleep 2
    fi
    
    # Perform the backup
    if eval "$backup_cmd"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log INFO "Backup completed successfully in ${duration} seconds"
        
        # Get backup information
        get_backup_info "$backup_path"
        
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log ERROR "Backup failed after ${duration} seconds"
        return 1
    fi
}

# Get backup information
get_backup_info() {
    local backup_path="$1"
    
    log INFO "Backup Information:"
    
    # List backup files
    if [[ -d "$backup_path" ]]; then
        local file_count=$(find "$backup_path" -type f | wc -l)
        local total_size=$(du -sh "$backup_path" | cut -f1)
        
        log INFO "Backup location: $backup_path"
        log INFO "Number of files: $file_count"
        log INFO "Total size: $total_size"
        
        # List backup files
        log INFO "Backup files:"
        find "$backup_path" -type f -exec ls -lh {} \; | while IFS= read -r line; do
            log INFO "  $line"
        done
    else
        log WARN "Backup directory not found: $backup_path"
    fi
}

# Restore backup (verification)
verify_backup() {
    local backup_path="$1"
    
    log INFO "Verifying backup integrity..."
    
    export DB2INSTANCE="$DB2_INSTANCE"
    
    # Create a temporary restore database name for verification
    local verify_db="VERIFY_$(date +%s)"
    local verify_path="/tmp/db2_verify_$$"
    
    # Attempt to restore to verify integrity
    if db2 "RESTORE DATABASE $DATABASE_NAME FROM $backup_path INTO $verify_db WITHOUT PROMPTING" 2>/dev/null; then
        log INFO "Backup verification successful"
        
        # Cleanup verification database
        db2 "DROP DATABASE $verify_db" 2>/dev/null || true
        rm -rf "$verify_path" 2>/dev/null || true
        
        return 0
    else
        log WARN "Backup verification failed or not possible"
        
        # Cleanup
        db2 "DROP DATABASE $verify_db" 2>/dev/null || true
        rm -rf "$verify_path" 2>/dev/null || true
        
        return 1
    fi
}

# List available backups
list_backups() {
    log INFO "Available backups in $BACKUP_DIR:"
    
    if [[ -d "$BACKUP_DIR" ]]; then
        find "$BACKUP_DIR" -maxdepth 1 -type d -name "*_ONLINE_*" -o -name "*_OFFLINE_*" | sort | while IFS= read -r backup; do
            local backup_name=$(basename "$backup")
            local backup_size=$(du -sh "$backup" 2>/dev/null | cut -f1 || echo "Unknown")
            local backup_date=$(echo "$backup_name" | grep -o '[0-9]\{8\}_[0-9]\{6\}' || echo "Unknown")
            
            log INFO "  $backup_name (Size: $backup_size, Date: $backup_date)"
        done
    else
        log INFO "  No backup directory found"
    fi
}

# Cleanup old backups
cleanup_old_backups() {
    log INFO "Cleaning up backups older than $RETENTION_DAYS days"
    
    local deleted_count=0
    
    # Find and remove old backup directories
    while IFS= read -r -d '' backup_dir; do
        rm -rf "$backup_dir"
        log INFO "Removed old backup: $(basename "$backup_dir")"
        ((deleted_count++))
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type d \( -name "*_ONLINE_*" -o -name "*_OFFLINE_*" \) -mtime +$RETENTION_DAYS -print0 2>/dev/null)
    
    if [[ $deleted_count -gt 0 ]]; then
        log INFO "Removed $deleted_count old backup(s)"
    else
        log INFO "No old backups to remove"
    fi
}

# Generate backup report
generate_backup_report() {
    local database="$1"
    local status="$2"
    local backup_type="$3"
    local backup_path="$4"
    local duration="$5"
    
    local report_file="$BACKUP_DIR/db2_backup_report.csv"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Create header if file doesn't exist
    if [[ ! -f "$report_file" ]]; then
        echo "Timestamp,Instance,Database,BackupType,Status,BackupPath,Duration_sec,Size" > "$report_file"
    fi
    
    local backup_size="Unknown"
    if [[ -n "$backup_path" && -d "$backup_path" ]]; then
        backup_size=$(du -sh "$backup_path" 2>/dev/null | cut -f1 || echo "Unknown")
    fi
    
    echo "$timestamp,$DB2_INSTANCE,$database,$backup_type,$status,$backup_path,$duration,$backup_size" >> "$report_file"
    
    log INFO "Backup report updated: $report_file"
}

# Get DB2 health information
get_db2_health() {
    log INFO "DB2 Health Information:"
    
    export DB2INSTANCE="$DB2_INSTANCE"
    
    # DB2 level
    log INFO "DB2 Version:"
    db2level | head -5
    
    # Instance status
    log INFO "Instance Status:"
    db2pd -dbptnmem 2>/dev/null | head -10 || log WARN "Could not get instance memory info"
    
    # Database status
    if [[ -n "$DATABASE_NAME" ]]; then
        log INFO "Database Status:"
        db2 "connect to $DATABASE_NAME" >/dev/null 2>&1
        db2 "SELECT SNAPSHOT_TIMESTAMP, DB_NAME, DB_STATUS, TOTAL_LOG_USED, TOTAL_LOG_AVAILABLE FROM SYSIBMADM.SNAPDB" 2>/dev/null || true
        db2 "connect reset" >/dev/null 2>&1
    fi
}

# Main function
main() {
    local backup_type="$BACKUP_TYPE"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--database)
                DATABASE_NAME="$2"
                shift 2
                ;;
            -i|--instance)
                DB2_INSTANCE="$2"
                shift 2
                ;;
            -h|--db2-home)
                DB2_HOME="$2"
                shift 2
                ;;
            -t|--type)
                backup_type="$2"
                shift 2
                ;;
            -b|--backup-dir)
                BACKUP_DIR="$2"
                shift 2
                ;;
            -p|--parallelism)
                PARALLELISM="$2"
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
            --include-logs)
                INCLUDE_LOGS="true"
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
    
    # Validate required parameters
    if [[ -z "$DATABASE_NAME" ]]; then
        log ERROR "Database name is required"
        usage
        exit 1
    fi
    
    # Validate backup type
    if [[ ! "$backup_type" =~ ^(ONLINE|OFFLINE)$ ]]; then
        log ERROR "Invalid backup type: $backup_type. Must be ONLINE or OFFLINE"
        exit 1
    fi
    
    # Update LOG_FILE path after BACKUP_DIR is potentially changed
    LOG_FILE="$BACKUP_DIR/db2_backup.log"
    
    log INFO "Starting IBM DB2 backup process"
    log INFO "Instance: $DB2_INSTANCE"
    log INFO "Database: $DATABASE_NAME"
    log INFO "Backup Type: $backup_type"
    log INFO "Backup Directory: $BACKUP_DIR"
    log INFO "Parallelism: $PARALLELISM"
    log INFO "Compression: $COMPRESS"
    log INFO "Include Logs: $INCLUDE_LOGS"
    log INFO "Retention: $RETENTION_DAYS days"
    
    # Check DB2 environment
    check_db2_environment
    
    # Create backup directory
    create_backup_directory
    
    # Test database connection
    if ! test_database_connection; then
        generate_backup_report "$DATABASE_NAME" "Connection Failed" "$backup_type" "" "0"
        exit 1
    fi
    
    # Get database information
    get_database_info
    
    # Get DB2 health information
    get_db2_health
    
    # Perform backup
    local start_time=$(date +%s)
    local backup_path=""
    
    if perform_backup "$DATABASE_NAME" "$backup_type"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        # Get the backup path (last created directory)
        backup_path=$(find "$BACKUP_DIR" -maxdepth 1 -type d -name "*${DATABASE_NAME}*" -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2- || echo "")
        
        log INFO "Backup process completed successfully"
        
        # List all available backups
        list_backups
        
        # Cleanup old backups
        cleanup_old_backups
        
        # Generate success report
        generate_backup_report "$DATABASE_NAME" "Success" "$backup_type" "$backup_path" "$duration"
        
        exit 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log ERROR "Backup process failed"
        
        # Generate failure report
        generate_backup_report "$DATABASE_NAME" "Failed" "$backup_type" "" "$duration"
        
        exit 1
    fi
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi