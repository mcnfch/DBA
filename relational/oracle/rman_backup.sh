#!/bin/bash
# Oracle RMAN Backup Script (Shell)
# Author: DBA Portfolio
# Purpose: Comprehensive Oracle RMAN backup automation with validation

set -euo pipefail

# Default configuration
ORACLE_SID="${ORACLE_SID:-ORCL}"
ORACLE_HOME="${ORACLE_HOME:-/u01/app/oracle/product/19.0.0/dbhome_1}"
BACKUP_TYPE="${BACKUP_TYPE:-FULL}"  # FULL, INCREMENTAL, ARCHIVELOG
BACKUP_DIR="${BACKUP_DIR:-/u01/backup}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
COMPRESS="${COMPRESS:-true}"
VALIDATE_BACKUP="${VALIDATE_BACKUP:-true}"
CROSSCHECK="${CROSSCHECK:-true}"
DELETE_OBSOLETE="${DELETE_OBSOLETE:-true}"
PARALLEL_DEGREE="${PARALLEL_DEGREE:-4}"
LOG_FILE="${LOG_FILE:-$BACKUP_DIR/rman_backup.log}"

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
Oracle RMAN Backup Script

Usage: $0 [OPTIONS]

Options:
    -s, --sid          Oracle SID (default: ORCL)
    -h, --oracle-home  Oracle Home directory
    -t, --type         Backup type: FULL, INCREMENTAL, ARCHIVELOG (default: FULL)
    -b, --backup-dir   Backup directory (default: /u01/backup)
    -r, --retention    Retention days (default: 7)
    -p, --parallel     Parallel degree (default: 4)
    --compress         Enable backup compression (default: true)
    --no-validate      Skip backup validation
    --no-crosscheck    Skip crosscheck of existing backups
    --no-delete        Skip deletion of obsolete backups
    --help            Show this help

Environment Variables:
    ORACLE_SID        Oracle System Identifier
    ORACLE_HOME       Oracle Home directory
    BACKUP_DIR        Backup destination directory
    RETENTION_DAYS    Backup retention in days

Examples:
    $0                                    # Full backup with defaults
    $0 -t INCREMENTAL                     # Incremental backup
    $0 -t ARCHIVELOG -p 2                # Archive log backup with 2 parallel processes
    $0 -s PROD -h /u01/app/oracle/19c     # Backup specific instance
EOF
}

# Check Oracle environment
check_oracle_environment() {
    log INFO "Checking Oracle environment..."
    
    # Check if Oracle Home exists
    if [[ ! -d "$ORACLE_HOME" ]]; then
        log ERROR "Oracle Home not found: $ORACLE_HOME"
        exit 1
    fi
    
    # Check if Oracle binaries exist
    if [[ ! -x "$ORACLE_HOME/bin/sqlplus" ]]; then
        log ERROR "Oracle sqlplus not found in $ORACLE_HOME/bin"
        exit 1
    fi
    
    if [[ ! -x "$ORACLE_HOME/bin/rman" ]]; then
        log ERROR "Oracle RMAN not found in $ORACLE_HOME/bin"
        exit 1
    fi
    
    # Set PATH
    export PATH="$ORACLE_HOME/bin:$PATH"
    export LD_LIBRARY_PATH="$ORACLE_HOME/lib:${LD_LIBRARY_PATH:-}"
    
    log INFO "Oracle environment configured successfully"
    log INFO "Oracle SID: $ORACLE_SID"
    log INFO "Oracle Home: $ORACLE_HOME"
}

# Test database connectivity
test_database_connection() {
    log INFO "Testing database connection..."
    
    local connection_test
    connection_test=$(sqlplus -s / as sysdba <<EOF
set pagesize 0 feedback off verify off heading off echo off
select 'CONNECTION_SUCCESS' from dual;
exit;
EOF
    )
    
    if echo "$connection_test" | grep -q "CONNECTION_SUCCESS"; then
        log INFO "Database connection successful"
        return 0
    else
        log ERROR "Database connection failed"
        return 1
    fi
}

# Get database information
get_database_info() {
    log INFO "Gathering database information..."
    
    sqlplus -s / as sysdba <<EOF
set pagesize 1000 linesize 200
col name format a20
col value format a50

prompt ===============================================
prompt DATABASE INFORMATION
prompt ===============================================

select name, open_mode, database_role, log_mode from v\$database;

prompt
prompt DATABASE SIZE:
select round(sum(bytes)/1024/1024/1024, 2) as "Size (GB)" from dba_data_files;

prompt
prompt ARCHIVELOG MODE:
archive log list;

prompt
prompt TABLESPACES:
col tablespace_name format a20
col status format a10
select tablespace_name, status, round(sum(bytes)/1024/1024, 2) as "Size (MB)"
from dba_data_files 
group by tablespace_name, status
order by tablespace_name;

prompt
prompt RECOVERY AREA USAGE:
col name format a20
select name, round(space_limit/1024/1024/1024, 2) as "Limit (GB)",
       round(space_used/1024/1024/1024, 2) as "Used (GB)",
       round((space_used/space_limit)*100, 2) as "Pct Used"
from v\$recovery_area_usage
where name = 'BACKUP';

exit;
EOF
}

# Create backup directory
create_backup_directory() {
    if [[ ! -d "$BACKUP_DIR" ]]; then
        log INFO "Creating backup directory: $BACKUP_DIR"
        mkdir -p "$BACKUP_DIR"
        
        # Set Oracle permissions
        chmod 755 "$BACKUP_DIR"
        
        # If running as oracle user, ensure ownership
        if [[ $(id -un) == "oracle" ]]; then
            chown oracle:oinstall "$BACKUP_DIR" 2>/dev/null || true
        fi
    fi
}

# Generate RMAN backup script
generate_rman_script() {
    local backup_type="$1"
    local script_file="$BACKUP_DIR/rman_backup_$(date +%Y%m%d_%H%M%S).rman"
    
    log INFO "Generating RMAN script: $script_file"
    
    cat > "$script_file" <<EOF
# RMAN Backup Script - Generated on $(date)
# Backup Type: $backup_type

CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF $RETENTION_DAYS DAYS;
CONFIGURE BACKUP OPTIMIZATION ON;
CONFIGURE DEFAULT DEVICE TYPE TO DISK;
CONFIGURE DEVICE TYPE DISK PARALLELISM $PARALLEL_DEGREE BACKUP TYPE TO BACKUPSET;
CONFIGURE CHANNEL DEVICE TYPE DISK FORMAT '$BACKUP_DIR/%d_%T_%s_%p.bkp';

EOF

    # Add compression if enabled
    if [[ "$COMPRESS" == "true" ]]; then
        cat >> "$script_file" <<EOF
CONFIGURE COMPRESSION ALGORITHM 'MEDIUM';

EOF
    fi

    # Add crosscheck commands
    if [[ "$CROSSCHECK" == "true" ]]; then
        cat >> "$script_file" <<EOF
# Crosscheck existing backups
CROSSCHECK BACKUP;
CROSSCHECK ARCHIVELOG ALL;

EOF
    fi

    # Add backup commands based on type
    case "$backup_type" in
        "FULL")
            cat >> "$script_file" <<EOF
# Full database backup
BACKUP AS COMPRESSED BACKUPSET DATABASE 
    PLUS ARCHIVELOG
    TAG 'FULL_DB_$(date +%Y%m%d_%H%M%S)'
    DELETE INPUT;

# Backup control file and spfile
BACKUP CURRENT CONTROLFILE TAG 'CF_$(date +%Y%m%d_%H%M%S)';
BACKUP SPFILE TAG 'SPFILE_$(date +%Y%m%d_%H%M%S)';

EOF
            ;;
        "INCREMENTAL")
            cat >> "$script_file" <<EOF
# Incremental level 1 backup
BACKUP AS COMPRESSED BACKUPSET 
    INCREMENTAL LEVEL 1 DATABASE
    TAG 'INCR_L1_$(date +%Y%m%d_%H%M%S)';

# Backup archive logs
BACKUP AS COMPRESSED BACKUPSET 
    ARCHIVELOG ALL
    DELETE INPUT
    TAG 'ARCH_$(date +%Y%m%d_%H%M%S)';

# Backup control file
BACKUP CURRENT CONTROLFILE TAG 'CF_$(date +%Y%m%d_%H%M%S)';

EOF
            ;;
        "ARCHIVELOG")
            cat >> "$script_file" <<EOF
# Archive log backup only
BACKUP AS COMPRESSED BACKUPSET 
    ARCHIVELOG ALL
    DELETE INPUT
    TAG 'ARCH_ONLY_$(date +%Y%m%d_%H%M%S)';

EOF
            ;;
        *)
            log ERROR "Unknown backup type: $backup_type"
            return 1
            ;;
    esac

    # Add validation if enabled
    if [[ "$VALIDATE_BACKUP" == "true" ]]; then
        cat >> "$script_file" <<EOF
# Validate backup
VALIDATE BACKUP;

EOF
    fi

    # Add cleanup if enabled
    if [[ "$DELETE_OBSOLETE" == "true" ]]; then
        cat >> "$script_file" <<EOF
# Delete obsolete backups
DELETE NOPROMPT OBSOLETE;

# Delete expired backups
DELETE NOPROMPT EXPIRED BACKUP;
DELETE NOPROMPT EXPIRED ARCHIVELOG ALL;

EOF
    fi

    # Add final report
    cat >> "$script_file" <<EOF
# Generate backup report
LIST BACKUP SUMMARY;
LIST ARCHIVELOG ALL;

REPORT OBSOLETE;
REPORT NEED BACKUP;

EXIT;
EOF

    echo "$script_file"
}

# Execute RMAN backup
execute_rman_backup() {
    local script_file="$1"
    local backup_type="$2"
    
    log INFO "Starting RMAN backup - Type: $backup_type"
    log INFO "Using script: $script_file"
    
    local start_time=$(date +%s)
    local rman_log="$BACKUP_DIR/rman_$(date +%Y%m%d_%H%M%S).log"
    
    # Execute RMAN
    if rman target / cmdfile="$script_file" log="$rman_log"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log INFO "RMAN backup completed successfully in ${duration} seconds"
        log INFO "RMAN log file: $rman_log"
        
        # Show backup summary
        show_backup_summary "$rman_log"
        
        return 0
    else
        local end_time=$(date +%s)  
        local duration=$((end_time - start_time))
        
        log ERROR "RMAN backup failed after ${duration} seconds"
        log ERROR "Check RMAN log file: $rman_log"
        
        # Show error details
        show_rman_errors "$rman_log"
        
        return 1
    fi
}

# Show backup summary
show_backup_summary() {
    local rman_log="$1"
    
    log INFO "Backup Summary:"
    
    # Extract key information from RMAN log
    if [[ -f "$rman_log" ]]; then
        # Show successful backups
        local backup_pieces=$(grep -c "piece handle=" "$rman_log" 2>/dev/null || echo "0")
        log INFO "Backup pieces created: $backup_pieces"
        
        # Show backup size if available
        local backup_size=$(grep "input datafile size=" "$rman_log" | tail -1 | sed 's/.*input datafile size=\([0-9.]*[A-Z]\).*/\1/' 2>/dev/null || echo "Unknown")
        if [[ "$backup_size" != "Unknown" ]]; then
            log INFO "Input datafile size: $backup_size"
        fi
        
        # Show compression ratio if available
        local output_size=$(grep "output size=" "$rman_log" | tail -1 | sed 's/.*output size=\([0-9.]*[A-Z]\).*/\1/' 2>/dev/null || echo "Unknown")
        if [[ "$output_size" != "Unknown" ]]; then
            log INFO "Output backup size: $output_size"
        fi
    fi
    
    # Show disk usage of backup directory
    local backup_dir_size=$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1 || echo "Unknown")
    log INFO "Total backup directory size: $backup_dir_size"
}

# Show RMAN errors
show_rman_errors() {
    local rman_log="$1"
    
    log ERROR "RMAN Error Details:"
    
    if [[ -f "$rman_log" ]]; then
        # Extract error messages
        local errors=$(grep -E "RMAN-[0-9]+:|ORA-[0-9]+:" "$rman_log" 2>/dev/null || true)
        
        if [[ -n "$errors" ]]; then
            echo "$errors" | while IFS= read -r line; do
                log ERROR "$line"
            done
        else
            log ERROR "No specific error codes found in RMAN log"
        fi
        
        # Show last few lines of log for context
        log ERROR "Last 10 lines of RMAN log:"
        tail -10 "$rman_log" | while IFS= read -r line; do
            log ERROR "$line"
        done
    fi
}

# Validate backups
validate_backups() {
    log INFO "Validating recent backups..."
    
    local validation_script="$BACKUP_DIR/validate_$(date +%Y%m%d_%H%M%S).rman"
    
    cat > "$validation_script" <<EOF
# Validate all backups
VALIDATE BACKUP;

# Check for corrupted backups
LIST BACKUP;

# Report any issues
REPORT OBSOLETE;
REPORT NEED BACKUP;

EXIT;
EOF

    if rman target / cmdfile="$validation_script" log="$BACKUP_DIR/validate.log"; then
        log INFO "Backup validation completed successfully"
        
        # Check for any validation errors
        if grep -q "validation failed\|found corruption\|FAILED" "$BACKUP_DIR/validate.log"; then
            log WARN "Validation found issues - check validate.log"
        else
            log INFO "All backups validated successfully"
        fi
    else
        log ERROR "Backup validation failed"
    fi
    
    # Cleanup validation script
    rm -f "$validation_script"
}

# Cleanup old backup files
cleanup_old_backups() {
    log INFO "Cleaning up old backup files..."
    
    # Find and remove old backup files
    local deleted_count=0
    
    while IFS= read -r -d '' file; do
        rm -f "$file"
        log INFO "Removed old backup file: $(basename "$file")"
        ((deleted_count++))
    done < <(find "$BACKUP_DIR" -name "*.bkp" -o -name "*.rman" -o -name "*.log" -type f -mtime +$RETENTION_DAYS -print0 2>/dev/null)
    
    if [[ $deleted_count -gt 0 ]]; then
        log INFO "Removed $deleted_count old backup file(s)"
    else
        log INFO "No old backup files to remove"
    fi
}

# Generate backup report
generate_backup_report() {
    local backup_type="$1"
    local status="$2"
    
    local report_file="$BACKUP_DIR/oracle_backup_report.csv"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Create header if file doesn't exist
    if [[ ! -f "$report_file" ]]; then
        echo "Timestamp,Oracle_SID,Backup_Type,Status,Backup_Dir,Duration" > "$report_file"
    fi
    
    local duration="N/A"
    echo "$timestamp,$ORACLE_SID,$backup_type,$status,$BACKUP_DIR,$duration" >> "$report_file"
    
    log INFO "Backup report updated: $report_file"
}

# Show database recovery area usage
show_recovery_area_usage() {
    log INFO "Recovery Area Usage:"
    
    sqlplus -s / as sysdba <<EOF
set pagesize 100 linesize 120
col name format a15
col space_limit format 999,999,999
col space_used format 999,999,999  
col space_reclaimable format 999,999,999
col pct_used format 999.99

select 
    name,
    round(space_limit/1024/1024) as "Limit_MB",
    round(space_used/1024/1024) as "Used_MB", 
    round(space_reclaimable/1024/1024) as "Reclaimable_MB",
    round((space_used/space_limit)*100, 2) as "Pct_Used"
from v\$recovery_area_usage
order by name;

exit;
EOF
}

# Main function
main() {
    local backup_type="$BACKUP_TYPE"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--sid)
                ORACLE_SID="$2"
                shift 2
                ;;
            -h|--oracle-home)
                ORACLE_HOME="$2"
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
            -r|--retention)
                RETENTION_DAYS="$2"
                shift 2
                ;;
            -p|--parallel)
                PARALLEL_DEGREE="$2"
                shift 2
                ;;
            --compress)
                COMPRESS="true"
                shift
                ;;
            --no-validate)
                VALIDATE_BACKUP="false"
                shift
                ;;
            --no-crosscheck)
                CROSSCHECK="false"
                shift
                ;;
            --no-delete)
                DELETE_OBSOLETE="false"
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
    
    # Validate backup type
    if [[ ! "$backup_type" =~ ^(FULL|INCREMENTAL|ARCHIVELOG)$ ]]; then
        log ERROR "Invalid backup type: $backup_type"
        log ERROR "Valid types: FULL, INCREMENTAL, ARCHIVELOG"
        exit 1
    fi
    
    # Update LOG_FILE path
    LOG_FILE="$BACKUP_DIR/rman_backup.log"
    
    log INFO "Starting Oracle RMAN backup process"
    log INFO "Oracle SID: $ORACLE_SID"
    log INFO "Oracle Home: $ORACLE_HOME"
    log INFO "Backup Type: $backup_type"
    log INFO "Backup Directory: $BACKUP_DIR"
    log INFO "Retention Days: $RETENTION_DAYS"
    log INFO "Parallel Degree: $PARALLEL_DEGREE"
    log INFO "Compression: $COMPRESS"
    
    # Check Oracle environment
    check_oracle_environment
    
    # Create backup directory
    create_backup_directory
    
    # Test database connection
    if ! test_database_connection; then
        generate_backup_report "$backup_type" "Connection Failed"
        exit 1
    fi
    
    # Get database information
    get_database_info
    
    # Show recovery area usage
    show_recovery_area_usage
    
    # Generate and execute RMAN script
    local script_file
    script_file=$(generate_rman_script "$backup_type")
    
    if execute_rman_backup "$script_file" "$backup_type"; then
        log INFO "Backup completed successfully"
        
        # Validate backups if enabled
        if [[ "$VALIDATE_BACKUP" == "true" ]]; then
            validate_backups
        fi
        
        # Cleanup old backups
        cleanup_old_backups
        
        # Generate success report
        generate_backup_report "$backup_type" "Success"
        
        log INFO "RMAN backup process completed successfully"
        exit 0
    else
        log ERROR "Backup process failed"
        generate_backup_report "$backup_type" "Failed"
        exit 1
    fi
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi