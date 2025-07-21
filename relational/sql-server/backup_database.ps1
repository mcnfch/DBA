# SQL Server Database Backup Script (PowerShell)
# Author: DBA Portfolio
# Purpose: Comprehensive SQL Server backup automation with AG support

param(
    [Parameter(Mandatory=$true)]
    [string]$ServerInstance,
    
    [Parameter(Mandatory=$true)]
    [string]$DatabaseName,
    
    [Parameter(Mandatory=$false)]
    [string]$BackupPath = "C:\DBBackups\SQLServer",
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("FULL", "DIFF", "LOG")]
    [string]$BackupType = "FULL",
    
    [Parameter(Mandatory=$false)]
    [int]$RetentionDays = 7,
    
    [Parameter(Mandatory=$false)]
    [bool]$Compress = $true,
    
    [Parameter(Mandatory=$false)]
    [bool]$Verify = $true,
    
    [Parameter(Mandatory=$false)]
    [bool]$CheckSum = $true,
    
    [Parameter(Mandatory=$false)]
    [int]$MaxTransferSize = 4194304,  # 4MB
    
    [Parameter(Mandatory=$false)]
    [int]$BlockSize = 65536  # 64KB
)

# Import SQL Server module
try {
    Import-Module SqlServer -ErrorAction Stop
    Write-Host "SQL Server module loaded successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to load SQL Server module: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Create backup directory
if (!(Test-Path $BackupPath)) {
    New-Item -ItemType Directory -Path $BackupPath -Force | Out-Null
    Write-Host "Created backup directory: $BackupPath" -ForegroundColor Green
}

# Generate backup filename
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupExtension = switch ($BackupType) {
    "FULL" { "bak" }
    "DIFF" { "dif" }
    "LOG"  { "trn" }
}
$backupFileName = "$DatabaseName" + "_$BackupType" + "_$timestamp.$backupExtension"
$backupFile = Join-Path $BackupPath $backupFileName

function Write-Log {
    param($Message, $Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    Write-Host $logMessage -ForegroundColor $(
        switch ($Level) {
            "ERROR" { "Red" }
            "WARN"  { "Yellow" }
            "INFO"  { "Green" }
            default { "White" }
        }
    )
    
    $logFile = Join-Path $BackupPath "backup_log.txt"
    Add-Content -Path $logFile -Value $logMessage
}

function Test-DatabaseAccess {
    param($ServerInstance, $DatabaseName)
    
    try {
        $query = "SELECT name FROM sys.databases WHERE name = '$DatabaseName'"
        $result = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $query -ErrorAction Stop
        
        if ($result) {
            Write-Log "Database '$DatabaseName' found on server '$ServerInstance'"
            return $true
        } else {
            Write-Log "Database '$DatabaseName' not found on server '$ServerInstance'" "ERROR"
            return $false
        }
    }
    catch {
        Write-Log "Failed to connect to server '$ServerInstance': $($_.Exception.Message)" "ERROR"
        return $false
    }
}

function Get-DatabaseInfo {
    param($ServerInstance, $DatabaseName)
    
    $query = @"
SELECT 
    d.name,
    d.database_id,
    d.recovery_model_desc,
    d.state_desc,
    d.is_read_only,
    CAST(SUM(mf.size) * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb,
    d.compatibility_level,
    d.collation_name
FROM sys.databases d
INNER JOIN sys.master_files mf ON d.database_id = mf.database_id
WHERE d.name = '$DatabaseName'
GROUP BY d.name, d.database_id, d.recovery_model_desc, d.state_desc, 
         d.is_read_only, d.compatibility_level, d.collation_name
"@
    
    try {
        $dbInfo = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $query
        Write-Log "Database Info - Size: $($dbInfo.size_mb)MB, Recovery Model: $($dbInfo.recovery_model_desc), State: $($dbInfo.state_desc)"
        return $dbInfo
    }
    catch {
        Write-Log "Failed to get database info: $($_.Exception.Message)" "ERROR"
        return $null
    }
}

function Test-BackupPrerequisites {
    param($ServerInstance, $DatabaseName, $BackupType)
    
    # Check if database is online
    $query = "SELECT state_desc FROM sys.databases WHERE name = '$DatabaseName'"
    $state = (Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $query).state_desc
    
    if ($state -ne "ONLINE") {
        Write-Log "Database '$DatabaseName' is not online (Current state: $state)" "ERROR"
        return $false
    }
    
    # Check if it's a system database for LOG backup
    if ($BackupType -eq "LOG") {
        $systemDatabases = @("master", "model", "msdb", "tempdb")
        if ($DatabaseName -in $systemDatabases) {
            Write-Log "Transaction log backup not supported for system database '$DatabaseName'" "ERROR"
            return $false
        }
        
        # Check recovery model for log backup
        $query = "SELECT recovery_model_desc FROM sys.databases WHERE name = '$DatabaseName'"
        $recoveryModel = (Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $query).recovery_model_desc
        
        if ($recoveryModel -eq "SIMPLE") {
            Write-Log "Transaction log backup not available in SIMPLE recovery model" "ERROR"
            return $false
        }
    }
    
    return $true
}

function Invoke-BackupDatabase {
    param($ServerInstance, $DatabaseName, $BackupFile, $BackupType, $Options)
    
    # Build backup command
    $backupCmd = "BACKUP "
    
    switch ($BackupType) {
        "FULL" { $backupCmd += "DATABASE" }
        "DIFF" { $backupCmd += "DATABASE" }
        "LOG"  { $backupCmd += "LOG" }
    }
    
    $backupCmd += " [$DatabaseName] TO DISK = '$BackupFile'"
    
    if ($BackupType -eq "DIFF") {
        $backupCmd += " WITH DIFFERENTIAL"
    }
    
    if ($Options.Compress) {
        $backupCmd += ", COMPRESSION"
    }
    
    if ($Options.CheckSum) {
        $backupCmd += ", CHECKSUM"
    }
    
    if ($Options.Verify) {
        $backupCmd += ", INIT"
    } else {
        $backupCmd += ", NOINIT"
    }
    
    $backupCmd += ", MAXTRANSFERSIZE = $($Options.MaxTransferSize)"
    $backupCmd += ", BLOCKSIZE = $($Options.BlockSize)"
    $backupCmd += ", STATS = 5"
    
    Write-Log "Executing backup command: $backupCmd"
    
    try {
        $startTime = Get-Date
        Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $backupCmd -QueryTimeout 0
        $endTime = Get-Date
        $duration = $endTime - $startTime
        
        Write-Log "Backup completed successfully in $($duration.TotalSeconds) seconds"
        return $true
    }
    catch {
        Write-Log "Backup failed: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

function Test-BackupFile {
    param($ServerInstance, $BackupFile)
    
    if (-not $Verify) {
        return $true
    }
    
    $verifyCmd = "RESTORE VERIFYONLY FROM DISK = '$BackupFile'"
    
    try {
        Write-Log "Verifying backup file..."
        Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $verifyCmd -QueryTimeout 300
        Write-Log "Backup verification completed successfully"
        return $true
    }
    catch {
        Write-Log "Backup verification failed: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

function Remove-OldBackups {
    param($BackupPath, $DatabaseName, $BackupType, $RetentionDays)
    
    $pattern = "$DatabaseName" + "_$BackupType" + "_*"
    $cutoffDate = (Get-Date).AddDays(-$RetentionDays)
    
    Get-ChildItem -Path $BackupPath -Filter $pattern | Where-Object {
        $_.CreationTime -lt $cutoffDate
    } | ForEach-Object {
        Remove-Item $_.FullName -Force
        Write-Log "Removed old backup: $($_.Name)"
    }
}

function Get-BackupHistory {
    param($ServerInstance, $DatabaseName)
    
    $query = @"
SELECT TOP 10
    bs.database_name,
    bs.backup_start_date,
    bs.backup_finish_date,
    DATEDIFF(SECOND, bs.backup_start_date, bs.backup_finish_date) as duration_seconds,
    CASE bs.type
        WHEN 'D' THEN 'Full'
        WHEN 'I' THEN 'Differential'  
        WHEN 'L' THEN 'Transaction Log'
    END as backup_type,
    CAST(bs.backup_size / 1048576.0 AS DECIMAL(10,2)) as backup_size_mb,
    CAST(bs.compressed_backup_size / 1048576.0 AS DECIMAL(10,2)) as compressed_size_mb,
    bmf.physical_device_name
FROM msdb.dbo.backupset bs
INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
WHERE bs.database_name = '$DatabaseName'
ORDER BY bs.backup_start_date DESC
"@
    
    try {
        $history = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $query
        Write-Log "Recent backup history retrieved"
        return $history
    }
    catch {
        Write-Log "Failed to get backup history: $($_.Exception.Message)" "WARN"
        return $null
    }
}

# Main execution
try {
    Write-Log "Starting SQL Server backup process"
    Write-Log "Server: $ServerInstance, Database: $DatabaseName, Type: $BackupType"
    
    # Test database access
    if (-not (Test-DatabaseAccess -ServerInstance $ServerInstance -DatabaseName $DatabaseName)) {
        exit 1
    }
    
    # Get database information
    $dbInfo = Get-DatabaseInfo -ServerInstance $ServerInstance -DatabaseName $DatabaseName
    if (-not $dbInfo) {
        exit 1
    }
    
    # Test backup prerequisites
    if (-not (Test-BackupPrerequisites -ServerInstance $ServerInstance -DatabaseName $DatabaseName -BackupType $BackupType)) {
        exit 1
    }
    
    # Show recent backup history
    $history = Get-BackupHistory -ServerInstance $ServerInstance -DatabaseName $DatabaseName
    if ($history) {
        Write-Log "Last backup: $($history[0].backup_finish_date) ($($history[0].backup_type))"
    }
    
    # Prepare backup options
    $backupOptions = @{
        Compress = $Compress
        CheckSum = $CheckSum
        Verify = $Verify
        MaxTransferSize = $MaxTransferSize
        BlockSize = $BlockSize
    }
    
    # Perform backup
    if (Invoke-BackupDatabase -ServerInstance $ServerInstance -DatabaseName $DatabaseName -BackupFile $backupFile -BackupType $BackupType -Options $backupOptions) {
        
        # Verify backup
        if (Test-BackupFile -ServerInstance $ServerInstance -BackupFile $backupFile) {
            
            # Get backup file size
            $backupFileInfo = Get-Item $backupFile
            $backupSizeMB = [math]::Round($backupFileInfo.Length / 1MB, 2)
            Write-Log "Backup file size: $backupSizeMB MB"
            
            # Clean up old backups
            Remove-OldBackups -BackupPath $BackupPath -DatabaseName $DatabaseName -BackupType $BackupType -RetentionDays $RetentionDays
            
            # Log success to backup log CSV
            $logEntry = [PSCustomObject]@{
                Timestamp = Get-Date
                Server = $ServerInstance
                Database = $DatabaseName
                BackupType = $BackupType
                BackupFile = $backupFile
                SizeMB = $backupSizeMB
                Status = "Success"
                Duration = "N/A"
            }
            
            $csvFile = Join-Path $BackupPath "backup_report.csv"
            $logEntry | Export-Csv -Path $csvFile -Append -NoTypeInformation
            
            Write-Log "Backup process completed successfully"
            exit 0
        }
    }
    
    Write-Log "Backup process failed" "ERROR"
    exit 1
}
catch {
    Write-Log "Unexpected error: $($_.Exception.Message)" "ERROR"
    exit 1
}