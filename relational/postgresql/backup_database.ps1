# PostgreSQL Database Backup Script (PowerShell)
# Author: DBA Portfolio
# Purpose: Automated PostgreSQL database backup with compression and retention

param(
    [Parameter(Mandatory=$true)]
    [string]$DatabaseName,
    
    [Parameter(Mandatory=$false)]
    [string]$BackupPath = "C:\DBBackups\PostgreSQL",
    
    [Parameter(Mandatory=$false)]
    [string]$PgDumpPath = "C:\Program Files\PostgreSQL\15\bin\pg_dump.exe",
    
    [Parameter(Mandatory=$false)]
    [int]$RetentionDays = 7,
    
    [Parameter(Mandatory=$false)]
    [string]$Host = "localhost",
    
    [Parameter(Mandatory=$false)]
    [string]$Port = "5432",
    
    [Parameter(Mandatory=$false)]
    [string]$Username = "postgres"
)

# Create backup directory if it doesn't exist
if (!(Test-Path $BackupPath)) {
    New-Item -ItemType Directory -Path $BackupPath -Force
}

# Generate timestamp for backup file
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupFile = "$BackupPath\$DatabaseName" + "_backup_$timestamp.sql"
$compressedFile = "$backupFile.gz"

try {
    Write-Host "Starting backup of database: $DatabaseName" -ForegroundColor Green
    
    # Set PGPASSWORD environment variable (should be set separately for security)
    $env:PGPASSWORD = $env:PGPASSWORD
    
    # Execute pg_dump
    $arguments = @(
        "-h", $Host,
        "-p", $Port,
        "-U", $Username,
        "-d", $DatabaseName,
        "--verbose",
        "--clean",
        "--create",
        "--if-exists",
        "-f", $backupFile
    )
    
    $process = Start-Process -FilePath $PgDumpPath -ArgumentList $arguments -Wait -PassThru -NoNewWindow
    
    if ($process.ExitCode -eq 0) {
        Write-Host "Backup completed successfully: $backupFile" -ForegroundColor Green
        
        # Compress backup file
        if (Get-Command "gzip" -ErrorAction SilentlyContinue) {
            Start-Process -FilePath "gzip" -ArgumentList $backupFile -Wait
            Write-Host "Backup compressed: $compressedFile" -ForegroundColor Green
        }
        
        # Clean up old backups
        $oldBackups = Get-ChildItem -Path $BackupPath -Filter "$DatabaseName*backup*" | 
                     Where-Object { $_.CreationTime -lt (Get-Date).AddDays(-$RetentionDays) }
        
        foreach ($oldBackup in $oldBackups) {
            Remove-Item $oldBackup.FullName -Force
            Write-Host "Removed old backup: $($oldBackup.Name)" -ForegroundColor Yellow
        }
        
        # Log backup details
        $logEntry = @{
            Timestamp = Get-Date
            Database = $DatabaseName
            BackupFile = if (Test-Path $compressedFile) { $compressedFile } else { $backupFile }
            Size = if (Test-Path $compressedFile) { (Get-Item $compressedFile).Length } else { (Get-Item $backupFile).Length }
            Status = "Success"
        }
        
        $logEntry | Export-Csv -Path "$BackupPath\backup_log.csv" -Append -NoTypeInformation
        
    } else {
        throw "pg_dump failed with exit code: $($process.ExitCode)"
    }
    
} catch {
    Write-Host "Backup failed: $($_.Exception.Message)" -ForegroundColor Red
    
    # Log failure
    $logEntry = @{
        Timestamp = Get-Date
        Database = $DatabaseName
        BackupFile = "N/A"
        Size = 0
        Status = "Failed: $($_.Exception.Message)"
    }
    
    $logEntry | Export-Csv -Path "$BackupPath\backup_log.csv" -Append -NoTypeInformation
    exit 1
}