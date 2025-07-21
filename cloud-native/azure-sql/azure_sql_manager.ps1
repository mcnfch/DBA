# Azure SQL Database Management Script (PowerShell)
# Author: DBA Portfolio
# Purpose: Comprehensive Azure SQL Database management and monitoring

param(
    [Parameter(Mandatory=$false)]
    [string]$SubscriptionId,
    
    [Parameter(Mandatory=$false)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$false)]
    [string]$ServerName,
    
    [Parameter(Mandatory=$false)]
    [string]$DatabaseName,
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("Info", "Backup", "Monitor", "Scale", "Restore")]
    [string]$Action = "Info",
    
    [Parameter(Mandatory=$false)]
    [string]$BackupRetentionDays = "7",
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("S0", "S1", "S2", "S3", "P1", "P2", "P4", "P6", "P11", "P15")]
    [string]$ServiceTier,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = ".",
    
    [Parameter(Mandatory=$false)]
    [switch]$EnableLongTermRetention
)

# Import required modules
try {
    Import-Module Az.Accounts -ErrorAction Stop
    Import-Module Az.Sql -ErrorAction Stop  
    Import-Module Az.Monitor -ErrorAction Stop
    Write-Host "Azure PowerShell modules loaded successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to load Azure PowerShell modules. Please install Az modules:" -ForegroundColor Red
    Write-Host "Install-Module -Name Az -AllowClobber -Scope CurrentUser" -ForegroundColor Yellow
    exit 1
}

function Write-Log {
    param($Message, $Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    
    switch ($Level) {
        "ERROR" { Write-Host $logMessage -ForegroundColor Red }
        "WARN"  { Write-Host $logMessage -ForegroundColor Yellow }
        "INFO"  { Write-Host $logMessage -ForegroundColor Green }
        default { Write-Host $logMessage }
    }
    
    # Write to log file
    $logFile = Join-Path $OutputPath "azure_sql_log.txt"
    Add-Content -Path $logFile -Value $logMessage
}

function Connect-ToAzure {
    Write-Log "Checking Azure authentication..."
    
    try {
        $context = Get-AzContext
        if ($context) {
            Write-Log "Using existing Azure session for account: $($context.Account.Id)"
            
            if ($SubscriptionId) {
                Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
                Write-Log "Switched to subscription: $SubscriptionId"
            }
        } else {
            Write-Log "No existing Azure session found. Please authenticate..."
            Connect-AzAccount
        }
    }
    catch {
        Write-Log "Azure authentication failed: $($_.Exception.Message)" "ERROR"
        exit 1
    }
}

function Get-AzureSQLInfo {
    Write-Log "Gathering Azure SQL Database information..."
    
    try {
        # Get subscription info
        $subscription = Get-AzContext
        Write-Log "Subscription: $($subscription.Subscription.Name) ($($subscription.Subscription.Id))"
        
        # Get all SQL servers if not specified
        if (-not $ServerName) {
            Write-Log "Getting all SQL servers in subscription..."
            $servers = Get-AzSqlServer
            
            Write-Log "Found $($servers.Count) SQL server(s):"
            foreach ($server in $servers) {
                Write-Host "  Server: $($server.ServerName)" -ForegroundColor Cyan
                Write-Host "    Resource Group: $($server.ResourceGroupName)"
                Write-Host "    Location: $($server.Location)"
                Write-Host "    Version: $($server.ServerVersion)"
                Write-Host "    Admin: $($server.SqlAdministratorLogin)"
                Write-Host ""
            }
            return
        }
        
        # Get specific server info
        $server = Get-AzSqlServer -ResourceGroupName $ResourceGroupName -ServerName $ServerName
        Write-Log "Server Information:"
        Write-Host "  Name: $($server.ServerName)" -ForegroundColor Cyan
        Write-Host "  Resource Group: $($server.ResourceGroupName)"
        Write-Host "  Location: $($server.Location)"
        Write-Host "  Version: $($server.ServerVersion)"
        Write-Host "  FQDN: $($server.FullyQualifiedDomainName)"
        Write-Host "  Admin Login: $($server.SqlAdministratorLogin)"
        
        # Get databases
        $databases = Get-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $ServerName
        Write-Log "Found $($databases.Count) database(s):"
        
        foreach ($db in $databases) {
            if ($db.DatabaseName -ne "master") {
                Write-Host "  Database: $($db.DatabaseName)" -ForegroundColor Yellow
                Write-Host "    Service Tier: $($db.CurrentServiceObjectiveName)"
                Write-Host "    Status: $($db.Status)"
                Write-Host "    Creation Date: $($db.CreationDate)"
                Write-Host "    Max Size: $([math]::Round($db.MaxSizeBytes / 1GB, 2)) GB"
                Write-Host "    Collation: $($db.CollationName)"
                
                # Get additional metrics
                try {
                    $usage = Get-AzSqlDatabaseUsage -ResourceGroupName $ResourceGroupName -ServerName $ServerName -DatabaseName $db.DatabaseName
                    Write-Host "    Current Size: $([math]::Round($usage.CurrentValue / 1MB, 2)) MB"
                }
                catch {
                    Write-Host "    Current Size: Unable to retrieve"
                }
                
                Write-Host ""
            }
        }
        
        # Get firewall rules
        Write-Log "Firewall Rules:"
        $firewallRules = Get-AzSqlServerFirewallRule -ResourceGroupName $ResourceGroupName -ServerName $ServerName
        foreach ($rule in $firewallRules) {
            Write-Host "  $($rule.FirewallRuleName): $($rule.StartIpAddress) - $($rule.EndIpAddress)" -ForegroundColor Magenta
        }
        
    }
    catch {
        Write-Log "Failed to get Azure SQL info: $($_.Exception.Message)" "ERROR"
    }
}

function Start-AzureSQLBackup {
    Write-Log "Configuring Azure SQL Database backup..."
    
    if (-not $DatabaseName) {
        Write-Log "Database name is required for backup configuration" "ERROR"
        return
    }
    
    try {
        # Get current database
        $database = Get-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $ServerName -DatabaseName $DatabaseName
        
        Write-Log "Current backup configuration for $DatabaseName:"
        Write-Host "  Service Tier: $($database.CurrentServiceObjectiveName)"
        
        # Configure backup retention policy
        if ($BackupRetentionDays) {
            Write-Log "Setting backup retention to $BackupRetentionDays days..."
            
            try {
                Set-AzSqlDatabaseBackupShortTermRetentionPolicy -ResourceGroupName $ResourceGroupName `
                    -ServerName $ServerName -DatabaseName $DatabaseName -RetentionDays $BackupRetentionDays
                Write-Log "Backup retention policy updated successfully"
            }
            catch {
                Write-Log "Failed to set backup retention: $($_.Exception.Message)" "ERROR"
            }
        }
        
        # Configure long-term retention if enabled
        if ($EnableLongTermRetention) {
            Write-Log "Configuring long-term retention policy..."
            
            try {
                Set-AzSqlDatabaseBackupLongTermRetentionPolicy -ResourceGroupName $ResourceGroupName `
                    -ServerName $ServerName -DatabaseName $DatabaseName `
                    -WeeklyRetention "P12W" -MonthlyRetention "P12M" -YearlyRetention "P5Y"
                    
                Write-Log "Long-term retention policy configured"
            }
            catch {
                Write-Log "Failed to set long-term retention: $($_.Exception.Message)" "ERROR"
            }
        }
        
        # Export database (BACPAC)
        $exportFile = "$DatabaseName-export-$(Get-Date -Format 'yyyyMMdd-HHmmss').bacpac"
        $storageAccount = "your-storage-account-name"  # Replace with actual storage account
        $storageKey = "your-storage-key"  # Replace with actual key
        
        Write-Log "Note: For BACPAC export, configure storage account details in script"
        
    }
    catch {
        Write-Log "Backup configuration failed: $($_.Exception.Message)" "ERROR"
    }
}

function Get-AzureSQLMetrics {
    Write-Log "Retrieving Azure SQL Database metrics..."
    
    if (-not $DatabaseName) {
        Write-Log "Database name is required for metrics" "ERROR"
        return
    }
    
    try {
        # Define time range (last 24 hours)
        $endTime = Get-Date
        $startTime = $endTime.AddHours(-24)
        
        # Get database resource ID
        $database = Get-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $ServerName -DatabaseName $DatabaseName
        $resourceId = $database.ResourceId
        
        Write-Log "Collecting metrics for the last 24 hours..."
        
        # Define metrics to collect
        $metrics = @(
            @{ Name = "cpu_percent"; DisplayName = "CPU Percentage" },
            @{ Name = "physical_data_read_percent"; DisplayName = "Data IO Percentage" },
            @{ Name = "log_write_percent"; DisplayName = "Log Write Percentage" },
            @{ Name = "dtu_consumption_percent"; DisplayName = "DTU Percentage" },
            @{ Name = "connection_successful"; DisplayName = "Successful Connections" },
            @{ Name = "connection_failed"; DisplayName = "Failed Connections" },
            @{ Name = "storage_percent"; DisplayName = "Storage Percentage" }
        )
        
        $metricsReport = @()
        
        foreach ($metric in $metrics) {
            try {
                Write-Log "Collecting $($metric.DisplayName) metrics..."
                
                $metricData = Get-AzMetric -ResourceId $resourceId -MetricName $metric.Name `
                    -StartTime $startTime -EndTime $endTime -TimeGrain "01:00:00" -AggregationType "Average"
                
                if ($metricData.Data) {
                    $avgValue = ($metricData.Data | Measure-Object -Property Average -Average).Average
                    $maxValue = ($metricData.Data | Measure-Object -Property Average -Maximum).Maximum
                    
                    $metricsReport += [PSCustomObject]@{
                        Metric = $metric.DisplayName
                        Average = [math]::Round($avgValue, 2)
                        Maximum = [math]::Round($maxValue, 2)
                        Unit = $metricData.Unit
                    }
                }
            }
            catch {
                Write-Log "Failed to get $($metric.DisplayName) metrics: $($_.Exception.Message)" "WARN"
            }
        }
        
        # Display metrics
        Write-Log "Performance Metrics (Last 24 Hours):"
        $metricsReport | Format-Table -Property Metric, Average, Maximum, Unit -AutoSize
        
        # Save metrics to CSV
        $metricsFile = Join-Path $OutputPath "azure_sql_metrics_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
        $metricsReport | Export-Csv -Path $metricsFile -NoTypeInformation
        Write-Log "Metrics saved to: $metricsFile"
        
        # Check for performance issues
        Write-Log "Performance Analysis:"
        foreach ($metric in $metricsReport) {
            if ($metric.Metric -eq "CPU Percentage" -and $metric.Average -gt 80) {
                Write-Log "HIGH CPU USAGE detected: $($metric.Average)% average" "WARN"
            }
            if ($metric.Metric -eq "DTU Percentage" -and $metric.Average -gt 80) {
                Write-Log "HIGH DTU USAGE detected: $($metric.Average)% average" "WARN"
            }
            if ($metric.Metric -eq "Storage Percentage" -and $metric.Average -gt 90) {
                Write-Log "HIGH STORAGE USAGE detected: $($metric.Average)% average" "WARN"
            }
        }
        
    }
    catch {
        Write-Log "Failed to get metrics: $($_.Exception.Message)" "ERROR"
    }
}

function Set-AzureSQLScale {
    Write-Log "Scaling Azure SQL Database..."
    
    if (-not $DatabaseName -or -not $ServiceTier) {
        Write-Log "Database name and service tier are required for scaling" "ERROR"
        return
    }
    
    try {
        # Get current database configuration
        $database = Get-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $ServerName -DatabaseName $DatabaseName
        $currentTier = $database.CurrentServiceObjectiveName
        
        Write-Log "Current service tier: $currentTier"
        Write-Log "Target service tier: $ServiceTier"
        
        if ($currentTier -eq $ServiceTier) {
            Write-Log "Database is already at the target service tier" "INFO"
            return
        }
        
        # Confirm scaling operation
        $confirmation = Read-Host "Are you sure you want to scale $DatabaseName from $currentTier to $ServiceTier? (y/N)"
        if ($confirmation -ne 'y' -and $confirmation -ne 'Y') {
            Write-Log "Scaling operation cancelled"
            return
        }
        
        Write-Log "Starting scaling operation..."
        $startTime = Get-Date
        
        Set-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $ServerName `
            -DatabaseName $DatabaseName -RequestedServiceObjectiveName $ServiceTier
        
        $endTime = Get-Date
        $duration = $endTime - $startTime
        
        Write-Log "Scaling completed successfully in $($duration.TotalSeconds) seconds"
        
        # Verify new configuration
        $updatedDatabase = Get-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $ServerName -DatabaseName $DatabaseName
        Write-Log "New service tier: $($updatedDatabase.CurrentServiceObjectiveName)"
        
    }
    catch {
        Write-Log "Scaling failed: $($_.Exception.Message)" "ERROR"
    }
}

function Start-AzureSQLRestore {
    Write-Log "Azure SQL Database restore operations..."
    
    if (-not $DatabaseName) {
        Write-Log "Database name is required for restore operations" "ERROR"
        return
    }
    
    try {
        # List available restore points
        Write-Log "Available restore points for $DatabaseName:"
        
        $restorePoints = Get-AzSqlDatabaseRestorePoint -ResourceGroupName $ResourceGroupName `
            -ServerName $ServerName -DatabaseName $DatabaseName
        
        if ($restorePoints) {
            $restorePoints | Select-Object RestorePointType, @{Name="RestorePointCreationDate";Expression={$_.RestorePointCreationDate.ToString("yyyy-MM-dd HH:mm:ss")}} | 
                Format-Table -AutoSize
        } else {
            Write-Log "No restore points found"
        }
        
        # Show geo-backup information
        Write-Log "Checking geo-backup availability..."
        try {
            $geoBackups = Get-AzSqlDatabaseGeoBackup -ResourceGroupName $ResourceGroupName -ServerName $ServerName
            Write-Log "Found $($geoBackups.Count) geo-backup(s)"
        }
        catch {
            Write-Log "Geo-backup information not available" "WARN"
        }
        
        Write-Log "To restore database, use:"
        Write-Host "Restore-AzSqlDatabase -FromPointInTimeBackup -PointInTime '2023-01-01 12:00:00' -ResourceGroupName '$ResourceGroupName' -ServerName '$ServerName' -DatabaseName '$DatabaseName' -TargetDatabaseName '${DatabaseName}_restored'" -ForegroundColor Cyan
        
    }
    catch {
        Write-Log "Restore operation failed: $($_.Exception.Message)" "ERROR"
    }
}

function Generate-AzureSQLReport {
    Write-Log "Generating comprehensive Azure SQL report..."
    
    $reportData = @{
        Timestamp = Get-Date
        Subscription = (Get-AzContext).Subscription.Name
        ResourceGroup = $ResourceGroupName
        Server = $ServerName
        Database = $DatabaseName
    }
    
    # Create HTML report
    $htmlReport = @"
<!DOCTYPE html>
<html>
<head>
    <title>Azure SQL Database Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #0078d4; color: white; padding: 20px; }
        .section { margin: 20px 0; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .metric-good { color: green; }
        .metric-warn { color: orange; }
        .metric-error { color: red; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Azure SQL Database Report</h1>
        <p>Generated: $($reportData.Timestamp)</p>
    </div>
    
    <div class="section">
        <h2>Environment Information</h2>
        <table>
            <tr><th>Property</th><th>Value</th></tr>
            <tr><td>Subscription</td><td>$($reportData.Subscription)</td></tr>
            <tr><td>Resource Group</td><td>$($reportData.ResourceGroup)</td></tr>
            <tr><td>Server</td><td>$($reportData.Server)</td></tr>
            <tr><td>Database</td><td>$($reportData.Database)</td></tr>
        </table>
    </div>
</body>
</html>
"@
    
    $reportFile = Join-Path $OutputPath "azure_sql_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').html"
    $htmlReport | Out-File -FilePath $reportFile -Encoding UTF8
    Write-Log "Report generated: $reportFile"
}

# Main execution
try {
    Write-Log "Starting Azure SQL Database management script"
    Write-Log "Action: $Action"
    
    # Connect to Azure
    Connect-ToAzure
    
    # Execute requested action
    switch ($Action) {
        "Info" {
            Get-AzureSQLInfo
        }
        "Backup" {
            Start-AzureSQLBackup
        }
        "Monitor" {
            Get-AzureSQLMetrics
        }
        "Scale" {
            Set-AzureSQLScale
        }
        "Restore" {
            Start-AzureSQLRestore
        }
        default {
            Write-Log "Unknown action: $Action" "ERROR"
            exit 1
        }
    }
    
    # Generate comprehensive report
    if ($ResourceGroupName -and $ServerName) {
        Generate-AzureSQLReport
    }
    
    Write-Log "Azure SQL Database management completed successfully"
}
catch {
    Write-Log "Script execution failed: $($_.Exception.Message)" "ERROR"
    exit 1
}