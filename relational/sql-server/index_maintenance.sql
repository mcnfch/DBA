-- SQL Server Index Maintenance Script
-- Author: DBA Portfolio
-- Purpose: Automated index maintenance based on fragmentation levels

SET NOCOUNT ON;

-- Variables for configuration
DECLARE @FragmentationThresholdReorganize FLOAT = 5.0;  -- Reorganize threshold
DECLARE @FragmentationThresholdRebuild FLOAT = 30.0;    -- Rebuild threshold  
DECLARE @MinPageCount INT = 1000;                       -- Minimum pages for consideration
DECLARE @MaxDurationMinutes INT = 240;                  -- Maximum runtime in minutes
DECLARE @UpdateStats BIT = 1;                          -- Update statistics after maintenance
DECLARE @OnlineRebuild BIT = 1;                        -- Use online rebuild if available
DECLARE @LogProgress BIT = 1;                          -- Log progress to table

-- Create maintenance log table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[IndexMaintenanceLog]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[IndexMaintenanceLog]
    (
        [LogID] [int] IDENTITY(1,1) NOT NULL,
        [ExecutionDate] [datetime2] NOT NULL DEFAULT (GETDATE()),
        [SchemaName] [nvarchar](128) NOT NULL,
        [TableName] [nvarchar](128) NOT NULL,
        [IndexName] [nvarchar](128) NOT NULL,
        [Operation] [nvarchar](20) NOT NULL,
        [FragmentationBefore] [float] NULL,
        [FragmentationAfter] [float] NULL,
        [PageCount] [bigint] NULL,
        [DurationSeconds] [int] NULL,
        [ErrorMessage] [nvarchar](max) NULL,
        CONSTRAINT [PK_IndexMaintenanceLog] PRIMARY KEY CLUSTERED ([LogID] ASC)
    );
    
    PRINT 'Created IndexMaintenanceLog table';
END

-- Create temp table for fragmentation analysis
CREATE TABLE #FragmentationInfo
(
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128), 
    IndexName NVARCHAR(128),
    IndexID INT,
    FragmentationPercent FLOAT,
    PageCount BIGINT,
    Operation NVARCHAR(20),
    Priority INT
);

-- Analyze fragmentation for all user indexes
INSERT INTO #FragmentationInfo
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    i.name AS IndexName,
    i.index_id AS IndexID,
    ips.avg_fragmentation_in_percent AS FragmentationPercent,
    ips.page_count AS PageCount,
    CASE 
        WHEN ips.avg_fragmentation_in_percent >= @FragmentationThresholdRebuild THEN 'REBUILD'
        WHEN ips.avg_fragmentation_in_percent >= @FragmentationThresholdReorganize THEN 'REORGANIZE'
        ELSE 'NONE'
    END AS Operation,
    CASE 
        WHEN ips.avg_fragmentation_in_percent >= @FragmentationThresholdRebuild THEN 1
        WHEN ips.avg_fragmentation_in_percent >= @FragmentationThresholdReorganize THEN 2
        ELSE 3
    END AS Priority
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE ips.page_count >= @MinPageCount
  AND i.index_id > 0  -- Exclude heap
  AND i.is_disabled = 0
  AND t.is_ms_shipped = 0;

-- Summary of findings
PRINT 'Index Fragmentation Analysis Summary:';
PRINT '=====================================';

SELECT 
    Operation,
    COUNT(*) as IndexCount,
    SUM(PageCount) as TotalPages,
    AVG(FragmentationPercent) as AvgFragmentation,
    MIN(FragmentationPercent) as MinFragmentation,
    MAX(FragmentationPercent) as MaxFragmentation
FROM #FragmentationInfo 
WHERE Operation != 'NONE'
GROUP BY Operation
ORDER BY 
    CASE Operation 
        WHEN 'REBUILD' THEN 1 
        WHEN 'REORGANIZE' THEN 2 
    END;

-- Detailed list of indexes requiring maintenance
SELECT TOP 50
    SchemaName,
    TableName, 
    IndexName,
    CAST(FragmentationPercent AS DECIMAL(5,2)) as FragmentationPercent,
    PageCount,
    Operation
FROM #FragmentationInfo 
WHERE Operation != 'NONE'
ORDER BY Priority, FragmentationPercent DESC, PageCount DESC;

-- Variables for execution loop
DECLARE @SQL NVARCHAR(MAX);
DECLARE @SchemaName NVARCHAR(128);
DECLARE @TableName NVARCHAR(128);
DECLARE @IndexName NVARCHAR(128);
DECLARE @IndexID INT;
DECLARE @Operation NVARCHAR(20);
DECLARE @FragmentationBefore FLOAT;
DECLARE @PageCount BIGINT;
DECLARE @StartTime DATETIME2;
DECLARE @EndTime DATETIME2;
DECLARE @DurationSeconds INT;
DECLARE @ErrorMessage NVARCHAR(MAX);
DECLARE @ProcessStartTime DATETIME2 = GETDATE();
DECLARE @ProcessedCount INT = 0;
DECLARE @EditionSupportsOnline BIT = 0;

-- Check if edition supports online operations
IF SERVERPROPERTY('EngineEdition') IN (3, 5, 8) -- Enterprise, Standard (limited), Managed Instance
    SET @EditionSupportsOnline = 1;

-- Cursor for processing each index
DECLARE index_cursor CURSOR FOR
SELECT SchemaName, TableName, IndexName, IndexID, Operation, FragmentationPercent, PageCount
FROM #FragmentationInfo 
WHERE Operation != 'NONE'
ORDER BY Priority, FragmentationPercent DESC, PageCount DESC;

OPEN index_cursor;
FETCH NEXT FROM index_cursor INTO @SchemaName, @TableName, @IndexName, @IndexID, @Operation, @FragmentationBefore, @PageCount;

WHILE @@FETCH_STATUS = 0 AND DATEDIFF(MINUTE, @ProcessStartTime, GETDATE()) < @MaxDurationMinutes
BEGIN
    SET @StartTime = GETDATE();
    SET @ErrorMessage = NULL;
    
    BEGIN TRY
        -- Build the maintenance command
        IF @Operation = 'REBUILD'
        BEGIN
            SET @SQL = N'ALTER INDEX [' + @IndexName + '] ON [' + @SchemaName + '].[' + @TableName + '] REBUILD';
            
            -- Add online option if supported and enabled
            IF @OnlineRebuild = 1 AND @EditionSupportsOnline = 1
            BEGIN
                -- Check if index can be rebuilt online (no LOB columns, etc.)
                DECLARE @CanRebuildOnline BIT = 1;
                
                -- Check for LOB columns that prevent online rebuild
                IF EXISTS (
                    SELECT 1 
                    FROM sys.columns c
                    INNER JOIN sys.index_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE ic.object_id = OBJECT_ID(@SchemaName + '.' + @TableName)
                      AND ic.index_id = @IndexID
                      AND c.system_type_id IN (34, 35, 99, 241) -- image, text, ntext, xml
                )
                    SET @CanRebuildOnline = 0;
                
                IF @CanRebuildOnline = 1
                    SET @SQL = @SQL + N' WITH (ONLINE = ON, MAXDOP = 0)';
                ELSE
                    SET @SQL = @SQL + N' WITH (MAXDOP = 0)';
            END
            ELSE
            BEGIN
                SET @SQL = @SQL + N' WITH (MAXDOP = 0)';
            END
        END
        ELSE -- REORGANIZE
        BEGIN
            SET @SQL = N'ALTER INDEX [' + @IndexName + '] ON [' + @SchemaName + '].[' + @TableName + '] REORGANIZE';
        END
        
        -- Execute the maintenance command
        PRINT 'Executing: ' + @Operation + ' on ' + @SchemaName + '.' + @TableName + '.' + @IndexName + 
              ' (Fragmentation: ' + CAST(@FragmentationBefore AS VARCHAR(10)) + '%, Pages: ' + CAST(@PageCount AS VARCHAR(20)) + ')';
        
        EXEC sp_executesql @SQL;
        
        SET @EndTime = GETDATE();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        SET @ProcessedCount = @ProcessedCount + 1;
        
        PRINT 'Completed in ' + CAST(@DurationSeconds AS VARCHAR(10)) + ' seconds';
        
        -- Update statistics if enabled and this was a rebuild
        IF @UpdateStats = 1 AND @Operation = 'REBUILD'
        BEGIN
            SET @SQL = N'UPDATE STATISTICS [' + @SchemaName + '].[' + @TableName + '] [' + @IndexName + '] WITH FULLSCAN';
            EXEC sp_executesql @SQL;
            PRINT 'Statistics updated';
        END
        
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @EndTime = GETDATE();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        
        PRINT 'Error processing ' + @SchemaName + '.' + @TableName + '.' + @IndexName + ': ' + @ErrorMessage;
    END CATCH
    
    -- Log the operation if enabled
    IF @LogProgress = 1
    BEGIN
        INSERT INTO [dbo].[IndexMaintenanceLog] 
        (SchemaName, TableName, IndexName, Operation, FragmentationBefore, PageCount, DurationSeconds, ErrorMessage)
        VALUES 
        (@SchemaName, @TableName, @IndexName, @Operation, @FragmentationBefore, @PageCount, @DurationSeconds, @ErrorMessage);
    END
    
    FETCH NEXT FROM index_cursor INTO @SchemaName, @TableName, @IndexName, @IndexID, @Operation, @FragmentationBefore, @PageCount;
END

CLOSE index_cursor;
DEALLOCATE index_cursor;

-- Final summary
DECLARE @TotalDurationMinutes INT = DATEDIFF(MINUTE, @ProcessStartTime, GETDATE());

PRINT '';
PRINT 'Index Maintenance Execution Summary:';
PRINT '===================================';
PRINT 'Total Duration: ' + CAST(@TotalDurationMinutes AS VARCHAR(10)) + ' minutes';
PRINT 'Indexes Processed: ' + CAST(@ProcessedCount AS VARCHAR(10));

IF DATEDIFF(MINUTE, @ProcessStartTime, GETDATE()) >= @MaxDurationMinutes
    PRINT 'WARNING: Maintenance stopped due to time limit';

-- Show recent maintenance log entries
IF @LogProgress = 1
BEGIN
    PRINT '';
    PRINT 'Recent Maintenance Operations:';
    PRINT '==============================';
    
    SELECT TOP 20
        ExecutionDate,
        SchemaName + '.' + TableName + '.' + IndexName AS ObjectName,
        Operation,
        CAST(FragmentationBefore AS DECIMAL(5,2)) as FragBefore,
        DurationSeconds,
        CASE WHEN ErrorMessage IS NULL THEN 'Success' ELSE 'Failed' END as Status
    FROM [dbo].[IndexMaintenanceLog]
    WHERE ExecutionDate >= @ProcessStartTime
    ORDER BY ExecutionDate DESC;
END

-- Cleanup
DROP TABLE #FragmentationInfo;

PRINT 'Index maintenance completed.';
GO

-- Additional query to check fragmentation after maintenance
-- (Run this separately if needed)
/*
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    i.name AS IndexName,
    ips.avg_fragmentation_in_percent AS CurrentFragmentation,
    ips.page_count AS PageCount,
    CASE 
        WHEN ips.avg_fragmentation_in_percent < 5 THEN 'Good'
        WHEN ips.avg_fragmentation_in_percent < 30 THEN 'Moderate'
        ELSE 'High'
    END AS FragmentationLevel
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE ips.page_count >= 1000
  AND i.index_id > 0
  AND i.is_disabled = 0
  AND t.is_ms_shipped = 0
ORDER BY ips.avg_fragmentation_in_percent DESC;
*/