// Services/SqlServerService.cs
using MDBImporter.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace MDBImporter.Services
{
    public class SqlServerService
    {
        private readonly string _connectionString;
        private readonly ILogger<SqlServerService> _logger;

        // 修改构造函数，使用IConfiguration获取连接字符串
        public SqlServerService(IConfiguration configuration, ILogger<SqlServerService> logger)
        {
            _logger = logger;
            _connectionString = configuration.GetConnectionString("SqlServer") ??
                              throw new InvalidOperationException("SQL Server连接字符串未配置");

            _logger.LogInformation("SqlServerService初始化完成");
        }

        // 原来的构造函数（保留但不使用）
        public SqlServerService(string connectionString)
        {
            _connectionString = connectionString;
        }

        // 测试数据库连接
        public async Task<bool> TestConnectionAsync()
        {
            try
            {
                _logger.LogInformation("测试SQL Server连接...");
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var result = connection.State == ConnectionState.Open;
                if (result)
                {
                    _logger.LogInformation("SQL Server连接测试成功");
                }
                else
                {
                    _logger.LogWarning("SQL Server连接测试失败");
                }

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SQL Server连接测试失败");
                return false;
            }
        }

        // 创建导入历史表
        public async Task CreateImportHistoryTableAsync()
        {
            _logger.LogInformation("创建/检查ImportHistory表...");

            var sql = @"
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ImportHistory' AND xtype='U')
                BEGIN
                    CREATE TABLE ImportHistory (
                        Id INT IDENTITY(1,1) PRIMARY KEY,
                        ComputerName NVARCHAR(100),
                        TableName NVARCHAR(200),
                        ImportTime DATETIME,
                        RecordsImported INT,
                        Status NVARCHAR(50),
                        ErrorMessage NVARCHAR(MAX),
                        FileName NVARCHAR(255),
                        FileSize BIGINT,
                        ImportDuration INT
                    )
                    
                    CREATE INDEX IX_ImportHistory_ImportTime ON ImportHistory(ImportTime DESC)
                    CREATE INDEX IX_ImportHistory_ComputerName ON ImportHistory(ComputerName)
                    CREATE INDEX IX_ImportHistory_Status ON ImportHistory(Status)
                    
                    PRINT 'ImportHistory表创建成功'
                END
                ELSE
                BEGIN
                    PRINT 'ImportHistory表已存在'
                END";

            try
            {
                await ExecuteNonQueryAsync(sql);
                _logger.LogInformation("ImportHistory表检查完成");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "创建ImportHistory表失败");
                throw;
            }
        }

        // 记录导入历史
        public async Task LogImportHistoryAsync(ImportHistory history)
        {
            var sql = @"
                INSERT INTO ImportHistory 
                (ComputerName, TableName, ImportTime, RecordsImported,Status, ErrorMessage, FileName, FileSize, ImportDuration) 
                VALUES 
                (@ComputerName, @TableName, @ImportTime, @RecordsImported,@Status, @ErrorMessage, @FileName, @FileSize, @ImportDuration)";
            try
            {
                using var connection = new SqlConnection(_connectionString);
                using var command = new SqlCommand(sql, connection);

                command.Parameters.AddWithValue("@ComputerName", history.ComputerName);
                command.Parameters.AddWithValue("@TableName", history.TableName);
                command.Parameters.AddWithValue("@ImportTime", DateTime.Now);
                command.Parameters.AddWithValue("@RecordsImported", history.RecordsImported);
                command.Parameters.AddWithValue("@Status", history.Status);
                command.Parameters.AddWithValue("@ErrorMessage", history.ErrorMessage);
                command.Parameters.AddWithValue("@FileName", history.FileName);
                command.Parameters.AddWithValue("@FileSize", history.FileSize);
                command.Parameters.AddWithValue("@ImportDuration", history.ImportDuration);

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();

                _logger.LogDebug("导入历史记录成功: {TableName}, {Records}条", history.TableName, history.RecordsImported);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "记录导入历史失败");
            }
        }

        // 执行非查询SQL
        public async Task ExecuteNonQueryAsync(string sql)
        {
            using var connection = new SqlConnection(_connectionString);
            using var command = new SqlCommand(sql, connection);
            await connection.OpenAsync();
            await command.ExecuteNonQueryAsync();
        }

        // 获取表统计数据
        public async Task<DataTable> GetTableStatisticsAsync()
        {
            var sql = @"
                SELECT 
                    t.name AS TableName,
                    p.rows AS RowCounts,
                    SUM(a.total_pages) * 8 AS TotalSpaceKB,
                    SUM(a.used_pages) * 8 AS UsedSpaceKB,
                    MAX(h.ImportTime) AS LastImportTime,
                    SUM(h.RecordsImported) AS TotalImported
                FROM sys.tables t
                LEFT JOIN sys.indexes i ON t.object_id = i.object_id
                LEFT JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
                LEFT JOIN (
                    SELECT TableName, MAX(ImportTime) as ImportTime, SUM(RecordsImported) as RecordsImported
                    FROM ImportHistory 
                    WHERE Status = 'Success'
                    GROUP BY TableName
                ) h ON t.name = h.TableName
                WHERE t.is_ms_shipped = 0
                GROUP BY t.name, p.rows
                ORDER BY t.name";

            using var connection = new SqlConnection(_connectionString);
            using var command = new SqlCommand(sql, connection);
            using var adapter = new SqlDataAdapter(command);
            var dataTable = new DataTable();

            await connection.OpenAsync();
            adapter.Fill(dataTable);

            _logger.LogInformation("获取表统计数据: {Count}个表", dataTable.Rows.Count);
            return dataTable;
        }

        // 获取连接字符串（供其他服务使用）
        public string GetConnectionString()
        {
            return _connectionString;
        }

        // 执行查询并返回DataTable
        public async Task<DataTable> ExecuteQueryAsync(string sql)
        {
            using var connection = new SqlConnection(_connectionString);
            using var command = new SqlCommand(sql, connection);
            using var adapter = new SqlDataAdapter(command);
            var dataTable = new DataTable();

            await connection.OpenAsync();
            adapter.Fill(dataTable);

            return dataTable;
        }

        // 获取数据库信息
        public async Task<Dictionary<string, object>> GetDatabaseInfoAsync()
        {
            var info = new Dictionary<string, object>();

            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                // 获取数据库名称
                using var cmd1 = new SqlCommand("SELECT DB_NAME()", connection);
                info["DatabaseName"] = await cmd1.ExecuteScalarAsync() ?? "未知";

                // 获取版本信息
                using var cmd2 = new SqlCommand("SELECT @@VERSION", connection);
                info["Version"] = await cmd2.ExecuteScalarAsync() ?? "未知";

                // 获取表数量
                using var cmd3 = new SqlCommand(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'",
                    connection);
                info["TableCount"] = await cmd3.ExecuteScalarAsync() ?? 0;

                // 获取总记录数
                using var cmd4 = new SqlCommand(@"
                    SELECT SUM(p.rows) 
                    FROM sys.tables t
                    INNER JOIN sys.partitions p ON t.object_id = p.object_id
                    WHERE t.is_ms_shipped = 0 AND p.index_id IN (0,1)",
                    connection);
                info["TotalRecords"] = await cmd4.ExecuteScalarAsync() ?? 0;

                // 获取数据库大小
                using var cmd5 = new SqlCommand(@"
                    SELECT 
                        SUM(size) * 8 / 1024 AS SizeMB,
                        SUM(CASE WHEN type = 0 THEN size ELSE 0 END) * 8 / 1024 AS DataSizeMB,
                        SUM(CASE WHEN type = 1 THEN size ELSE 0 END) * 8 / 1024 AS LogSizeMB
                    FROM sys.master_files 
                    WHERE database_id = DB_ID()",
                    connection);

                using var reader = await cmd5.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    info["TotalSizeMB"] = reader[0] != DBNull.Value ? reader.GetInt32(0) : 0;
                    info["DataSizeMB"] = reader[1] != DBNull.Value ? reader.GetInt32(1) : 0;
                    info["LogSizeMB"] = reader[2] != DBNull.Value ? reader.GetInt32(2) : 0;
                }

                _logger.LogInformation("获取数据库信息成功");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取数据库信息失败");
            }

            return info;
        }

        // 清空指定表
        public async Task<int> TruncateTableAsync(string tableName)
        {
            var sql = $"TRUNCATE TABLE [{tableName}]";

            try
            {
                await ExecuteNonQueryAsync(sql);
                _logger.LogInformation("清空表成功: {TableName}", tableName);
                return 1;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "清空表失败: {TableName}", tableName);
                throw;
            }
        }

        // 删除指定表
        public async Task<int> DropTableAsync(string tableName)
        {
            var sql = $"DROP TABLE IF EXISTS [{tableName}]";

            try
            {
                await ExecuteNonQueryAsync(sql);
                _logger.LogInformation("删除表成功: {TableName}", tableName);
                return 1;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "删除表失败: {TableName}", tableName);
                throw;
            }
        }




        // 删除所有导入的表
        public async Task<int> DeleteAllImportTablesAsync()
        {
            var deletedCount = 0;

            try
            {
                _logger.LogInformation("开始删除所有导入的表...");

                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                // 获取所有导入的表（根据命名模式）
                var sql = @"
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' 
                AND TABLE_NAME NOT IN ('ImportHistory')  -- 排除历史表
                ORDER BY TABLE_NAME";

                var tablesToDelete = new List<string>();

                using (var command = new SqlCommand(sql, connection))
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        tablesToDelete.Add(reader["TABLE_NAME"].ToString());
                    }
                }

                if (tablesToDelete.Count == 0)
                {
                    _logger.LogInformation("没有找到要删除的导入表");
                    return 0;
                }

                _logger.LogInformation($"找到 {tablesToDelete.Count} 个导入表需要删除");

                // 禁用外键约束（如果需要）
                await ExecuteNonQueryAsync("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'");

                // 删除表
                foreach (var table in tablesToDelete)
                {
                    try
                    {
                        var dropSql = $"DROP TABLE [{table}]";
                        using var dropCommand = new SqlCommand(dropSql, connection);
                        await dropCommand.ExecuteNonQueryAsync();

                        deletedCount++;
                        _logger.LogInformation($"已删除表: {table}");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"删除表失败 {table}");
                    }
                }

                // 重新启用外键约束
                await ExecuteNonQueryAsync("EXEC sp_msforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'");

                _logger.LogInformation($"删除完成，共删除 {deletedCount} 个表");
                var history = new ImportHistory();
                history.ComputerName = "SYSTEM";
                history.TableName = "ALL_TABLES";
                history.ImportTime = DateTime.Now;
                history.RecordsImported = deletedCount;
                history.Status = "SystemOperation";
                history.ErrorMessage = string.Empty;
                history.FileName = string.Empty;
                history.FileSize = string.Empty;
                history.ImportDuration = string.Empty;
                // 记录删除历史
                await LogImportHistoryAsync(history);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "删除所有导入表失败");
                throw;
            }

            return deletedCount;
        }

        // 清空所有导入的表（只清空数据，保留表结构）
        public async Task<int> TruncateAllImportTablesAsync()
        {
            var truncatedCount = 0;

            try
            {
                _logger.LogInformation("开始清空所有导入的表...");

                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                // 获取所有导入的表
                var sql = @"
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT IN ('ImportHistory') ORDER BY TABLE_NAME";

                var tablesToTruncate = new List<string>();

                using (var command = new SqlCommand(sql, connection))
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        tablesToTruncate.Add(reader["TABLE_NAME"].ToString());
                    }
                }

                if (tablesToTruncate.Count == 0)
                {
                    _logger.LogInformation("没有找到要清空的导入表");
                    return 0;
                }

                _logger.LogInformation($"找到 {tablesToTruncate.Count} 个导入表需要清空");

                // 禁用外键约束
                await ExecuteNonQueryAsync("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'");

                // 清空表
                foreach (var table in tablesToTruncate)
                {
                    try
                    {
                        var truncateSql = $"TRUNCATE TABLE [{table}]";
                        using var truncateCommand = new SqlCommand(truncateSql, connection);
                        await truncateCommand.ExecuteNonQueryAsync();

                        truncatedCount++;
                        _logger.LogInformation($"已清空表: {table}");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"清空表失败 {table}");
                    }
                }

                // 重新启用外键约束
                await ExecuteNonQueryAsync("EXEC sp_msforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'");

                _logger.LogInformation($"清空完成，共清空 {truncatedCount} 个表");
                var history = new ImportHistory();
                history.ComputerName = "SYSTEM";
                history.TableName = "ALL_TABLES";
                history.ImportTime = DateTime.Now;
                history.RecordsImported = truncatedCount;
                history.Status = "Truncated";
                history.ErrorMessage = string.Empty;
                history.FileName = string.Empty;
                history.FileSize = string.Empty;
                history.ImportDuration = string.Empty;
                // 记录清空历史
                await LogImportHistoryAsync(history);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "清空所有导入表失败");
                throw;
            }

            return truncatedCount;
        }

        // 获取导入表的统计信息
        public async Task<DataTable> GetImportTablesStatisticsAsync()
        {
            var sql = @"
            SELECT 
                t.name AS TableName,
                p.rows AS RowCounts,
                SUM(a.total_pages) * 8 AS TotalSpaceKB,
                SUM(a.used_pages) * 8 AS UsedSpaceKB,
                MAX(h.ImportTime) AS LastImportTime,
                SUM(h.RecordsImported) AS TotalImported,
                CASE 
                    WHEN t.name LIKE '%[_]%[_]%' THEN 'MDB导入表'
                    WHEN t.name LIKE 'MDB[_]%' THEN 'MDB导入表'
                    WHEN t.name LIKE 'COMPUTER%[_]%' THEN '计算机导入表'
                    ELSE '其他表'
                END AS TableType
            FROM sys.tables t
            LEFT JOIN sys.indexes i ON t.object_id = i.object_id
            LEFT JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
            LEFT JOIN (
                SELECT TableName, MAX(ImportTime) as ImportTime, SUM(RecordsImported) as RecordsImported
                FROM ImportHistory 
                WHERE Status = 'Success'
                GROUP BY TableName
            ) h ON t.name = h.TableName
            WHERE t.is_ms_shipped = 0
            AND t.name NOT IN ('ImportHistory')
            GROUP BY t.name, p.rows
            ORDER BY TableType, TableName";

            using var connection = new SqlConnection(_connectionString);
            using var command = new SqlCommand(sql, connection);
            using var adapter = new SqlDataAdapter(command);
            var dataTable = new DataTable();

            await connection.OpenAsync();
            adapter.Fill(dataTable);

            _logger.LogInformation("获取导入表统计: {Count}个表", dataTable.Rows.Count);
            return dataTable;
        }

        // 检查是否有导入表
        public async Task<bool> HasImportTablesAsync()
        {
            var sql = @"
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' 
            AND TABLE_NAME NOT IN ('ImportHistory')";

            try
            {
                using var connection = new SqlConnection(_connectionString);
                using var command = new SqlCommand(sql, connection);

                await connection.OpenAsync();
                var result = await command.ExecuteScalarAsync();

                return Convert.ToInt32(result) > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "检查导入表是否存在失败");
                return false;
            }
        }

        // 获取导入表的详细信息
        public async Task<DataTable> GetImportTablesDetailsAsync()
        {
            var sql = @"
            SELECT 
                t.TABLE_NAME,
                t.TABLE_TYPE,
                c.COLUMN_COUNT,
                r.ROW_COUNT,
                h.LAST_IMPORT_TIME,
                h.TOTAL_IMPORTED,
                DATEDIFF(DAY, h.LAST_IMPORT_TIME, GETDATE()) AS DAYS_SINCE_IMPORT,
                CASE 
                    WHEN t.TABLE_NAME LIKE '%[_]%[_]%' THEN 'MDB导入表'
                    WHEN t.TABLE_NAME LIKE 'MDB[_]%' THEN 'MDB导入表'
                    WHEN t.TABLE_NAME LIKE 'COMPUTER%[_]%' THEN '计算机导入表'
                    ELSE '其他表'
                END AS IMPORT_TYPE,
                SUBSTRING(t.TABLE_NAME, 1, 
                    CASE 
                        WHEN CHARINDEX('_', t.TABLE_NAME, CHARINDEX('_', t.TABLE_NAME) + 1) > 0 
                        THEN CHARINDEX('_', t.TABLE_NAME, CHARINDEX('_', t.TABLE_NAME) + 1) - 1
                        ELSE LEN(t.TABLE_NAME)
                    END) AS PREFIX_INFO
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN (
                SELECT TABLE_NAME, COUNT(*) AS COLUMN_COUNT
                FROM INFORMATION_SCHEMA.COLUMNS
                GROUP BY TABLE_NAME
            ) c ON t.TABLE_NAME = c.TABLE_NAME
            LEFT JOIN (
                SELECT 
                    t.name AS TABLE_NAME,
                    SUM(p.rows) AS ROW_COUNT
                FROM sys.tables t
                INNER JOIN sys.partitions p ON t.object_id = p.object_id
                WHERE p.index_id IN (0, 1)
                GROUP BY t.name
            ) r ON t.TABLE_NAME = r.TABLE_NAME
            LEFT JOIN (
                SELECT 
                    TableName,
                    MAX(ImportTime) AS LAST_IMPORT_TIME,
                    SUM(RecordsImported) AS TOTAL_IMPORTED
                FROM ImportHistory
                WHERE Status = 'Success'
                GROUP BY TableName
            ) h ON t.TABLE_NAME = h.TableName
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            AND (
                t.TABLE_NAME LIKE '%[_]%[_]%' OR
                t.TABLE_NAME LIKE 'MDB[_]%' OR
                t.TABLE_NAME LIKE 'COMPUTER%[_]%'
            )
            AND t.TABLE_NAME NOT IN ('ImportHistory')
            ORDER BY t.TABLE_NAME";

            using var connection = new SqlConnection(_connectionString);
            using var command = new SqlCommand(sql, connection);
            using var adapter = new SqlDataAdapter(command);
            var dataTable = new DataTable();

            await connection.OpenAsync();
            adapter.Fill(dataTable);

            return dataTable;
        }


        // 检查表是否存在
        public async Task<bool> TableExistsAsync(string tableName)
        {
            var sql = @"
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = @TableName AND TABLE_TYPE = 'BASE TABLE'";

            try
            {
                using var connection = new SqlConnection(_connectionString);
                using var command = new SqlCommand(sql, connection);
                command.Parameters.AddWithValue("@TableName", tableName);

                await connection.OpenAsync();
                var result = await command.ExecuteScalarAsync();

                return Convert.ToInt32(result) > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "检查表是否存在失败: {TableName}", tableName);
                return false;
            }
        }

        // 获取表的列信息
        public async Task<DataTable> GetTableColumnsAsync(string tableName)
        {
            var sql = @"
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = @TableName
                ORDER BY ORDINAL_POSITION";

            using var connection = new SqlConnection(_connectionString);
            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@TableName", tableName);
            using var adapter = new SqlDataAdapter(command);
            var dataTable = new DataTable();

            await connection.OpenAsync();
            adapter.Fill(dataTable);

            return dataTable;
        }
    }
}