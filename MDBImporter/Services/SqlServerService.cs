// Services/SqlServerService.cs
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
        public async Task LogImportHistoryAsync(string computerName, string tableName,
            int recordsImported, string status, string errorMessage = "",
            string fileName = "", long fileSize = 0, int importDuration = 0)
        {
            var sql = @"
                INSERT INTO ImportHistory (
                    ComputerName, TableName, ImportTime, RecordsImported, 
                    Status, ErrorMessage, FileName, FileSize, ImportDuration
                ) VALUES (
                    @ComputerName, @TableName, @ImportTime, @RecordsImported,
                    @Status, @ErrorMessage, @FileName, @FileSize, @ImportDuration
                )";

            try
            {
                using var connection = new SqlConnection(_connectionString);
                using var command = new SqlCommand(sql, connection);

                command.Parameters.AddWithValue("@ComputerName", computerName ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@TableName", tableName ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@ImportTime", DateTime.Now);
                command.Parameters.AddWithValue("@RecordsImported", recordsImported);
                command.Parameters.AddWithValue("@Status", status ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@ErrorMessage", errorMessage ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@FileName", fileName ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@FileSize", fileSize);
                command.Parameters.AddWithValue("@ImportDuration", importDuration);

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();

                _logger.LogDebug("导入历史记录成功: {TableName}, {Records}条", tableName, recordsImported);
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