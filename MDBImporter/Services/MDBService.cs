// Services/MDBService.cs
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using MDBImporter.Core;

namespace MDBImporter.Services
{
    public class MDBService
    {
        private readonly SqlServerService _sqlServerService;
        private readonly ProviderHelper _providerHelper;
        private readonly ILogger<MDBService> _logger;

        public MDBService(SqlServerService sqlServerService,
                         ProviderHelper providerHelper,
                         ILogger<MDBService> logger)
        {
            _sqlServerService = sqlServerService;
            _providerHelper = providerHelper;
            _logger = logger;
        }

        // 测试MDB文件连接
        public bool TestMDBConnection(string mdbPath)
        {
            if (!File.Exists(mdbPath))
            {
                _logger.LogWarning($"MDB文件不存在: {mdbPath}");
                return false;
            }

            try
            {
                var connectionString = _providerHelper.GetConnectionString(mdbPath);
                using var connection = new OleDbConnection(connectionString);
                connection.Open();
                return connection.State == ConnectionState.Open;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"MDB连接测试失败: {mdbPath}");
                return false;
            }
        }

        // 获取MDB文件中的所有表名
        public List<string> GetTablesFromMDB(string mdbPath)
        {
            var tables = new List<string>();

            if (!File.Exists(mdbPath))
            {
                _logger.LogWarning($"MDB文件不存在: {mdbPath}");
                return tables;
            }

            try
            {
                var connectionString = _providerHelper.GetConnectionString(mdbPath);
                using var connection = new OleDbConnection(connectionString);
                connection.Open();

                // 获取所有用户表（排除系统表）
                var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables,
                    new object[] { null, null, null, "TABLE" });

                if (schema != null)
                {
                    foreach (DataRow row in schema.Rows)
                    {
                        var tableName = row["TABLE_NAME"].ToString();
                        if (!string.IsNullOrEmpty(tableName))
                            tables.Add(tableName);
                    }
                    _logger.LogInformation($"从 {Path.GetFileName(mdbPath)} 获取到 {tables.Count} 个表");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"获取MDB表失败: {mdbPath}");
            }

            return tables;
        }

        // 从MDB导入数据到SQL Server
        public async Task<int> ImportDataFromMDBAsync(string mdbPath, string tableName,
            string sqlServerTableName, string computerName)
        {
            var recordsImported = 0;

            try
            {
                var connectionString = _providerHelper.GetConnectionString(mdbPath);
                using var mdbConnection = new OleDbConnection(connectionString);
                await mdbConnection.OpenAsync();

                _logger.LogInformation($"开始导入表 {tableName} 从 {Path.GetFileName(mdbPath)}");

                // 从MDB读取数据
                var query = $"SELECT * FROM [{tableName}]";
                using var mdbCommand = new OleDbCommand(query, mdbConnection);

                // 使用 ExecuteReader 获取 DbDataReader
                using var reader = await mdbCommand.ExecuteReaderAsync();

                // 获取表结构
                var schemaTable = reader.GetSchemaTable();
                if (schemaTable == null || schemaTable.Rows.Count == 0)
                {
                    throw new Exception($"表 {tableName} 没有数据或不存在");
                }

                // 创建SQL Server表（如果不存在）
                await CreateTableInSqlServerAsync(schemaTable, sqlServerTableName);

                // 批量插入数据 - 修复：传入 DbDataReader
                recordsImported = await BulkInsertDataAsync(reader, sqlServerTableName);

                // 记录导入历史
                await _sqlServerService.LogImportHistoryAsync(computerName, sqlServerTableName,
                    recordsImported, "Success");

                _logger.LogInformation($"成功导入 {recordsImported} 条记录到表 {sqlServerTableName}");
                return recordsImported;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"导入失败: {mdbPath} -> {tableName}");
                await _sqlServerService.LogImportHistoryAsync(computerName, sqlServerTableName,
                    0, "Failed", ex.Message);
                return 0;
            }
        }

        // 在SQL Server中创建表
        public  async Task CreateTableInSqlServerAsync(DataTable schemaTable, string tableName)
        {
            try
            {
                var columns = new List<string>();

                foreach (DataRow row in schemaTable.Rows)
                {
                    var columnName = row["ColumnName"].ToString();
                    var dataType = (Type)row["DataType"];
                    var sqlType = ConvertToSqlType(dataType);

                    columns.Add($"[{columnName}] {sqlType}");
                }

                var createTableSql = $@"
                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{tableName}' AND xtype='U')
                    CREATE TABLE [{tableName}] (
                        {string.Join(",\n    ", columns)}
                    )";

                await _sqlServerService.ExecuteNonQueryAsync(createTableSql);
                _logger.LogDebug($"创建表 {tableName}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"创建表失败: {tableName}");
                throw;
            }
        }

        // 批量插入数据 - 使用 DbDataReader 参数
        private async Task<int> BulkInsertDataAsync(DbDataReader reader, string tableName)
        {
            var recordsImported = 0;
            var sqlConnectionString = _sqlServerService.GetConnectionString();

            if (string.IsNullOrEmpty(sqlConnectionString))
            {
                _logger.LogError("SQL Server连接字符串为空");
                return 0;
            }

            using var sqlConnection = new SqlConnection(sqlConnectionString);
            await sqlConnection.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(sqlConnection)
            {
                DestinationTableName = tableName,
                BatchSize = 1000,
                BulkCopyTimeout = 300
            };

            // 映射列 - 从reader获取列信息
            var schemaTable = reader.GetSchemaTable();
            if (schemaTable != null)
            {
                foreach (DataRow row in schemaTable.Rows)
                {
                    var columnName = row["ColumnName"].ToString();
                    bulkCopy.ColumnMappings.Add(columnName, columnName);
                }
            }

            try
            {
                // 使用 WriteToServerAsync 处理 DbDataReader
                await bulkCopy.WriteToServerAsync(reader);
                recordsImported = bulkCopy.RowsCopied;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"批量插入失败: {tableName}");
                throw;
            }

            return recordsImported;
        }

        // 转换数据类型
        private string ConvertToSqlType(Type dataType)
        {
            if (dataType == typeof(string)) return "NVARCHAR(MAX)";
            if (dataType == typeof(int)) return "INT";
            if (dataType == typeof(long)) return "BIGINT";
            if (dataType == typeof(decimal)) return "DECIMAL(18,2)";
            if (dataType == typeof(DateTime)) return "DATETIME";
            if (dataType == typeof(bool)) return "BIT";
            if (dataType == typeof(double)) return "FLOAT";
            if (dataType == typeof(float)) return "REAL";
            if (dataType == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (dataType == typeof(byte[])) return "VARBINARY(MAX)";
            if (dataType == typeof(byte)) return "TINYINT";
            if (dataType == typeof(short)) return "SMALLINT";

            return "NVARCHAR(MAX)";
        }

        // 获取表的列信息
        public async Task<List<ColumnInfo>> GetTableColumnsAsync(string mdbPath, string tableName)
        {
            var columns = new List<ColumnInfo>();

            if (!File.Exists(mdbPath))
                return columns;

            try
            {
                var connectionString = _providerHelper.GetConnectionString(mdbPath);
                using var connection = new OleDbConnection(connectionString);
                await connection.OpenAsync();

                // 获取表的结构信息
                var schema = await connection.GetSchemaAsync("Columns", new string[] { null, null, tableName });

                foreach (DataRow row in schema.Rows)
                {
                    var column = new ColumnInfo
                    {
                        ColumnName = row["COLUMN_NAME"].ToString(),
                        DataType = row["DATA_TYPE"].ToString(),
                        IsNullable = row["IS_NULLABLE"].ToString() == "YES"
                    };

                    if (row["CHARACTER_MAXIMUM_LENGTH"] != DBNull.Value)
                        column.MaxLength = Convert.ToInt32(row["CHARACTER_MAXIMUM_LENGTH"]);

                    columns.Add(column);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"获取表列信息失败: {tableName}");
            }

            return columns;
        }

        // 获取MDB文件的基本信息
        public async Task<MDBFileInfo> GetMDBFileInfoAsync(string mdbPath)
        {
            var info = new MDBFileInfo
            {
                FilePath = mdbPath,
                FileName = Path.GetFileName(mdbPath),
                FileSize = new FileInfo(mdbPath).Length,
                LastModified = File.GetLastWriteTime(mdbPath)
            };

            try
            {
                var connectionString = _providerHelper.GetConnectionString(mdbPath);
                using var connection = new OleDbConnection(connectionString);
                await connection.OpenAsync();

                // 获取表数量
                var tables = GetTablesFromMDB(mdbPath);
                info.TableCount = tables.Count;

                // 获取总记录数
                foreach (var table in tables)
                {
                    try
                    {
                        var countQuery = $"SELECT COUNT(*) FROM [{table}]";
                        using var command = new OleDbCommand(countQuery, connection);
                        var count = Convert.ToInt32(await command.ExecuteScalarAsync());
                        info.TotalRecords += count;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, $"统计表 {table} 记录数失败");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"获取MDB文件信息失败: {mdbPath}");
            }

            return info;
        }

        // 从MDB文件读取数据到DataTable
        public async Task<DataTable> ReadMDBTableToDataTableAsync(string mdbPath, string tableName)
        {
            var dataTable = new DataTable();

            if (!File.Exists(mdbPath))
                return dataTable;

            try
            {
                var connectionString = _providerHelper.GetConnectionString(mdbPath);
                using var connection = new OleDbConnection(connectionString);
                await connection.OpenAsync();

                var query = $"SELECT * FROM [{tableName}]";
                using var adapter = new OleDbDataAdapter(query, connection);
                adapter.Fill(dataTable);

                _logger.LogInformation($"从 {tableName} 读取了 {dataTable.Rows.Count} 行数据");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"读取表数据失败: {tableName}");
            }

            return dataTable;
        }
    }

    // 辅助类
    public class ColumnInfo
    {
        public string ColumnName { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public bool IsNullable { get; set; }
        public int? MaxLength { get; set; }
    }

    public class MDBFileInfo
    {
        public string FilePath { get; set; } = string.Empty;
        public string FileName { get; set; } = string.Empty;
        public long FileSize { get; set; }
        public DateTime LastModified { get; set; }
        public int TableCount { get; set; }
        public long TotalRecords { get; set; }
    }
}