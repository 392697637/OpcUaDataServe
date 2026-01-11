// Services/MDBService.cs
using MDBImporter.Core;
using MDBImporter.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MDBImporter.Services
{
    public class MDBService
    {
        private readonly SqlServerService _sqlServerService;
        private readonly ProviderHelper _providerHelper;
        private readonly ILogger<MDBService> _logger;
        private BulkCopySettings _bulkCopySettings = new BulkCopySettings();

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
        public async Task<int> ImportDataFromMDBAsync(string mdbPath, string tableName, string sqlServerTableName, string computerName)
        {
            var recordsImported = 0;
            var history = new ImportHistory();
            history.ComputerName = computerName;
            history.TableName = sqlServerTableName;
            history.PrimaryKey = string.Empty;
            history.PrimaryKeyData = string.Empty;
            history.ImportTime = DateTime.Now;
            history.ErrorMessage = string.Empty;
            history.FileName = Path.GetFileName(mdbPath);
            history.Remark = string.Empty;
            history.ImportDuration = string.Empty;
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

                // 获取MDB表的主键信息
                var primaryKeys = await GetMDBPrimaryKeysAsync(mdbConnection, tableName);
                history.PrimaryKey = string.Join(",", primaryKeys);

                // 创建SQL Server表（如果不存在），包含主键
                await CreateTableInSqlServerAsync(schemaTable, sqlServerTableName, primaryKeys);
                history.PrimaryKeyData = string.Empty;
                // 批量插入数据
                recordsImported = await BulkInsertDataAsync(reader, sqlServerTableName);

                var row = await GetLastInsertedRecordAsync( sqlServerTableName, history.PrimaryKey);
                if (row != null)
                {
                    List<string> list = new List<string>();
                    foreach (var item in primaryKeys)
                    {
                        list.Add(row["DID"].ToString());
                    }
                    history.PrimaryKeyData = string.Join(",", list);
                }
               
                history.RecordsImported = recordsImported;
                history.Status = "Success";
                // 记录导入历史
                await _sqlServerService.LogImportHistoryAsync(history);

                _logger.LogInformation($"成功导入 {recordsImported} 条记录到表 {sqlServerTableName}");
                return recordsImported;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"导入失败: {mdbPath} -> {tableName}");
                history.RecordsImported = 0;
                history.Status = "Failed";
                history.ErrorMessage = ex.Message;
                await _sqlServerService.LogImportHistoryAsync(history);
                return 0;
            }
        }
        // 获取MDB表的主键信息（修复版）
        private async Task<List<string>> GetMDBPrimaryKeysAsync(OleDbConnection connection, string tableName)
        {
            var primaryKeys = new List<string>();

            try
            {
                // 方法1: 使用 OleDbConnection.GetSchema 的特定方法获取主键
                DataTable primaryKeyInfo = null;

                try
                {
                    // 尝试使用不同的方法获取主键信息
                    primaryKeyInfo = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, new object[] { null, null, tableName });
                }
                catch (Exception)
                {
                    // 如果上面的方法失败，尝试其他方法
                }

                if (primaryKeyInfo != null && primaryKeyInfo.Rows.Count > 0)
                {
                    // 方法1成功: 使用 OleDbSchemaGuid.Primary_Keys
                    foreach (DataRow row in primaryKeyInfo.Rows)
                    {
                        var columnName = row["COLUMN_NAME"].ToString();
                        if (!string.IsNullOrEmpty(columnName))
                        {
                            primaryKeys.Add(columnName);
                        }
                    }
                }
                else
                {
                    // 方法2: 使用索引信息推断主键
                    try
                    {
                        var indexInfo = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Indexes, new object[] { null, null, null, null, tableName });

                        if (indexInfo != null)
                        {
                            var primaryIndexRows = indexInfo.AsEnumerable()
                                .Where(row => row.Field<bool>("PRIMARY_KEY") == true)
                                .OrderBy(row => row.Field<short>("ORDINAL_POSITION"));

                            foreach (var row in primaryIndexRows)
                            {
                                var columnName = row["COLUMN_NAME"].ToString();
                                if (!string.IsNullOrEmpty(columnName))
                                {
                                    primaryKeys.Add(columnName);
                                }
                            }
                        }
                    }
                    catch (Exception)
                    {
                        // 继续尝试其他方法
                    }
                }

                // 如果上述方法都失败，尝试使用 ADOX COM 对象（需要引用 Microsoft ADO Ext）
                if (primaryKeys.Count == 0)
                {
                    primaryKeys = await GetPrimaryKeysUsingAdoxAsync(connection.DataSource, tableName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"获取表 {tableName} 的主键信息失败: {ex.Message}");
            }

            return primaryKeys;
        }

        // 使用 ADOX COM 对象获取主键（更可靠的方法）
        private async Task<List<string>> GetPrimaryKeysUsingAdoxAsync(string mdbPath, string tableName)
        {
            var primaryKeys = new List<string>();

            try
            {
                // 注意: 需要在项目中引用 Microsoft ADO Ext. 2.8 for DDL and Security
                // 或者使用动态调用

                await Task.Run(() =>
                {
                    dynamic catalog = null;
                    dynamic table = null;

                    try
                    {
                        // 创建 ADOX Catalog 对象
                        Type catalogType = Type.GetTypeFromProgID("ADOX.Catalog");
                        if (catalogType != null)
                        {
                            catalog = Activator.CreateInstance(catalogType);

                            // 打开 Access 数据库
                            string connectionString = $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={mdbPath};";
                            catalog.ActiveConnection = connectionString;

                            // 查找指定的表
                            foreach (dynamic tbl in catalog.Tables)
                            {
                                if (tbl.Type == "TABLE" && tbl.Name == tableName)
                                {
                                    table = tbl;
                                    break;
                                }
                            }

                            // 获取主键
                            if (table != null && table.Indexes.Count > 0)
                            {
                                foreach (dynamic index in table.Indexes)
                                {
                                    if (index.PrimaryKey == true)
                                    {
                                        foreach (dynamic column in index.Columns)
                                        {
                                            primaryKeys.Add(column.Name);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    finally
                    {
                        // 清理 COM 对象
                        if (catalog != null)
                        {
                            System.Runtime.InteropServices.Marshal.ReleaseComObject(catalog);
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"使用 ADOX 获取主键失败: {ex.Message}");
            }

            return primaryKeys;
        }

        // 备选方案：使用更简单直接的方法
        private async Task<List<string>> GetPrimaryKeysSimpleAsync(OleDbConnection connection, string tableName)
        {
            var primaryKeys = new List<string>();

            try
            {
                // 方法1: 直接查询系统表（适用于 Jet OLE DB 4.0）
                try
                {
                    string query = @"
                SELECT i.name AS IndexName, c.name AS ColumnName
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.is_primary_key = 1
                  AND OBJECT_NAME(i.object_id) = @TableName
                ORDER BY ic.key_ordinal";

                    using var command = new OleDbCommand(query, connection);
                    command.Parameters.AddWithValue("@TableName", tableName);

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        var columnName = reader["ColumnName"].ToString();
                        if (!string.IsNullOrEmpty(columnName))
                        {
                            primaryKeys.Add(columnName);
                        }
                    }
                }
                catch
                {
                    // 如果上面的查询失败，尝试 Access 特定的系统表查询
                    try
                    {
                        // 注意: Access 可能需要特殊权限才能访问 MSysObjects
                        string query = @"
                    SELECT MSysObjects.Name AS TableName, MSysColumns.Name AS ColumnName
                    FROM MSysObjects 
                    INNER JOIN MSysColumns ON MSysObjects.Id = MSysColumns.Id
                    WHERE MSysObjects.Name = ?
                      AND MSysObjects.Type = 1
                      AND MSysColumns.ColumnRequired = True";

                        using var command = new OleDbCommand(query, connection);
                        command.Parameters.AddWithValue("@p1", tableName);

                        using var reader = await command.ExecuteReaderAsync();
                        while (await reader.ReadAsync())
                        {
                            var columnName = reader["ColumnName"].ToString();
                            if (!string.IsNullOrEmpty(columnName))
                            {
                                primaryKeys.Add(columnName);
                            }
                        }
                    }
                    catch (Exception sysEx)
                    {
                        _logger.LogWarning($"查询系统表失败: {sysEx.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"简单方法获取主键失败: {ex.Message}");
            }

            return primaryKeys;
        }


        private async Task CreateTableInSqlServerAsync(DataTable schemaTable, string tableName, List<string> primaryKeys)
        {
            var columns = new List<string>();
            var primaryKeyColumns = new List<string>();

            foreach (DataRow row in schemaTable.Rows)
            {
                var columnName = row["ColumnName"].ToString();
                var dataType = row["DataType"].ToString();
                var maxLength = Convert.ToInt32(row["ColumnSize"]);
                var isNullable = Convert.ToBoolean(row["AllowDBNull"]);
                var isIdentity = false;

                // 检查是否为自动编号（IDENTITY）字段
                if (row.Table.Columns.Contains("IsAutoIncrement"))
                {
                    isIdentity = Convert.ToBoolean(row["IsAutoIncrement"]);
                }

                // 检查当前列是否在主键列表中
                bool isPrimaryKey = primaryKeys.Contains(columnName, StringComparer.OrdinalIgnoreCase);

                // 如果是主键列，必须设置为 NOT NULL
                if (isPrimaryKey)
                {
                    isNullable = false;  // 强制设为 NOT NULL
                    primaryKeyColumns.Add(columnName);
                }

                // 映射Access数据类型到SQL Server数据类型
                string sqlServerType;
                switch (dataType)
                {
                    case "System.String":
                        sqlServerType = maxLength <= 8000 ? $"NVARCHAR({(maxLength > 0 ? maxLength : "MAX")})" : "NTEXT";
                        break;
                    case "System.Int32":
                        sqlServerType = "INT";
                        if (isIdentity) sqlServerType += " IDENTITY(1,1)";
                        break;
                    case "System.Int64":
                        sqlServerType = "BIGINT";
                        if (isIdentity) sqlServerType += " IDENTITY(1,1)";
                        break;
                    case "System.Decimal":
                        sqlServerType = "DECIMAL(18, 2)";
                        break;
                    case "System.Double":
                    case "System.Single":
                        sqlServerType = "FLOAT";
                        break;
                    case "System.DateTime":
                        sqlServerType = "DATETIME";
                        break;
                    case "System.Boolean":
                        sqlServerType = "BIT";
                        break;
                    case "System.Byte[]":
                        sqlServerType = "VARBINARY(MAX)";
                        break;
                    case "System.Guid":
                        sqlServerType = "UNIQUEIDENTIFIER";
                        break;
                    default:
                        sqlServerType = "NVARCHAR(MAX)";
                        break;
                }

                var nullClause = isNullable ? "NULL" : "NOT NULL";
                columns.Add($"[{columnName}] {sqlServerType} {nullClause}");
            }

            // 构建CREATE TABLE语句
            var createTableSql = new StringBuilder();

            // 先检查表是否存在
            createTableSql.AppendLine($"IF OBJECT_ID('{tableName}', 'U') IS NULL");
            createTableSql.AppendLine("BEGIN");
            createTableSql.AppendLine($"    CREATE TABLE [{tableName}] (");
            createTableSql.AppendLine($"        {string.Join(",\n        ", columns)}");

            // 如果有主键，添加主键约束
            if (primaryKeyColumns.Count > 0)
            {
                var pkColumns = string.Join(", ", primaryKeyColumns.Select(pk => $"[{pk}]"));
                createTableSql.AppendLine($"        ,CONSTRAINT [PK_{tableName}] PRIMARY KEY ({pkColumns})");

                _logger.LogInformation($"表 {tableName} 将创建主键: {string.Join(", ", primaryKeyColumns)}");
            }
            else if (primaryKeys.Count > 0)
            {
                // 过滤掉可能不存在的列
                var existingColumns = schemaTable.AsEnumerable()
                    .Select(row => row["ColumnName"].ToString())
                    .ToList();

                var validPrimaryKeys = primaryKeys
                    .Where(pk => existingColumns.Contains(pk, StringComparer.OrdinalIgnoreCase))
                    .ToList();

                if (validPrimaryKeys.Count > 0)
                {
                    // 记录警告：这些列是主键但允许NULL，可能需要手动修复
                    _logger.LogWarning($"表 {tableName} 的主键列允许NULL值：{string.Join(", ", validPrimaryKeys)}");
                    _logger.LogWarning("您可能需要手动将这些列修改为NOT NULL并添加主键约束");
                }
            }

            createTableSql.AppendLine("    )");
            createTableSql.AppendLine($" PRINT '表 {tableName} 创建成功'");
            createTableSql.AppendLine("END");
            createTableSql.AppendLine("ELSE");
            createTableSql.AppendLine("BEGIN");
            createTableSql.AppendLine($" PRINT '表 {tableName} 已存在，跳过创建'");
            createTableSql.AppendLine("END");

            _logger.LogDebug($"创建表SQL:\n{createTableSql}");

            using var connection = new SqlConnection(_sqlServerService.GetConnectionString());
            await connection.OpenAsync();

            using var command = new SqlCommand(createTableSql.ToString(), connection);
            await command.ExecuteNonQueryAsync();

            _logger.LogInformation($"已处理表 {tableName}");
        }

        // 创建SQL Server表（包含主键）

        // 备用方法：使用更直接的方式获取主键（如果上述方法不工作）
        private async Task<List<string>> GetMDBPrimaryKeysDirectAsync(OleDbConnection connection, string tableName)
        {
            var primaryKeys = new List<string>();

            try
            {
                // 使用MSysObjects系统表（仅适用于Access数据库）
                var query = @"
            SELECT MSysObjects.Name AS TableName, MSysObjects.Type,
                   MSysColumns.Name AS ColumnName
            FROM MSysObjects 
            INNER JOIN MSysColumns ON MSysObjects.Id = MSysColumns.Id
            WHERE MSysObjects.Name = @TableName 
              AND MSysObjects.Type = 1
              AND MSysColumns.ColumnRequired = True
            ORDER BY MSysColumns.ColumnId";

                using var command = new OleDbCommand(query, connection);
                command.Parameters.AddWithValue("@TableName", tableName);

                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var columnName = reader["ColumnName"].ToString();
                    if (!string.IsNullOrEmpty(columnName))
                    {
                        primaryKeys.Add(columnName);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"直接获取主键失败: {ex.Message}");
            }

            return primaryKeys;
        }
        // 在SQL Server中创建表
        public async Task CreateTableInSqlServerAsync(DataTable schemaTable, string tableName)
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
            // 使用事务确保数据一致性
            using var transaction = await sqlConnection.BeginTransactionAsync();
            try
            {
                using var bulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, (SqlTransaction)transaction)
                {
                    DestinationTableName = tableName,
                    BatchSize = _bulkCopySettings.BatchSize, // ← 批次大小  
                    BulkCopyTimeout = _bulkCopySettings.TimeoutSeconds,  // ← 超时时间（秒）
                    EnableStreaming = _bulkCopySettings.EnableStreaming, // ← 启用流式传输
                    NotifyAfter = _bulkCopySettings.NotifyAfter // ← 每插入多少行触发一次事件
                };
                // 映射列
                var schemaTable = reader.GetSchemaTable();
                if (schemaTable != null)
                {
                    foreach (DataRow row in schemaTable.Rows)
                    {
                        var columnName = row["ColumnName"].ToString();
                        bulkCopy.ColumnMappings.Add(columnName, columnName);
                    }
                }
                bulkCopy.SqlRowsCopied += (sender, e) =>
                {
                    _logger.LogInformation($"已导入 {e.RowsCopied} 条记录");
                };
                await bulkCopy.WriteToServerAsync(reader);
                recordsImported = (int)bulkCopy.RowsCopied;
                await transaction.CommitAsync();
                _logger.LogInformation($"批量插入完成: {tableName}, 共导入 {recordsImported} 条记录");
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.LogError(ex, $"批量插入失败: {tableName}");
                throw new InvalidOperationException($"批量插入失败: {tableName}", ex);
            }
            finally
            {
                if (!reader.IsClosed) // 确保reader关闭
                    reader.Close();
            }
            return recordsImported;
        }



        // 获取最后一次批量插入的数据记录（根据主键）
        private async Task<DataRow?> GetLastInsertedRecordAsync(string tableName, string primaryKeyColumn)
        {
            var sqlConnectionString = _sqlServerService.GetConnectionString();

            if (string.IsNullOrEmpty(sqlConnectionString))
            {
                _logger.LogError("SQL Server连接字符串为空");
                return null;
            }

            using var sqlConnection = new SqlConnection(sqlConnectionString);
            await sqlConnection.OpenAsync();

            try
            {
                // 使用查询获取最后一条记录（假设主键是自增或时间戳等可排序的）
                var query = $"SELECT TOP 1 * FROM {tableName} ORDER BY {primaryKeyColumn} DESC";

                //using var command = new SqlCommand(query, sqlConnection);
                //using var reader = await command.ExecuteReaderAsync();

                //if (await reader.ReadAsync())
                //{
                //    var dataTable = new DataTable();
                //    dataTable.Load(reader);
                //    return dataTable.Rows[0];
                //}

                using var command = new SqlCommand(query, sqlConnection);
                using var adapter = new SqlDataAdapter(command);

                var dataTable = new DataTable();
                adapter.Fill(dataTable);

                if (dataTable.Rows.Count > 0)
                {
                    return dataTable.Rows[0];
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"获取最后插入记录失败: {tableName}");
                throw new InvalidOperationException($"获取最后插入记录失败: {tableName}", ex);
            }

            return null;
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

        internal void SetBulkCopySettings(BulkCopySettings bulkCopySettings)
        {
            _bulkCopySettings = bulkCopySettings;
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