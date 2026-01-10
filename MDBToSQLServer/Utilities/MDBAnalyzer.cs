using System.Data;
using System.Data.OleDb;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MDBToSQLServer.Core;

namespace MDBToSQLServer.Utilities
{
    public class MDBAnalyzer
    {
        private readonly ProviderHelper _providerHelper;
        private readonly ILogger<MDBAnalyzer> _logger;

        public MDBAnalyzer(ProviderHelper providerHelper, ILogger<MDBAnalyzer> logger)
        {
            _providerHelper = providerHelper;
            _logger = logger;
        }

        public class TableInfo
        {
            public string TableName { get; set; } = string.Empty;
            public int RowCount { get; set; }
            public List<ColumnInfo> Columns { get; set; } = new();
            public DateTime CreatedDate { get; set; }
            public DateTime ModifiedDate { get; set; }
            public string? Description { get; set; }
        }

        public class ColumnInfo
        {
            public string ColumnName { get; set; } = string.Empty;
            public string DataType { get; set; } = string.Empty;
            public int MaxLength { get; set; }
            public bool IsNullable { get; set; }
            public bool IsPrimaryKey { get; set; }
            public bool IsIdentity { get; set; }
            public string? DefaultValue { get; set; }
            public string? Description { get; set; }
        }

        public class MDBFileInfo
        {
            public string FileName { get; set; } = string.Empty;
            public string FilePath { get; set; } = string.Empty;
            public long FileSize { get; set; }
            public DateTime CreatedDate { get; set; }
            public DateTime ModifiedDate { get; set; }
            public DateTime AccessedDate { get; set; }
            public string? FileVersion { get; set; }
            public string? AccessVersion { get; set; }
            public int TableCount { get; set; }
            public int TotalRows { get; set; }
            public List<TableInfo> Tables { get; set; } = new();
            public bool IsValid { get; set; }
            public string? ErrorMessage { get; set; }
        }

        public MDBFileInfo AnalyzeMDB(string filePath)
        {
            var fileInfo = new MDBFileInfo
            {
                FileName = Path.GetFileName(filePath),
                FilePath = filePath
            };

            try
            {
                // 获取文件基本信息
                var fileInfoDetails = new FileInfo(filePath);
                fileInfo.FileSize = fileInfoDetails.Length;
                fileInfo.CreatedDate = fileInfoDetails.CreationTime;
                fileInfo.ModifiedDate = fileInfoDetails.LastWriteTime;
                fileInfo.AccessedDate = fileInfoDetails.LastAccessTime;

                // 连接到MDB文件
                string connStr = _providerHelper.GetConnectionString(filePath);

                using (OleDbConnection conn = new OleDbConnection(connStr))
                {
                    conn.Open();

                    // 获取数据库版本信息
                    fileInfo.AccessVersion = GetAccessVersion(conn);

                    // 获取所有表
                    fileInfo.Tables = GetTables(conn);
                    fileInfo.TableCount = fileInfo.Tables.Count;
                    fileInfo.TotalRows = fileInfo.Tables.Sum(t => t.RowCount);

                    fileInfo.IsValid = true;

                    _logger.LogInformation("MDB文件分析完成: {FileName} ({TableCount}个表, {TotalRows}行)",
                        fileInfo.FileName, fileInfo.TableCount, fileInfo.TotalRows);
                }
            }
            catch (Exception ex)
            {
                fileInfo.IsValid = false;
                fileInfo.ErrorMessage = ex.Message;

                _logger.LogError(ex, "分析MDB文件失败: {FilePath}", filePath);
            }

            return fileInfo;
        }

        private string? GetAccessVersion(OleDbConnection conn)
        {
            try
            {
                // 尝试获取版本信息
                string sql = "SELECT @@VERSION";
                using (var cmd = new OleDbCommand(sql, conn))
                {
                    var result = cmd.ExecuteScalar();
                    return result?.ToString();
                }
            }
            catch
            {
                // 通过连接属性判断
                string provider = conn.Provider;
                if (provider.Contains("12.0"))
                    return "Access 2007 or later";
                else if (provider.Contains("4.0"))
                    return "Access 2003 or earlier";
                else
                    return "Unknown";
            }
        }

        private List<TableInfo> GetTables(OleDbConnection conn)
        {
            var tables = new List<TableInfo>();

            try
            {
                // 获取所有表
                DataTable schemaTables = conn.GetSchema("Tables");

                foreach (DataRow row in schemaTables.Rows)
                {
                    string tableName = row["TABLE_NAME"].ToString() ?? string.Empty;
                    string tableType = row["TABLE_TYPE"].ToString() ?? string.Empty;

                    // 只处理普通表，跳过系统表
                    if (tableType == "TABLE" && !tableName.StartsWith("MSys"))
                    {
                        var tableInfo = new TableInfo
                        {
                            TableName = tableName
                        };

                        try
                        {
                            // 获取表信息
                            tableInfo.Columns = GetColumns(conn, tableName);
                            tableInfo.RowCount = GetRowCount(conn, tableName);

                            // 尝试获取表描述
                            tableInfo.Description = GetTableDescription(conn, tableName);

                            tables.Add(tableInfo);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "分析表失败: {TableName}", tableName);
                            // 继续处理其他表
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取表列表失败");
            }

            return tables;
        }

        private List<ColumnInfo> GetColumns(OleDbConnection conn, string tableName)
        {
            var columns = new List<ColumnInfo>();

            try
            {
                // 获取列信息
                var restrictions = new string[4];
                restrictions[2] = tableName;
                restrictions[3] = "TABLE";

                DataTable schemaColumns = conn.GetSchema("Columns", restrictions);

                foreach (DataRow row in schemaColumns.Rows)
                {
                    var columnInfo = new ColumnInfo
                    {
                        ColumnName = row["COLUMN_NAME"].ToString() ?? string.Empty,
                        DataType = row["DATA_TYPE"].ToString() ?? string.Empty,
                        MaxLength = Convert.ToInt32(row["CHARACTER_MAXIMUM_LENGTH"]),
                        IsNullable = row["IS_NULLABLE"].ToString() == "YES"
                    };

                    // 检查是否为标识列
                    columnInfo.IsIdentity = IsIdentityColumn(conn, tableName, columnInfo.ColumnName);

                    // 获取默认值
                    columnInfo.DefaultValue = GetColumnDefaultValue(conn, tableName, columnInfo.ColumnName);

                    columns.Add(columnInfo);
                }

                // 检查主键
                var primaryKeys = GetPrimaryKeys(conn, tableName);
                foreach (var column in columns)
                {
                    column.IsPrimaryKey = primaryKeys.Contains(column.ColumnName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取列信息失败: {TableName}", tableName);
            }

            return columns;
        }

        private int GetRowCount(OleDbConnection conn, string tableName)
        {
            try
            {
                string sql = $"SELECT COUNT(*) FROM [{tableName}]";
                using (var cmd = new OleDbCommand(sql, conn))
                {
                    return Convert.ToInt32(cmd.ExecuteScalar());
                }
            }
            catch
            {
                return 0;
            }
        }

        private bool IsIdentityColumn(OleDbConnection conn, string tableName, string columnName)
        {
            try
            {
                // 查询标识列
                string sql = $"SELECT TOP 1 [{columnName}] FROM [{tableName}] WHERE 1=0";
                using (var cmd = new OleDbCommand(sql, conn))
                using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    var schemaTable = reader.GetSchemaTable();
                    if (schemaTable != null)
                    {
                        foreach (DataRow row in schemaTable.Rows)
                        {
                            if (row["ColumnName"].ToString() == columnName)
                            {
                                return row["IsIdentity"] != DBNull.Value && (bool)row["IsIdentity"];
                            }
                        }
                    }
                }

                return false;
            }
            catch
            {
                return false;
            }
        }

        private string? GetColumnDefaultValue(OleDbConnection conn, string tableName, string columnName)
        {
            try
            {
                // 这个查询可能不适用于所有MDB文件
                string sql = @"
                    SELECT COLUMN_DEFAULT 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = @TableName AND COLUMN_NAME = @ColumnName";

                using (var cmd = new OleDbCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@TableName", tableName);
                    cmd.Parameters.AddWithValue("@ColumnName", columnName);

                    var result = cmd.ExecuteScalar();
                    return result?.ToString();
                }
            }
            catch
            {
                return null;
            }
        }

        private HashSet<string> GetPrimaryKeys(OleDbConnection conn, string tableName)
        {
            var primaryKeys = new HashSet<string>();

            try
            {
                var restrictions = new string[4];
                restrictions[2] = tableName;
                restrictions[3] = "TABLE";

                DataTable schemaIndexes = conn.GetSchema("Indexes", restrictions);

                foreach (DataRow row in schemaIndexes.Rows)
                {
                    string indexName = row["INDEX_NAME"].ToString() ?? string.Empty;
                    string columnName = row["COLUMN_NAME"].ToString() ?? string.Empty;
                    bool isPrimary = row["PRIMARY_KEY"] != DBNull.Value && (bool)row["PRIMARY_KEY"];

                    if (isPrimary && !string.IsNullOrEmpty(columnName))
                    {
                        primaryKeys.Add(columnName);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取主键失败: {TableName}", tableName);
            }

            return primaryKeys;
        }

        private string? GetTableDescription(OleDbConnection conn, string tableName)
        {
            try
            {
                // 尝试获取表描述（不一定所有MDB都有）
                string sql = $"SELECT Description FROM MSysObjects WHERE Name='{tableName}' AND Type=1";
                using (var cmd = new OleDbCommand(sql, conn))
                {
                    var result = cmd.ExecuteScalar();
                    return result?.ToString();
                }
            }
            catch
            {
                return null;
            }
        }

        public void GenerateCreateTableScripts(string filePath, string outputFolder)
        {
            var fileInfo = AnalyzeMDB(filePath);

            if (!fileInfo.IsValid)
            {
                _logger.LogError("无法生成脚本，文件无效: {FilePath}", filePath);
                return;
            }

            Directory.CreateDirectory(outputFolder);

            string baseFileName = Path.GetFileNameWithoutExtension(fileInfo.FileName);
            string scriptFile = Path.Combine(outputFolder, $"{baseFileName}_CreateTables.sql");

            var script = new System.Text.StringBuilder();

            // 文件头
            script.AppendLine("-- ============================================");
            script.AppendLine($"-- 自动生成的SQL创建脚本");
            script.AppendLine($"-- 源文件: {fileInfo.FileName}");
            script.AppendLine($"-- 生成时间: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            script.AppendLine($"-- 表数量: {fileInfo.TableCount}");
            script.AppendLine($"-- 总行数: {fileInfo.TotalRows}");
            script.AppendLine("-- ============================================");
            script.AppendLine();
            script.AppendLine("USE [YourDatabaseName] -- 请修改为你的数据库名");
            script.AppendLine("GO");
            script.AppendLine();

            // 为每个表生成创建脚本
            foreach (var table in fileInfo.Tables)
            {
                script.AppendLine($"-- 表: {table.TableName}");
                script.AppendLine($"-- 行数: {table.RowCount}");
                if (!string.IsNullOrEmpty(table.Description))
                {
                    script.AppendLine($"-- 描述: {table.Description}");
                }
                script.AppendLine($"CREATE TABLE [dbo].[{table.TableName}] (");

                // 列定义
                var columnDefinitions = new List<string>();
                foreach (var column in table.Columns)
                {
                    string columnDef = $"    [{column.ColumnName}] {GetSqlDataType(column)}";

                    if (column.IsIdentity)
                    {
                        columnDef += " IDENTITY(1,1)";
                    }

                    columnDef += column.IsNullable ? " NULL" : " NOT NULL";

                    if (!string.IsNullOrEmpty(column.DefaultValue))
                    {
                        columnDef += $" DEFAULT {column.DefaultValue}";
                    }

                    columnDefinitions.Add(columnDef);
                }

                script.AppendLine(string.Join(",\r\n", columnDefinitions));
                script.AppendLine(")");
                script.AppendLine("GO");
                script.AppendLine();

                // 主键约束
                var primaryKeyColumns = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (primaryKeyColumns.Count > 0)
                {
                    string pkColumns = string.Join(", ", primaryKeyColumns.Select(c => $"[{c.ColumnName}]"));
                    script.AppendLine($"ALTER TABLE [dbo].[{table.TableName}]");
                    script.AppendLine($"ADD CONSTRAINT [PK_{table.TableName}] PRIMARY KEY ({pkColumns})");
                    script.AppendLine("GO");
                    script.AppendLine();
                }

                // 索引
                foreach (var column in table.Columns)
                {
                    if (ShouldCreateIndex(column))
                    {
                        script.AppendLine($"CREATE NONCLUSTERED INDEX [IX_{table.TableName}_{column.ColumnName}]");
                        script.AppendLine($"ON [dbo].[{table.TableName}] ([{column.ColumnName}])");
                        script.AppendLine("GO");
                        script.AppendLine();
                    }
                }

                script.AppendLine("-- ============================================");
                script.AppendLine();
            }

            // 写入文件
            File.WriteAllText(scriptFile, script.ToString(), System.Text.Encoding.UTF8);

            _logger.LogInformation("SQL创建脚本已生成: {ScriptFile}", scriptFile);
        }

        private string GetSqlDataType(ColumnInfo column)
        {
            string dataType = column.DataType.ToUpper();

            // 根据Access数据类型映射到SQL Server数据类型
            switch (dataType)
            {
                case "VARCHAR":
                case "CHAR":
                case "TEXT":
                    if (column.MaxLength > 0 && column.MaxLength <= 8000)
                        return $"NVARCHAR({column.MaxLength})";
                    else
                        return "NVARCHAR(MAX)";

                case "INTEGER":
                case "INT":
                    return "INT";

                case "LONG":
                case "BIGINT":
                    return "BIGINT";

                case "SINGLE":
                case "FLOAT":
                    return "FLOAT";

                case "DOUBLE":
                    return "FLOAT";

                case "CURRENCY":
                case "DECIMAL":
                case "NUMERIC":
                    return "DECIMAL(18, 2)";

                case "DATETIME":
                case "DATE":
                case "TIME":
                    return "DATETIME";

                case "BOOLEAN":
                case "BIT":
                    return "BIT";

                case "BINARY":
                case "VARBINARY":
                case "IMAGE":
                    return "VARBINARY(MAX)";

                case "GUID":
                    return "UNIQUEIDENTIFIER";

                default:
                    return "NVARCHAR(MAX)";
            }
        }

        private bool ShouldCreateIndex(ColumnInfo column)
        {
            // 为某些类型的列创建索引
            string columnName = column.ColumnName.ToLower();

            if (column.IsPrimaryKey || column.IsIdentity)
                return false;

            // 外键列
            if (columnName.EndsWith("id") || columnName.EndsWith("_id"))
                return true;

            // 日期列
            if (columnName.Contains("date") || columnName.Contains("time"))
                return true;

            // 代码列
            if (columnName.Contains("code") || columnName.Contains("no"))
                return true;

            return false;
        }

        public void GenerateAnalysisReport(string filePath, string outputFolder)
        {
            var fileInfo = AnalyzeMDB(filePath);

            Directory.CreateDirectory(outputFolder);

            string baseFileName = Path.GetFileNameWithoutExtension(fileInfo.FileName);
            string reportFile = Path.Combine(outputFolder, $"{baseFileName}_AnalysisReport.txt");

            var report = new System.Text.StringBuilder();

            report.AppendLine("=== MDB文件分析报告 ===");
            report.AppendLine($"文件名: {fileInfo.FileName}");
            report.AppendLine($"文件路径: {fileInfo.FilePath}");
            report.AppendLine($"文件大小: {FormatFileSize(fileInfo.FileSize)}");
            report.AppendLine($"创建时间: {fileInfo.CreatedDate:yyyy-MM-dd HH:mm:ss}");
            report.AppendLine($"修改时间: {fileInfo.ModifiedDate:yyyy-MM-dd HH:mm:ss}");
            report.AppendLine($"Access版本: {fileInfo.AccessVersion}");
            report.AppendLine($"有效性: {(fileInfo.IsValid ? "有效" : "无效")}");

            if (!fileInfo.IsValid)
            {
                report.AppendLine($"错误信息: {fileInfo.ErrorMessage}");
            }
            else
            {
                report.AppendLine($"表数量: {fileInfo.TableCount}");
                report.AppendLine($"总行数: {fileInfo.TotalRows:N0}");
                report.AppendLine();
                report.AppendLine("=== 表详细信息 ===");

                foreach (var table in fileInfo.Tables)
                {
                    report.AppendLine($"\n表名: {table.TableName}");
                    report.AppendLine($"行数: {table.RowCount:N0}");

                    if (!string.IsNullOrEmpty(table.Description))
                    {
                        report.AppendLine($"描述: {table.Description}");
                    }

                    report.AppendLine("列结构:");
                    report.AppendLine("  列名               类型              长度   空值   主键   标识");
                    report.AppendLine("  ------------------ ---------------- ------ ------ ------ ------");

                    foreach (var column in table.Columns)
                    {
                        string dataType = GetSqlDataType(column);
                        string maxLength = column.MaxLength > 0 ? column.MaxLength.ToString() : "MAX";
                        string nullable = column.IsNullable ? "YES" : "NO";
                        string primaryKey = column.IsPrimaryKey ? "YES" : "NO";
                        string identity = column.IsIdentity ? "YES" : "NO";

                        report.AppendLine($"  {column.ColumnName,-18} {dataType,-16} {maxLength,6} {nullable,6} {primaryKey,6} {identity,6}");
                    }
                }
            }

            // 写入文件
            File.WriteAllText(reportFile, report.ToString(), System.Text.Encoding.UTF8);

            _logger.LogInformation("分析报告已生成: {ReportFile}", reportFile);
        }

        private string FormatFileSize(long bytes)
        {
            string[] sizes = ["B", "KB", "MB", "GB"];
            double len = bytes;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len /= 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }

        public List<MDBFileInfo> AnalyzeMultipleFiles(List<string> filePaths)
        {
            var results = new List<MDBFileInfo>();

            foreach (var filePath in filePaths)
            {
                try
                {
                    var fileInfo = AnalyzeMDB(filePath);
                    results.Add(fileInfo);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "分析文件失败: {FilePath}", filePath);
                }
            }

            return results;
        }

        public void GenerateComparisonReport(List<string> filePaths, string outputFolder)
        {
            Directory.CreateDirectory(outputFolder);

            var fileInfos = AnalyzeMultipleFiles(filePaths);

            string reportFile = Path.Combine(outputFolder, $"ComparisonReport_{DateTime.Now:yyyyMMdd_HHmmss}.txt");

            var report = new System.Text.StringBuilder();

            report.AppendLine("=== MDB文件比较报告 ===");
            report.AppendLine($"生成时间: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            report.AppendLine($"比较文件数: {fileInfos.Count}");
            report.AppendLine("=".PadRight(60, '='));
            report.AppendLine();

            // 统计摘要
            report.AppendLine("📊 统计摘要:");
            report.AppendLine($"  有效文件: {fileInfos.Count(f => f.IsValid)}");
            report.AppendLine($"  无效文件: {fileInfos.Count(f => !f.IsValid)}");
            report.AppendLine($"  总表数量: {fileInfos.Sum(f => f.TableCount)}");
            report.AppendLine($"  总行数: {fileInfos.Sum(f => f.TotalRows):N0}");
            report.AppendLine($"  总文件大小: {FormatFileSize(fileInfos.Sum(f => f.FileSize))}");
            report.AppendLine();

            // 文件详情
            report.AppendLine("📄 文件详情:");
            foreach (var fileInfo in fileInfos)
            {
                report.AppendLine($"\n{fileInfo.FileName}");
                report.AppendLine($"  状态: {(fileInfo.IsValid ? "有效" : "无效")}");
                report.AppendLine($"  大小: {FormatFileSize(fileInfo.FileSize)}");
                report.AppendLine($"  表数量: {fileInfo.TableCount}");
                report.AppendLine($"  行数: {fileInfo.TotalRows:N0}");
                report.AppendLine($"  Access版本: {fileInfo.AccessVersion}");

                if (!fileInfo.IsValid && !string.IsNullOrEmpty(fileInfo.ErrorMessage))
                {
                    report.AppendLine($"  错误: {fileInfo.ErrorMessage}");
                }
            }

            // 表结构比较
            report.AppendLine("\n📋 表结构比较:");
            var allTables = new Dictionary<string, List<string>>();

            foreach (var fileInfo in fileInfos.Where(f => f.IsValid))
            {
                foreach (var table in fileInfo.Tables)
                {
                    if (!allTables.ContainsKey(table.TableName))
                    {
                        allTables[table.TableName] = new List<string>();
                    }
                    allTables[table.TableName].Add(fileInfo.FileName);
                }
            }

            foreach (var tableEntry in allTables.OrderBy(t => t.Key))
            {
                report.AppendLine($"\n表: {tableEntry.Key}");
                report.AppendLine($"  出现在 {tableEntry.Value.Count} 个文件中:");
                foreach (var fileName in tableEntry.Value)
                {
                    report.AppendLine($"    - {fileName}");
                }
            }

            File.WriteAllText(reportFile, report.ToString(), System.Text.Encoding.UTF8);

            _logger.LogInformation("比较报告已生成: {ReportFile}", reportFile);
        }
    }
}