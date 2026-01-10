using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data;
using System.Data.OleDb;
using System.Text;

namespace MDBToSQLServer.Core
{
    public class MDBImporter : IDisposable
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<MDBImporter> _logger;
        private readonly string _sourceFolder;
        private readonly string _archiveFolder;
        private readonly string _retryFolder;
        private readonly string _errorFolder;
        private readonly string _sqlServerConnectionString;
        private readonly int _batchSize;
        private readonly bool _autoCreateTables;
        private readonly bool _keepSourceFiles;
        private readonly int _commandTimeout;

        private TableManager? _tableManager;
        private ProviderHelper? _providerHelper;
        private FileStatusManager? _statusManager;

        public MDBImporter(
            IConfiguration configuration,
            ILogger<MDBImporter> logger,
            TableManager tableManager,
            ProviderHelper providerHelper,
            FileStatusManager statusManager)
        {
            _configuration = configuration;
            _logger = logger;

            var settings = configuration.GetSection("ApplicationSettings");

            // 读取配置
            _sourceFolder = settings["SourceFolder"] ?? @"D:\MDBFiles\Source\";
            _archiveFolder = settings["ArchiveFolder"] ?? @"D:\MDBFiles\Archive\";
            _retryFolder = settings["RetryFolder"] ?? @"D:\MDBFiles\Retry\";
            _errorFolder = settings["ErrorFolder"] ?? @"D:\MDBFiles\Error\";
            _batchSize = int.Parse(settings["BatchSize"] ?? "5000");
            _autoCreateTables = bool.Parse(settings["AutoCreateTables"] ?? "true");
            _keepSourceFiles = bool.Parse(settings["KeepSourceFiles"] ?? "true");
            _commandTimeout = int.Parse(settings["CommandTimeout"] ?? "300");

            // 数据库连接字符串
            _sqlServerConnectionString = configuration.GetConnectionString("SqlServer")
                ?? throw new InvalidOperationException("未配置SQL Server连接字符串");

            // 初始化辅助类
            _tableManager = tableManager;
            _providerHelper = providerHelper;
            _statusManager = statusManager;

            // 创建必要的文件夹
            CreateDirectories();

            _logger.LogInformation("MDBImporter初始化完成。源文件夹: {SourceFolder}", _sourceFolder);
        }

        private void CreateDirectories()
        {
            Directory.CreateDirectory(_sourceFolder);
            Directory.CreateDirectory(_archiveFolder);
            Directory.CreateDirectory(_retryFolder);
            Directory.CreateDirectory(_errorFolder);
            Directory.CreateDirectory(Path.Combine(_archiveFolder, "Success"));
            Directory.CreateDirectory(Path.Combine(_archiveFolder, "Partial"));
            Directory.CreateDirectory(Path.Combine(_archiveFolder, "Failed"));
        }

        public ImportResult ProcessFile(string filePath)
        {
            string fileName = Path.GetFileName(filePath);
            var result = new ImportResult
            {
                FileName = fileName,
                FilePath = filePath,
                StartTime = DateTime.Now
            };

            _logger.LogInformation("开始处理文件: {FileName}", fileName);

            try
            {
                // 检查文件是否被锁定
                if (_providerHelper?.IsFileLocked(filePath) == true)
                {
                    throw new IOException($"文件被其他进程占用: {fileName}");
                }

                // 获取合适的连接字符串
                string mdbConnStr = _providerHelper?.GetConnectionString(filePath)
                    ?? throw new InvalidOperationException("ProviderHelper未初始化");

                using (OleDbConnection mdbConn = new OleDbConnection(mdbConnStr))
                {
                    mdbConn.Open();
                    _logger.LogInformation("成功连接到MDB文件，提供程序: {Provider}", mdbConn.Provider);

                    // 获取所有表
                    var tables = GetTables(mdbConn);
                    result.TotalTables = tables.Count;

                    if (tables.Count == 0)
                    {
                        _logger.LogWarning("文件中未找到任何数据表: {FileName}", fileName);
                        result.Status = ImportStatus.Skipped;
                        result.Message = "文件中没有数据表";
                        return result;
                    }

                    _logger.LogInformation("找到 {TableCount} 个数据表", tables.Count);

                    // 处理每个表
                    foreach (var tableName in tables)
                    {
                        try
                        {
                            ImportTable(mdbConn, tableName, fileName);
                            result.SuccessTables++;
                        }
                        catch (Exception tableEx)
                        {
                            result.FailedTables++;
                            _logger.LogError(tableEx, "导入表 {TableName} 失败", tableName);

                            // 记录表级错误
                            result.TableErrors.Add(new TableError
                            {
                                TableName = tableName,
                                ErrorMessage = tableEx.Message
                            });
                        }
                    }
                }

                // 更新结果
                result.EndTime = DateTime.Now;
                result.Duration = result.EndTime - result.StartTime;

                if (result.FailedTables == 0 && result.SuccessTables > 0)
                {
                    result.Status = ImportStatus.Success;
                    result.Message = "全部导入成功";
                }
                else if (result.SuccessTables > 0)
                {
                    result.Status = ImportStatus.PartialSuccess;
                    result.Message = $"部分成功 ({result.SuccessTables}/{result.TotalTables})";
                }
                else
                {
                    result.Status = ImportStatus.Failed;
                    result.Message = "全部导入失败";
                }

                _logger.LogInformation("文件处理完成: {FileName} - {Status}", fileName, result.Status);
            }
            catch (Exception ex)
            {
                result.Status = ImportStatus.Failed;
                result.Message = ex.Message;
                result.EndTime = DateTime.Now;
                result.Duration = result.EndTime - result.StartTime;

                _logger.LogError(ex, "处理文件失败: {FileName}", fileName);

                // 保存错误信息
                SaveErrorDetails(filePath, ex, result);
            }

            return result;
        }

        private List<string> GetTables(OleDbConnection connection)
        {
            var tables = new List<string>();

            try
            {
                DataTable schema = connection.GetSchema("Tables");
                foreach (DataRow row in schema.Rows)
                {
                    string tableName = row["TABLE_NAME"].ToString() ?? string.Empty;
                    string tableType = row["TABLE_TYPE"].ToString() ?? string.Empty;

                    // 跳过系统表和临时表
                    if (tableType == "TABLE" &&
                        !tableName.StartsWith("MSys") &&
                        !tableName.StartsWith("~") &&
                        !tableName.StartsWith("_"))
                    {
                        tables.Add(tableName);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取表列表失败");
                throw;
            }

            return tables;
        }

        private void ImportTable(OleDbConnection mdbConn, string tableName, string sourceFile)
        {
            _logger.LogInformation("开始导入表: {TableName}", tableName);

            string query = $"SELECT * FROM [{tableName}]";

            using (OleDbCommand cmd = new OleDbCommand(query, mdbConn))
            {
                cmd.CommandTimeout = _commandTimeout;

                using (OleDbDataReader reader = cmd.ExecuteReader())
                {
                    // 确定目标表名
                    string destTableName = GetDestinationTableName(tableName, sourceFile);

                    // 确保目标表存在
                    if (_autoCreateTables)
                    {
                        EnsureDestinationTableExists(reader, destTableName);
                    }
                    else if (!_tableManager!.TableExists(destTableName))
                    {
                        throw new InvalidOperationException($"目标表不存在: {destTableName}");
                    }

                    // 执行批量导入
                    BulkInsertData(reader, destTableName, sourceFile);
                }
            }

            _logger.LogInformation("表导入完成: {TableName}", tableName);
        }

        private string GetDestinationTableName(string sourceTableName, string sourceFile)
        {
            // 默认策略：添加前缀避免冲突
            string prefix = _configuration.GetSection("ApplicationSettings")["TablePrefix"] ?? "MDB_";
            string sanitizedTableName = SanitizeTableName(sourceTableName);

            return $"{prefix}{sanitizedTableName}";
        }

        private string SanitizeTableName(string tableName)
        {
            // 移除非法字符
            char[] invalidChars = [' ', '-', '.', ',', ';', ':', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '+', '=', '[', ']', '{', '}', '|', '\\', '/', '<', '>', '?', '"', '\''];

            string sanitized = tableName;
            foreach (var ch in invalidChars)
            {
                sanitized = sanitized.Replace(ch.ToString(), "_");
            }

            // 确保以字母开头
            if (sanitized.Length > 0 && char.IsDigit(sanitized[0]))
            {
                sanitized = "T_" + sanitized;
            }

            // 限制长度
            if (sanitized.Length > 128)
            {
                sanitized = sanitized[..128];
            }

            return sanitized;
        }

        private void EnsureDestinationTableExists(OleDbDataReader reader, string tableName)
        {
            try
            {
                if (!_tableManager!.TableExists(tableName))
                {
                    _logger.LogInformation("创建目标表: {TableName}", tableName);
                    _tableManager.CreateTableFromReader(reader, tableName);
                }
                else
                {
                    // 检查表结构是否需要更新
                    bool syncStructure = bool.Parse(_configuration.GetSection("ApplicationSettings")["SyncTableStructure"] ?? "false");
                    if (syncStructure)
                    {
                        _tableManager.SyncTableStructure(tableName, reader);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "创建/检查表失败: {TableName}", tableName);
                throw;
            }
        }

        //private void BulkInsertData(OleDbDataReader reader, string tableName, string sourceFile)
        //{
        //    int totalRows = 0;

        //    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_sqlServerConnectionString,
        //        SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction))
        //    {
        //        bulkCopy.DestinationTableName = tableName;
        //        bulkCopy.BatchSize = _batchSize;
        //        bulkCopy.BulkCopyTimeout = _commandTimeout;
        //        bulkCopy.NotifyAfter = 1000;

        //        // 设置列映射
        //        for (int i = 0; i < reader.FieldCount; i++)
        //        {
        //            string columnName = reader.GetName(i);
        //            try
        //            {
        //                if (columnName != "__SourceFile")
        //                {
        //                    bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(i, columnName));
        //                }
        //                //bulkCopy.ColumnMappings.Add(columnName, columnName);
        //            }
        //            catch (Exception ex)
        //            {
        //                _logger.LogWarning(ex, "列映射失败 {ColumnName}", columnName);
        //            }
        //        }

        //        // 添加额外列
        //        bool addSourceFile = bool.Parse(_configuration.GetSection("ApplicationSettings")["AddSourceFileColumn"] ?? "true");
        //        bool addImportTime = bool.Parse(_configuration.GetSection("ApplicationSettings")["AddImportTimeColumn"] ?? "true");

        //        if (addSourceFile)
        //        {
        //            bulkCopy.ColumnMappings.Add("__SourceFile", "__SourceFile");
        //        }
        //        if (addImportTime)
        //        {
        //            bulkCopy.ColumnMappings.Add("__ImportTime", "__ImportTime");
        //        }

        //        // 进度通知
        //        bulkCopy.SqlRowsCopied += (sender, e) =>
        //        {
        //            totalRows = (int)e.RowsCopied;
        //            if (totalRows % 10000 == 0)
        //            {
        //                _logger.LogInformation("表 {TableName} 已导入 {RowCount} 行", tableName, totalRows);
        //            }
        //        };

        //        // 使用DataReader直接写入
        //        try
        //        {
        //            bulkCopy.WriteToServer(reader);
        //            _logger.LogInformation("表 {TableName} 导入完成，共 {RowCount} 行", tableName, totalRows);
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, "批量插入失败: {TableName}", tableName);
        //            throw;
        //        }
        //    }

        //}

        private void BulkInsertData(OleDbDataReader reader, string tableName, string sourceFile)
        {
            int totalRows = 0;

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_sqlServerConnectionString,
                SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = _batchSize;
                bulkCopy.BulkCopyTimeout = _commandTimeout;
                bulkCopy.NotifyAfter = 1000;

                // 只映射源数据中实际存在的列
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string columnName = reader.GetName(i);
                    try
                    {
                        // 统一使用名称映射
                        bulkCopy.ColumnMappings.Add(columnName, columnName);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "列映射失败 {ColumnName}", columnName);
                    }
                }

                // 注释掉额外列的添加（因为它们不在源数据中）
                // bool addSourceFile = bool.Parse(_configuration.GetSection("ApplicationSettings")["AddSourceFileColumn"] ?? "true");
                // bool addImportTime = bool.Parse(_configuration.GetSection("ApplicationSettings")["AddImportTimeColumn"] ?? "true");

                // 改为导入后更新这些列
                // if (addSourceFile)
                // {
                //     bulkCopy.ColumnMappings.Add("__SourceFile", "__SourceFile");
                // }
                // if (addImportTime)
                // {
                //     bulkCopy.ColumnMappings.Add("__ImportTime", "__ImportTime");
                // }

                // 进度通知
                bulkCopy.SqlRowsCopied += (sender, e) =>
                {
                    totalRows = (int)e.RowsCopied;
                    if (totalRows % 10000 == 0)
                    {
                        _logger.LogInformation("表 {TableName} 已导入 {RowCount} 行", tableName, totalRows);
                    }
                };

                try
                {
                    bulkCopy.WriteToServer(reader);
                    _logger.LogInformation("表 {TableName} 导入完成，共 {RowCount} 行", tableName, totalRows);

                    // 导入后更新额外列
                    UpdateExtraColumns(tableName, sourceFile);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "批量插入失败: {TableName}", tableName);
                    throw;
                }
            }
        }

        private void UpdateExtraColumns(string tableName, string sourceFile)
        {
            try
            {
                bool addSourceFile = bool.Parse(_configuration.GetSection("ApplicationSettings")["AddSourceFileColumn"] ?? "true");
                bool addImportTime = bool.Parse(_configuration.GetSection("ApplicationSettings")["AddImportTimeColumn"] ?? "true");

                if (!addSourceFile && !addImportTime)
                    return;

                using (var connection = new SqlConnection(_sqlServerConnectionString))
                {
                    connection.Open();

                    var updateSql = new StringBuilder($"UPDATE [{tableName}] SET ");
                    var parameters = new List<SqlParameter>();

                    if (addSourceFile)
                    {
                        updateSql.Append("__SourceFile = @SourceFile");
                        parameters.Add(new SqlParameter("@SourceFile", sourceFile));
                    }

                    if (addImportTime)
                    {
                        if (addSourceFile)
                            updateSql.Append(", ");
                        updateSql.Append("__ImportTime = @ImportTime");
                        parameters.Add(new SqlParameter("@ImportTime", DateTime.Now));
                    }

                    // 只更新没有这些值的行
                    updateSql.Append(" WHERE __SourceFile IS NULL OR __ImportTime IS NULL");

                    using (var command = new SqlCommand(updateSql.ToString(), connection))
                    {
                        command.Parameters.AddRange(parameters.ToArray());
                        int rowsUpdated = command.ExecuteNonQuery();

                        if (rowsUpdated > 0)
                        {
                            _logger.LogInformation("更新了 {RowCount} 行的额外列", rowsUpdated);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "更新额外列失败");
            }
        }

        private void SaveErrorDetails(string filePath, Exception ex, ImportResult result)
        {
            try
            {
                string errorFileName = $"{Path.GetFileNameWithoutExtension(filePath)}_{DateTime.Now:yyyyMMddHHmmss}_ERROR.json";
                string errorFilePath = Path.Combine(_errorFolder, errorFileName);

                var errorDetails = new
                {
                    FileName = Path.GetFileName(filePath),
                    FilePath = filePath,
                    ErrorTime = DateTime.Now,
                    ErrorType = ex.GetType().Name,
                    ErrorMessage = ex.Message,
                    StackTrace = ex.StackTrace,
                    ImportResult = result
                };

                string json = JsonConvert.SerializeObject(errorDetails, Formatting.Indented);
                File.WriteAllText(errorFilePath, json);

                _logger.LogInformation("错误详情已保存: {ErrorFilePath}", errorFilePath);
            }
            catch (Exception saveEx)
            {
                _logger.LogError(saveEx, "保存错误详情失败");
            }
        }

        public void ArchiveFile(string filePath, ImportStatus status)
        {
            if (!_keepSourceFiles)
                return;

            try
            {
                string fileName = Path.GetFileName(filePath);
                string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                string statusFolder = status.ToString();
                string archivePath = Path.Combine(_archiveFolder, statusFolder);

                Directory.CreateDirectory(archivePath);

                string destFileName = $"{Path.GetFileNameWithoutExtension(fileName)}_" +
                                     $"{timestamp}_{status}{Path.GetExtension(fileName)}";
                string destPath = Path.Combine(archivePath, destFileName);

                File.Copy(filePath, destPath, true);
                _logger.LogInformation("文件已归档到: {DestPath}", destPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "归档文件失败");
            }
        }

        public void Dispose()
        {
            _tableManager = null;
            _providerHelper = null;
            _statusManager = null;
            GC.SuppressFinalize(this);
        }
    }

    public class ImportResult
    {
        public string FileName { get; set; } = string.Empty;
        public string FilePath { get; set; } = string.Empty;
        public ImportStatus Status { get; set; }
        public string Message { get; set; } = string.Empty;
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public TimeSpan? Duration  { get; set; }
        public int TotalTables { get; set; }
        public int SuccessTables { get; set; }
        public int FailedTables { get; set; }
        public List<TableError> TableErrors { get; set; } = new List<TableError>();
    }

    public enum ImportStatus
    {
        Pending,
        Processing,
        Success,
        PartialSuccess,
        Failed,
        Skipped
    }

    public class TableError
    {
        public string TableName { get; set; } = string.Empty;
        public string ErrorMessage { get; set; } = string.Empty;
        public DateTime ErrorTime { get; set; } = DateTime.Now;
    }
}