using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace MDBToSQLServer.Core
{
    public class FileStatusManager
    {
        private readonly string _sourceFolder;
        private readonly string _statusDbPath;
        private readonly ILogger<FileStatusManager> _logger;
        private readonly object _lock = new();

        public enum FileStatus
        {
            Pending,        // 等待处理
            Processing,     // 处理中
            Success,        // 成功
            PartialSuccess, // 部分成功
            Failed,         // 失败
            Skipped         // 跳过
        }

        public class FileInfoData
        {
            public string FileName { get; set; } = string.Empty;
            public string FullPath { get; set; } = string.Empty;
            public FileStatus Status { get; set; }
            public DateTime LastModified { get; set; }
            public DateTime ProcessTime { get; set; }
            public string? ErrorMessage { get; set; }
            public int RetryCount { get; set; }
            public string? DestinationPath { get; set; }
            public long FileSize { get; set; }
            public int TableCount { get; set; }
            public int ImportedRows { get; set; }

            public FileInfoData Clone()
            {
                return new FileInfoData
                {
                    FileName = this.FileName,
                    FullPath = this.FullPath,
                    Status = this.Status,
                    LastModified = this.LastModified,
                    ProcessTime = this.ProcessTime,
                    ErrorMessage = this.ErrorMessage,
                    RetryCount = this.RetryCount,
                    DestinationPath = this.DestinationPath,
                    FileSize = this.FileSize,
                    TableCount = this.TableCount,
                    ImportedRows = this.ImportedRows
                };
            }
        }

        public class Statistics
        {
            public int TotalFiles { get; set; }
            public int PendingCount { get; set; }
            public int ProcessingCount { get; set; }
            public int SuccessCount { get; set; }
            public int PartialSuccessCount { get; set; }
            public int FailedCount { get; set; }
            public int SkippedCount { get; set; }
            public int TotalRetryCount { get; set; }
            public long TotalFileSize { get; set; }
            public int TotalTables { get; set; }
            public long TotalImportedRows { get; set; }
            public DateTime LastProcessTime { get; set; }
        }

        public FileStatusManager(IConfiguration configuration, ILogger<FileStatusManager> logger)
        {
            var sourceFolder = configuration.GetSection("ApplicationSettings")["SourceFolder"]
                ?? throw new ArgumentNullException("SourceFolder");

            _sourceFolder = sourceFolder;
            _statusDbPath = Path.Combine(sourceFolder, "_import_status.json");
            _logger = logger;

            _logger.LogInformation("FileStatusManager初始化，源文件夹: {SourceFolder}", _sourceFolder);
        }

        public void Initialize()
        {
            lock (_lock)
            {
                try
                {
                    if (!File.Exists(_statusDbPath))
                    {
                        // 创建初始状态文件
                        var files = GetPendingFiles();
                        SaveStatus(files);
                        _logger.LogInformation("状态数据库初始化完成，找到 {FileCount} 个文件", files.Count);
                    }
                    else
                    {
                        // 加载现有状态
                        var files = LoadStatus();
                        _logger.LogInformation("加载状态数据库，包含 {FileCount} 个文件记录", files.Count);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "初始化状态管理器失败");
                    throw;
                }
            }
        }

        public List<FileInfoData> GetPendingFiles()
        {
            try
            {
                var existingStatus = LoadStatus();
                var existingFiles = new HashSet<string>(
                    existingStatus.Select(f => f.FullPath),
                    StringComparer.OrdinalIgnoreCase);

                var pendingFiles = new List<FileInfoData>();

                // 获取所有MDB文件
                var files = Directory.GetFiles(_sourceFolder, "*.mdb", SearchOption.TopDirectoryOnly)
                    .Concat(Directory.GetFiles(_sourceFolder, "*.accdb", SearchOption.TopDirectoryOnly));

                foreach (var filePath in files)
                {
                    try
                    {
                        var fileInfo = new FileInfoData
                        {
                            FileName = Path.GetFileName(filePath),
                            FullPath = filePath,
                            LastModified = File.GetLastWriteTime(filePath),
                            FileSize = new FileInfo(filePath).Length
                        };

                        // 检查是否已在状态数据库中
                        var existingFile = existingStatus.FirstOrDefault(f =>
                            f.FullPath.Equals(filePath, StringComparison.OrdinalIgnoreCase));

                        if (existingFile != null)
                        {
                            // 如果文件已修改，重置为待处理状态
                            if (existingFile.LastModified < fileInfo.LastModified)
                            {
                                existingFile.Status = FileStatus.Pending;
                                existingFile.LastModified = fileInfo.LastModified;
                                existingFile.FileSize = fileInfo.FileSize;
                                existingFile.ErrorMessage = null;
                                pendingFiles.Add(existingFile.Clone());
                            }
                            else if (existingFile.Status == FileStatus.Pending ||
                                     existingFile.Status == FileStatus.Failed)
                            {
                                pendingFiles.Add(existingFile.Clone());
                            }
                        }
                        else
                        {
                            // 新文件
                            fileInfo.Status = FileStatus.Pending;
                            pendingFiles.Add(fileInfo);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "处理文件信息失败: {FilePath}", filePath);
                    }
                }

                return pendingFiles;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取待处理文件失败");
                return new List<FileInfoData>();
            }
        }

        public List<FileInfoData> GetFailedFiles()
        {
            return LoadStatus()
                .Where(f => f.Status == FileStatus.Failed)
                .ToList();
        }

        public List<FileInfoData> GetFilesByStatus(FileStatus status)
        {
            return LoadStatus()
                .Where(f => f.Status == status)
                .OrderByDescending(f => f.ProcessTime)
                .ToList();
        }

        public void MarkAsProcessing(string filePath)
        {
            UpdateFileStatus(filePath, FileStatus.Processing, null);
        }

        public void MarkAsSuccess(string filePath, string? destPath = null,
            int tableCount = 0, int importedRows = 0)
        {
            var fileInfo = GetFileInfo(filePath);
            fileInfo.Status = FileStatus.Success;
            fileInfo.ProcessTime = DateTime.Now;
            fileInfo.DestinationPath = destPath;
            fileInfo.TableCount = tableCount;
            fileInfo.ImportedRows = importedRows;
            fileInfo.ErrorMessage = null;

            UpdateFileInfo(fileInfo);
        }

        public void MarkAsPartialSuccess(string filePath, string? error = null,
            int tableCount = 0, int importedRows = 0)
        {
            var fileInfo = GetFileInfo(filePath);
            fileInfo.Status = FileStatus.PartialSuccess;
            fileInfo.ProcessTime = DateTime.Now;
            fileInfo.TableCount = tableCount;
            fileInfo.ImportedRows = importedRows;
            fileInfo.ErrorMessage = error;

            UpdateFileInfo(fileInfo);
        }

        public void MarkAsFailed(string filePath, Exception ex)
        {
            var fileInfo = GetFileInfo(filePath);
            fileInfo.Status = FileStatus.Failed;
            fileInfo.ProcessTime = DateTime.Now;
            fileInfo.ErrorMessage = ex?.Message;
            fileInfo.RetryCount++;

            UpdateFileInfo(fileInfo);
        }

        public void MarkAsSkipped(string filePath, string reason)
        {
            var fileInfo = GetFileInfo(filePath);
            fileInfo.Status = FileStatus.Skipped;
            fileInfo.ProcessTime = DateTime.Now;
            fileInfo.ErrorMessage = reason;

            UpdateFileInfo(fileInfo);
        }

        public bool ShouldRetry(string filePath)
        {
            var fileInfo = GetFileInfo(filePath);
            int maxRetries = 3; // 可以从配置读取

            return fileInfo.Status == FileStatus.Failed &&
                   fileInfo.RetryCount < maxRetries;
        }

        public Statistics GetStatistics()
        {
            var files = LoadStatus();

            return new Statistics
            {
                TotalFiles = files.Count,
                PendingCount = files.Count(f => f.Status == FileStatus.Pending),
                ProcessingCount = files.Count(f => f.Status == FileStatus.Processing),
                SuccessCount = files.Count(f => f.Status == FileStatus.Success),
                PartialSuccessCount = files.Count(f => f.Status == FileStatus.PartialSuccess),
                FailedCount = files.Count(f => f.Status == FileStatus.Failed),
                SkippedCount = files.Count(f => f.Status == FileStatus.Skipped),
                TotalRetryCount = files.Sum(f => f.RetryCount),
                TotalFileSize = files.Sum(f => f.FileSize),
                TotalTables = files.Sum(f => f.TableCount),
                TotalImportedRows = files.Sum(f => f.ImportedRows),
                LastProcessTime = files.Max(f => f.ProcessTime)
            };
        }

        public void GenerateReport(string reportPath)
        {
            try
            {
                var stats = GetStatistics();
                var allFiles = LoadStatus();

                var report = new System.Text.StringBuilder();
                report.AppendLine("=== MDB文件导入状态报告 ===");
                report.AppendLine($"生成时间: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                report.AppendLine($"报告文件: {reportPath}");
                report.AppendLine("=".PadRight(60, '='));
                report.AppendLine();

                // 统计信息
                report.AppendLine("📊 统计信息:");
                report.AppendLine($"   文件总数: {stats.TotalFiles}");
                report.AppendLine($"   成功文件: {stats.SuccessCount}");
                report.AppendLine($"   部分成功: {stats.PartialSuccessCount}");
                report.AppendLine($"   失败文件: {stats.FailedCount}");
                report.AppendLine($"   跳过文件: {stats.SkippedCount}");
                report.AppendLine($"   待处理文件: {stats.PendingCount}");
                report.AppendLine($"   总重试次数: {stats.TotalRetryCount}");
                report.AppendLine($"   总文件大小: {FormatFileSize(stats.TotalFileSize)}");
                report.AppendLine($"   总表数量: {stats.TotalTables}");
                report.AppendLine($"   总导入行数: {stats.TotalImportedRows:N0}");
                report.AppendLine($"   最后处理时间: {stats.LastProcessTime:yyyy-MM-dd HH:mm:ss}");
                report.AppendLine();

                // 状态分布
                report.AppendLine("📈 状态分布:");
                var statusGroups = allFiles.GroupBy(f => f.Status)
                    .Select(g => new { Status = g.Key, Count = g.Count() })
                    .OrderByDescending(g => g.Count);

                foreach (var group in statusGroups)
                {
                    double percentage = stats.TotalFiles > 0 ?
                        (double)group.Count / stats.TotalFiles * 100 : 0;
                    report.AppendLine($"   {GetStatusDisplayName(group.Status)}: {group.Count} ({percentage:F1}%)");
                }
                report.AppendLine();

                // 最近处理的文件
                report.AppendLine("🕒 最近处理的文件 (最近10个):");
                var recentFiles = allFiles
                    .Where(f => f.ProcessTime > DateTime.MinValue)
                    .OrderByDescending(f => f.ProcessTime)
                    .Take(10);

                foreach (var file in recentFiles)
                {
                    report.AppendLine($"   {file.FileName}");
                    report.AppendLine($"     状态: {GetStatusDisplayName(file.Status)}");
                    report.AppendLine($"     处理时间: {file.ProcessTime:yyyy-MM-dd HH:mm:ss}");
                    report.AppendLine($"     大小: {FormatFileSize(file.FileSize)}");
                    report.AppendLine($"     表数量: {file.TableCount}");
                    report.AppendLine($"     导入行数: {file.ImportedRows:N0}");
                    if (!string.IsNullOrEmpty(file.ErrorMessage))
                    {
                        report.AppendLine($"     错误: {file.ErrorMessage}");
                    }
                    report.AppendLine();
                }

                // 失败文件详情
                var failedFiles = allFiles.Where(f => f.Status == FileStatus.Failed).ToList();
                if (failedFiles.Count > 0)
                {
                    report.AppendLine("⚠️ 失败文件详情:");
                    foreach (var file in failedFiles)
                    {
                        report.AppendLine($"   {file.FileName}");
                        report.AppendLine($"     重试次数: {file.RetryCount}");
                        report.AppendLine($"     最后错误: {file.ErrorMessage}");
                        report.AppendLine($"     最后处理: {file.ProcessTime:yyyy-MM-dd HH:mm:ss}");
                        report.AppendLine();
                    }
                }

                // 写入文件
                File.WriteAllText(reportPath, report.ToString(), System.Text.Encoding.UTF8);

                _logger.LogInformation("状态报告已生成: {ReportPath}", reportPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "生成状态报告失败");
                throw;
            }
        }

        private string GetStatusDisplayName(FileStatus status)
        {
            return status switch
            {
                FileStatus.Pending => "待处理",
                FileStatus.Processing => "处理中",
                FileStatus.Success => "成功",
                FileStatus.PartialSuccess => "部分成功",
                FileStatus.Failed => "失败",
                FileStatus.Skipped => "跳过",
                _ => "未知"
            };
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

        private FileInfoData GetFileInfo(string filePath)
        {
            var allFiles = LoadStatus();
            return allFiles.FirstOrDefault(f => f.FullPath.Equals(filePath, StringComparison.OrdinalIgnoreCase))
                   ?? CreateNewFileInfo(filePath);
        }

        private FileInfoData CreateNewFileInfo(string filePath)
        {
            try
            {
                var fileInfo = new FileInfoData
                {
                    FileName = Path.GetFileName(filePath),
                    FullPath = filePath,
                    Status = FileStatus.Pending,
                    LastModified = File.GetLastWriteTime(filePath),
                    FileSize = new FileInfo(filePath).Length
                };

                return fileInfo;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "创建文件信息失败: {FilePath}", filePath);

                // 返回基本信息
                return new FileInfoData
                {
                    FileName = Path.GetFileName(filePath),
                    FullPath = filePath,
                    Status = FileStatus.Pending,
                    LastModified = DateTime.Now,
                    FileSize = 0
                };
            }
        }

        private void UpdateFileStatus(string filePath, FileStatus status, string? errorMessage)
        {
            var fileInfo = GetFileInfo(filePath);
            fileInfo.Status = status;
            fileInfo.ProcessTime = DateTime.Now;
            fileInfo.ErrorMessage = errorMessage;

            UpdateFileInfo(fileInfo);
        }

        private void UpdateFileInfo(FileInfoData fileInfo)
        {
            var allFiles = LoadStatus();

            // 更新或添加文件信息
            int index = allFiles.FindIndex(f =>
                f.FullPath.Equals(fileInfo.FullPath, StringComparison.OrdinalIgnoreCase));

            if (index >= 0)
            {
                allFiles[index] = fileInfo;
            }
            else
            {
                allFiles.Add(fileInfo);
            }

            SaveStatus(allFiles);
        }

        private List<FileInfoData> LoadStatus()
        {
            lock (_lock)
            {
                if (!File.Exists(_statusDbPath))
                    return new List<FileInfoData>();

                try
                {
                    string json = File.ReadAllText(_statusDbPath);
                    var files = JsonConvert.DeserializeObject<List<FileInfoData>>(json)
                        ?? new List<FileInfoData>();

                    // 过滤掉不存在的文件
                    files = files.Where(f => File.Exists(f.FullPath)).ToList();

                    return files;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "加载状态文件失败");
                    return new List<FileInfoData>();
                }
            }
        }

        private void SaveStatus(List<FileInfoData> files)
        {
            lock (_lock)
            {
                try
                {
                    string json = JsonConvert.SerializeObject(files, Formatting.Indented);
                    File.WriteAllText(_statusDbPath, json);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "保存状态文件失败");
                }
            }
        }

        public void CleanupStatusDatabase(int daysToKeep)
        {
            lock (_lock)
            {
                try
                {
                    var files = LoadStatus();
                    DateTime cutoffDate = DateTime.Now.AddDays(-daysToKeep);

                    // 移除过期的记录
                    int initialCount = files.Count;
                    files = files.Where(f => f.ProcessTime > cutoffDate).ToList();

                    if (files.Count < initialCount)
                    {
                        SaveStatus(files);
                        _logger.LogInformation("状态数据库清理完成，移除了 {RemovedCount} 条过期记录",
                            initialCount - files.Count);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "清理状态数据库失败");
                }
            }
        }
    }
}