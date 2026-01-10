using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace MDBToSQLServer.Core
{
    public class SmartFileProcessor : IDisposable
    {
        private readonly FileStatusManager _statusManager;
        private readonly MDBImporter _importer;
        private readonly string _sourceFolder;
        private readonly string _archiveFolder;
        private readonly string _retryFolder;
        private readonly int _maxRetryCount;
        private readonly bool _keepSourceFiles;
        private readonly bool _parallelProcessing;
        private readonly int _maxDegreeOfParallelism;
        private readonly ILogger<SmartFileProcessor> _logger;

        private readonly ConcurrentDictionary<string, ImportResult> _processingResults;
        private CancellationTokenSource? _cancellationTokenSource;

        public SmartFileProcessor(
            IConfiguration configuration,
            ILogger<SmartFileProcessor> logger,
            FileStatusManager statusManager,
            MDBImporter importer)
        {
            var settings = configuration.GetSection("ApplicationSettings");

            _sourceFolder = settings["SourceFolder"] ?? @"D:\MDBFiles\Source\";
            _archiveFolder = settings["ArchiveFolder"] ?? @"D:\MDBFiles\Archive\";
            _retryFolder = settings["RetryFolder"] ?? @"D:\MDBFiles\Retry\";
            _maxRetryCount = int.Parse(settings["MaxRetryCount"] ?? "3");
            _keepSourceFiles = bool.Parse(settings["KeepSourceFiles"] ?? "true");
            _parallelProcessing = bool.Parse(settings["ParallelProcessing"] ?? "false");
            _maxDegreeOfParallelism = int.Parse(settings["MaxDegreeOfParallelism"] ?? "4");
            _logger = logger;

            _statusManager = statusManager;
            _importer = importer;
            _processingResults = new ConcurrentDictionary<string, ImportResult>();
            _cancellationTokenSource = new CancellationTokenSource();

            // 初始化状态管理器
            _statusManager.Initialize();

            _logger.LogInformation("SmartFileProcessor初始化完成。并行处理: {ParallelProcessing}", _parallelProcessing);
        }

        // 新增：处理单个文件的方法
        public ImportResult ProcessFile(string filePath)
        {
            string fileName = Path.GetFileName(filePath);
            var result = new ImportResult
            {
                FileName = fileName,
                FilePath = filePath,
                StartTime = DateTime.Now
            };

            _logger.LogInformation("开始处理单个文件: {FileName}", fileName);

            try
            {
                // 检查文件是否存在
                if (!File.Exists(filePath))
                {
                    _logger.LogWarning("文件不存在: {FilePath}", filePath);
                    result.Status = ImportStatus.Failed;
                    result.Message = "文件不存在";
                    result.EndTime = DateTime.Now;
                    return result;
                }

                // 检查文件大小
                var fileSize = new FileInfo(filePath).Length;
                if (fileSize == 0)
                {
                    _logger.LogWarning("文件为空: {FileName}", fileName);
                    result.Status = ImportStatus.Failed;
                    result.Message = "文件为空";
                    result.EndTime = DateTime.Now;
                    return result;
                }

                // 标记为处理中
                _statusManager.MarkAsProcessing(filePath);

                // 使用 MDBImporter 导入文件
                var importResult = _importer.ProcessFile(filePath);

                // 记录处理结果
                _processingResults[filePath] = importResult;

                // 更新状态
                switch (importResult.Status)
                {
                    case ImportStatus.Success:
                        _statusManager.MarkAsSuccess(filePath);
                        break;

                    case ImportStatus.PartialSuccess:
                        _statusManager.MarkAsPartialSuccess(filePath, importResult.Message);
                        break;

                    case ImportStatus.Failed:
                        _statusManager.MarkAsFailed(filePath, new Exception(importResult.Message));

                        // 复制到重试文件夹
                        var fileInfo = _statusManager.GetFilesByStatus(FileStatusManager.FileStatus.Failed)
                            .FirstOrDefault(f => f.FullPath.Equals(filePath, StringComparison.OrdinalIgnoreCase));

                        if (fileInfo != null && fileInfo.RetryCount < _maxRetryCount)
                        {
                            CopyToRetryFolder(filePath);
                        }
                        break;

                    case ImportStatus.Skipped:
                        _statusManager.MarkAsSkipped(filePath, importResult.Message);
                        break;
                }

                // 返回导入结果
                result = importResult;
                _logger.LogInformation("单个文件处理完成: {FileName} - {Status}", fileName, result.Status);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理单个文件失败: {FileName}", fileName);

                result.Status = ImportStatus.Failed;
                result.Message = ex.Message;
                result.EndTime = DateTime.Now;

                _statusManager.MarkAsFailed(filePath, ex);
            }
            finally
            {
                if (result.EndTime == default)
                {
                    result.EndTime = DateTime.Now;
                }
                result.Duration = result.EndTime - result.StartTime;

                // 永远不删除源文件
                _logger.LogInformation("源文件保留在: {FilePath}", filePath);
            }

            return result;
        }

        // 新增：批量处理文件的方法（原有的 ProcessAllFiles 方法）
        public ProcessingResult ProcessAllFiles()
        {
            _logger.LogInformation("开始批量处理所有文件");

            var result = new ProcessingResult
            {
                StartTime = DateTime.Now
            };

            try
            {
                // 获取所有待处理文件
                var pendingFiles = _statusManager.GetPendingFiles();
                result.TotalFiles = pendingFiles.Count;

                _logger.LogInformation("找到 {PendingFileCount} 个待处理文件", pendingFiles.Count);

                // 创建计数器
                var counters = new ProcessingCounters();

                if (_parallelProcessing)
                {
                    ProcessFilesInParallel(pendingFiles, counters);
                }
                else
                {
                    ProcessFilesSequentially(pendingFiles, counters);
                }

                // 更新统计到结果
                result.SuccessCount = counters.SuccessCount;
                result.PartialSuccessCount = counters.PartialSuccessCount;
                result.FailedCount = counters.FailedCount;
                result.SkippedCount = counters.SkippedCount;

                // 生成报告
                GenerateProcessingReport(result);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("处理被用户取消");
                result.Message = "处理被用户取消";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理所有文件失败");
                result.Message = $"处理失败: {ex.Message}";
            }
            finally
            {
                result.EndTime = DateTime.Now;
                result.Duration = result.EndTime - result.StartTime;

                _logger.LogInformation("批量处理完成。总耗时: {Duration:F1}秒", result.Duration.TotalSeconds);
            }

            return result;
        }

        private void ProcessFilesSequentially(List<FileStatusManager.FileInfoData> files, ProcessingCounters counters)
        {
            foreach (var fileInfo in files)
            {
                if (_cancellationTokenSource?.Token.IsCancellationRequested == true)
                    break;

                ProcessSingleFile(fileInfo, counters);
            }
        }

        private void ProcessFilesInParallel(List<FileStatusManager.FileInfoData> files, ProcessingCounters counters)
        {
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = _maxDegreeOfParallelism,
                CancellationToken = _cancellationTokenSource?.Token ?? CancellationToken.None
            };

            Parallel.ForEach(files, parallelOptions, fileInfo =>
            {
                ProcessSingleFile(fileInfo, counters);
            });
        }

        private void ProcessSingleFile(FileStatusManager.FileInfoData fileInfo, ProcessingCounters counters)
        {
            string filePath = fileInfo.FullPath;
            string fileName = fileInfo.FileName;

            try
            {
                // 标记为处理中
                _statusManager.MarkAsProcessing(filePath);

                _logger.LogInformation("开始处理文件: {FileName}", fileName);

                // 检查文件是否存在
                if (!File.Exists(filePath))
                {
                    _logger.LogWarning("文件不存在: {FilePath}", filePath);
                    _statusManager.MarkAsSkipped(filePath, "文件不存在");
                    Interlocked.Increment(ref counters._skippedCount);
                    return;
                }

                // 检查文件大小
                var fileSize = new FileInfo(filePath).Length;
                if (fileSize == 0)
                {
                    _logger.LogWarning("文件为空: {FileName}", fileName);
                    _statusManager.MarkAsSkipped(filePath, "文件为空");
                    Interlocked.Increment(ref counters._skippedCount);
                    return;
                }

                // 导入文件
                var importResult = _importer.ProcessFile(filePath);

                // 记录处理结果
                _processingResults[filePath] = importResult;

                // 更新状态和计数器
                switch (importResult.Status)
                {
                    case ImportStatus.Success:
                        _statusManager.MarkAsSuccess(filePath);
                        Interlocked.Increment(ref counters._successCount);

                        // 归档文件
                        if (_keepSourceFiles)
                        {
                            _importer.ArchiveFile(filePath, importResult.Status);
                        }
                        break;

                    case ImportStatus.PartialSuccess:
                        _statusManager.MarkAsPartialSuccess(filePath, importResult.Message);
                        Interlocked.Increment(ref counters._partialSuccessCount);

                        // 归档文件
                        if (_keepSourceFiles)
                        {
                            _importer.ArchiveFile(filePath, importResult.Status);
                        }
                        break;

                    case ImportStatus.Failed:
                        _statusManager.MarkAsFailed(filePath, new Exception(importResult.Message));
                        Interlocked.Increment(ref counters._failedCount);

                        // 复制到重试文件夹
                        if (fileInfo.RetryCount < _maxRetryCount)
                        {
                            CopyToRetryFolder(filePath);
                        }
                        break;

                    case ImportStatus.Skipped:
                        _statusManager.MarkAsSkipped(filePath, importResult.Message);
                        Interlocked.Increment(ref counters._skippedCount);
                        break;
                }

                _logger.LogInformation("文件处理完成: {FileName} - {Status}", fileName, importResult.Status);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理文件失败: {FileName}", fileName);

                _statusManager.MarkAsFailed(filePath, ex);
                Interlocked.Increment(ref counters._failedCount);

                // 复制到重试文件夹
                if (fileInfo.RetryCount < _maxRetryCount)
                {
                    CopyToRetryFolder(filePath);
                }
            }
            finally
            {
                // 永远不删除源文件
                _logger.LogInformation("源文件保留在: {FilePath}", filePath);
            }
        }

        // 新增：处理指定文件列表
        public ProcessingResult ProcessFiles(List<string> filePaths)
        {
            _logger.LogInformation("开始处理指定文件列表，共 {FileCount} 个文件", filePaths.Count);

            var result = new ProcessingResult
            {
                StartTime = DateTime.Now,
                TotalFiles = filePaths.Count
            };

            try
            {
                // 创建计数器
                var counters = new ProcessingCounters();

                foreach (var filePath in filePaths)
                {
                    if (_cancellationTokenSource?.Token.IsCancellationRequested == true)
                        break;

                    try
                    {
                        // 处理单个文件
                        var fileResult = ProcessFile(filePath);

                        // 更新计数器
                        switch (fileResult.Status)
                        {
                            case ImportStatus.Success:
                                Interlocked.Increment(ref counters._successCount);
                                break;
                            case ImportStatus.PartialSuccess:
                                Interlocked.Increment(ref counters._partialSuccessCount);
                                break;
                            case ImportStatus.Failed:
                                Interlocked.Increment(ref counters._failedCount);
                                break;
                            case ImportStatus.Skipped:
                                Interlocked.Increment(ref counters._skippedCount);
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "处理文件失败: {FilePath}", filePath);
                        Interlocked.Increment(ref counters._failedCount);
                    }
                }

                // 更新统计到结果
                result.SuccessCount = counters.SuccessCount;
                result.PartialSuccessCount = counters.PartialSuccessCount;
                result.FailedCount = counters.FailedCount;
                result.SkippedCount = counters.SkippedCount;

                // 生成报告
                GenerateProcessingReport(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理文件列表失败");
                result.Message = $"处理失败: {ex.Message}";
            }
            finally
            {
                result.EndTime = DateTime.Now;
                result.Duration = result.EndTime - result.StartTime;

                _logger.LogInformation("文件列表处理完成。总耗时: {Duration:F1}秒", result.Duration.TotalSeconds);
            }

            return result;
        }

        public ProcessingResult ProcessFailedFiles()
        {
            _logger.LogInformation("开始重试失败文件");

            var result = new ProcessingResult
            {
                StartTime = DateTime.Now
            };

            try
            {
                // 获取需要重试的文件
                var failedFiles = _statusManager.GetFailedFiles()
                    .Where(f => f.RetryCount < _maxRetryCount)
                    .ToList();

                result.TotalFiles = failedFiles.Count;

                _logger.LogInformation("找到 {FailedFileCount} 个需要重试的文件", failedFiles.Count);

                // 创建计数器
                var counters = new ProcessingCounters();

                foreach (var fileInfo in failedFiles)
                {
                    if (_cancellationTokenSource?.Token.IsCancellationRequested == true)
                        break;

                    RetryFailedFile(fileInfo, counters);
                }

                // 更新统计到结果
                result.SuccessCount = counters.SuccessCount;
                result.PartialSuccessCount = counters.PartialSuccessCount;
                result.FailedCount = counters.FailedCount;
                result.SkippedCount = counters.SkippedCount;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "重试失败文件失败");
                result.Message = $"重试失败: {ex.Message}";
            }
            finally
            {
                result.EndTime = DateTime.Now;
                result.Duration = result.EndTime - result.StartTime;
            }

            return result;
        }

        private void RetryFailedFile(FileStatusManager.FileInfoData fileInfo, ProcessingCounters counters)
        {
            string fileName = fileInfo.FileName;

            try
            {
                _logger.LogInformation("重试文件: {FileName} (第{RetryCount}次重试)",
                    fileName, fileInfo.RetryCount + 1);

                // 查找要处理的文件（优先使用重试文件夹中的副本）
                string fileToProcess = FindFileToProcess(fileInfo);

                if (!File.Exists(fileToProcess))
                {
                    _logger.LogWarning("文件不存在: {FilePath}", fileToProcess);
                    Interlocked.Increment(ref counters._skippedCount);
                    return;
                }

                // 导入文件
                var importResult = _importer.ProcessFile(fileToProcess);

                // 更新状态
                if (importResult.Status == ImportStatus.Success ||
                    importResult.Status == ImportStatus.PartialSuccess)
                {
                    _statusManager.MarkAsSuccess(fileInfo.FullPath);
                    Interlocked.Increment(ref counters._successCount);

                    // 归档文件
                    if (_keepSourceFiles)
                    {
                        _importer.ArchiveFile(fileToProcess, importResult.Status);
                    }
                }
                else
                {
                    _statusManager.MarkAsFailed(fileInfo.FullPath, new Exception(importResult.Message));
                    Interlocked.Increment(ref counters._failedCount);
                }

                _logger.LogInformation("重试完成: {FileName} - {Status}", fileName, importResult.Status);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "重试文件失败: {FileName}", fileName);

                _statusManager.MarkAsFailed(fileInfo.FullPath, ex);
                Interlocked.Increment(ref counters._failedCount);
            }
        }

        private string FindFileToProcess(FileStatusManager.FileInfoData fileInfo)
        {
            // 首先检查重试文件夹
            string retryPath = Path.Combine(_retryFolder, fileInfo.FileName);
            if (File.Exists(retryPath))
                return retryPath;

            // 检查源文件夹
            string sourcePath = fileInfo.FullPath;
            if (File.Exists(sourcePath))
                return sourcePath;

            // 查找带时间戳的文件
            var retryFiles = Directory.GetFiles(_retryFolder,
                $"{Path.GetFileNameWithoutExtension(fileInfo.FileName)}_*{Path.GetExtension(fileInfo.FileName)}");

            if (retryFiles.Length > 0)
            {
                return retryFiles.OrderByDescending(f => File.GetLastWriteTime(f)).First();
            }

            throw new FileNotFoundException($"找不到文件: {fileInfo.FileName}");
        }

        private void CopyToRetryFolder(string filePath)
        {
            try
            {
                string fileName = Path.GetFileName(filePath);
                string retryPath = Path.Combine(_retryFolder, fileName);

                // 如果文件已存在，添加时间戳
                if (File.Exists(retryPath))
                {
                    string timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
                    string newName = $"{Path.GetFileNameWithoutExtension(fileName)}_" +
                                    $"{timestamp}{Path.GetExtension(fileName)}";
                    retryPath = Path.Combine(_retryFolder, newName);
                }

                File.Copy(filePath, retryPath, true);
                _logger.LogInformation("文件已复制到重试文件夹: {RetryPath}", retryPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "复制到重试文件夹失败");
            }
        }

        private void GenerateProcessingReport(ProcessingResult result)
        {
            try
            {
                bool generateReport = true; // 可以从配置读取

                if (generateReport)
                {
                    string reportPath = Path.Combine(_archiveFolder,
                        $"ProcessingReport_{DateTime.Now:yyyyMMdd_HHmmss}.txt");

                    _statusManager.GenerateReport(reportPath);

                    // 添加处理结果摘要
                    string summary = $@"

=== 本次处理摘要 ===
开始时间: {result.StartTime:yyyy-MM-dd HH:mm:ss}
结束时间: {result.EndTime:yyyy-MM-dd HH:mm:ss}
总耗时: {result.Duration.TotalSeconds:F1}秒

处理统计:
  成功: {result.SuccessCount}
  部分成功: {result.PartialSuccessCount}
  失败: {result.FailedCount}
  跳过: {result.SkippedCount}
  总计: {result.TotalFiles}

";

                    File.AppendAllText(reportPath, summary);

                    _logger.LogInformation("处理报告已生成: {ReportPath}", reportPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "生成处理报告失败");
            }
        }

        public void CleanupOldFiles(int daysToKeep)
        {
            _logger.LogInformation("清理 {DaysToKeep} 天前的文件", daysToKeep);

            try
            {
                DateTime cutoffDate = DateTime.Now.AddDays(-daysToKeep);

                // 清理归档文件夹
                CleanupFolder(_archiveFolder, cutoffDate);

                // 清理重试文件夹
                CleanupFolder(_retryFolder, cutoffDate);

                // 清理错误文件夹
                CleanupFolder(Path.Combine(_archiveFolder, "Error"), cutoffDate);

                _logger.LogInformation("文件清理完成");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "清理文件失败");
            }
        }

        private void CleanupFolder(string folderPath, DateTime cutoffDate)
        {
            if (!Directory.Exists(folderPath))
                return;

            try
            {
                var files = Directory.GetFiles(folderPath, "*", SearchOption.AllDirectories)
                    .Where(f => File.GetLastWriteTime(f) < cutoffDate)
                    .ToList();

                int deletedCount = 0;
                foreach (var file in files)
                {
                    try
                    {
                        File.Delete(file);
                        deletedCount++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "删除文件失败: {FilePath}", file);
                    }
                }

                _logger.LogInformation("清理文件夹 {FolderPath}: 删除了 {DeletedCount} 个文件",
                    folderPath, deletedCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "清理文件夹失败: {FolderPath}", folderPath);
            }
        }

        public void CancelProcessing()
        {
            _cancellationTokenSource?.Cancel();
            _logger.LogInformation("处理已取消");
        }

        public void Dispose()
        {
            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.Dispose();
            _importer?.Dispose();
            GC.SuppressFinalize(this);
        }

        // 内部计数器类
        private class ProcessingCounters
        {
            public int _successCount = 0;
            public int _partialSuccessCount = 0;
            public int _failedCount = 0;
            public int _skippedCount = 0;

            public int SuccessCount => _successCount;
            public int PartialSuccessCount => _partialSuccessCount;
            public int FailedCount => _failedCount;
            public int SkippedCount => _skippedCount;
        }
    }

    public class ProcessingResult
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public TimeSpan Duration { get; set; }
        public string Message { get; set; } = string.Empty;
        public int TotalFiles { get; set; }
        public int SuccessCount { get; set; }
        public int PartialSuccessCount { get; set; }
        public int FailedCount { get; set; }
        public int SkippedCount { get; set; }
    }
}