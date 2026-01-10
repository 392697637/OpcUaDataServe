using MDBToSQLServer.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace MDBToSQLServer.Services
{
    public class FileMonitorService : IDisposable
    {
        private readonly FileSystemWatcher _watcher;
        private readonly SmartFileProcessor _processor;
        private readonly string _sourceFolder;
        private readonly string _archiveFolder;
        private readonly int _processingDelay;
        private readonly int _batchProcessingInterval;
        private readonly bool _enableRealTimeProcessing;
        private readonly ILogger<FileMonitorService> _logger;

        private readonly ConcurrentQueue<string> _fileQueue;
        private readonly System.Timers.Timer _processingTimer;
        private readonly System.Timers.Timer _batchTimer;
        private bool _isProcessing;
        private bool _isRunning;
        private CancellationTokenSource? _cancellationTokenSource;

        public event EventHandler<FileProcessedEventArgs>? FileProcessed;
        public event EventHandler<FileProcessingErrorEventArgs>? FileProcessingError;

        public class FileProcessedEventArgs : EventArgs
        {
            public string FilePath { get; set; } = string.Empty;
            public ImportResult? Result { get; set; }
            public DateTime ProcessTime { get; set; }
        }

        public class FileProcessingErrorEventArgs : EventArgs
        {
            public string FilePath { get; set; } = string.Empty;
            public Exception? Error { get; set; }
            public DateTime ErrorTime { get; set; }
        }

        public FileMonitorService(
            IConfiguration configuration,
            ILogger<FileMonitorService> logger,
            SmartFileProcessor processor)
        {
            var settings = configuration.GetSection("ApplicationSettings");

            _sourceFolder = settings["SourceFolder"] ?? @"D:\MDBFiles\Source\";
            _archiveFolder = settings["ArchiveFolder"] ?? @"D:\MDBFiles\Archive\";
            _processingDelay = int.Parse(settings["ProcessingDelaySeconds"] ?? "5");
            _batchProcessingInterval = int.Parse(settings["BatchProcessingIntervalMinutes"] ?? "60");
            _enableRealTimeProcessing = bool.Parse(settings["MonitorSourceFolder"] ?? "false");
            _logger = logger;

            _fileQueue = new ConcurrentQueue<string>();
            _processor = processor;
            _cancellationTokenSource = new CancellationTokenSource();

            // 初始化文件监控器
            _watcher = new FileSystemWatcher
            {
                Path = _sourceFolder,
                Filter = "*.mdb",
                NotifyFilter = NotifyFilters.FileName |
                              NotifyFilters.LastWrite |
                              NotifyFilters.CreationTime,
                EnableRaisingEvents = false,
                IncludeSubdirectories = false
            };

            // 注册事件处理程序
            _watcher.Created += OnFileCreated;
            _watcher.Changed += OnFileChanged;
            _watcher.Renamed += OnFileRenamed;
            _watcher.Error += OnWatcherError;

            // 初始化处理定时器
            _processingTimer = new System.Timers.Timer(_processingDelay * 1000);
            _processingTimer.Elapsed += ProcessQueuedFiles;
            _processingTimer.AutoReset = true;

            // 初始化批量处理定时器
            _batchTimer = new System.Timers.Timer(_batchProcessingInterval * 60 * 1000);
            _batchTimer.Elapsed += OnBatchProcessing;
            _batchTimer.AutoReset = true;

            _logger.LogInformation("FileMonitorService初始化完成。源文件夹: {SourceFolder}", _sourceFolder);
        }

        public void Start()
        {
            if (_isRunning)
                return;

            try
            {
                _logger.LogInformation("启动文件监控服务...");

                // 启动文件监控
                _watcher.EnableRaisingEvents = true;

                // 启动处理定时器
                _processingTimer.Start();

                // 启动批量处理定时器
                _batchTimer.Start();

                _isRunning = true;
                _cancellationTokenSource = new CancellationTokenSource();

                // 处理已有文件
                Task.Run(() => ProcessExistingFiles());

                _logger.LogInformation("文件监控服务已启动");
                _logger.LogInformation("监控文件夹: {SourceFolder}", _sourceFolder);
                _logger.LogInformation("处理延迟: {ProcessingDelay}秒", _processingDelay);
                _logger.LogInformation("批量处理间隔: {BatchProcessingInterval}分钟", _batchProcessingInterval);
                _logger.LogInformation("实时处理: {EnableRealTimeProcessing}", _enableRealTimeProcessing);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "启动文件监控服务失败");
                throw;
            }
        }

        public void Stop()
        {
            if (!_isRunning)
                return;

            try
            {
                _logger.LogInformation("停止文件监控服务...");

                _cancellationTokenSource?.Cancel();

                // 停止文件监控
                _watcher.EnableRaisingEvents = false;

                // 停止定时器
                _processingTimer.Stop();
                _batchTimer.Stop();

                // 处理队列中剩余的文件
                ProcessRemainingFiles();

                _isRunning = false;

                _logger.LogInformation("文件监控服务已停止");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "停止文件监控服务失败");
                throw;
            }
        }

        private void OnFileCreated(object sender, FileSystemEventArgs e)
        {
            if (!_enableRealTimeProcessing)
                return;

            try
            {
                string fileName = e.Name;
                _logger.LogInformation("检测到新文件: {FileName}", fileName);

                // 将文件加入处理队列
                _fileQueue.Enqueue(e.FullPath);

                // 记录文件信息
                LogFileInfo(e.FullPath, "创建");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理文件创建事件失败: {FileName}", e.Name);
            }
        }

        private void OnFileChanged(object sender, FileSystemEventArgs e)
        {
            if (!_enableRealTimeProcessing)
                return;

            try
            {
                // 避免处理临时文件
                if (e.Name.StartsWith("~") || e.Name.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
                    return;

                string fileName = e.Name;
                _logger.LogInformation("检测到文件变化: {FileName}", fileName);

                // 检查文件是否完成写入
                if (IsFileReady(e.FullPath))
                {
                    _fileQueue.Enqueue(e.FullPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理文件变化事件失败: {FileName}", e.Name);
            }
        }

        private void OnFileRenamed(object sender, RenamedEventArgs e)
        {
            if (!_enableRealTimeProcessing)
                return;

            try
            {
                _logger.LogInformation("检测到文件重命名: {OldName} -> {NewName}", e.OldName, e.Name);

                // 只处理MDB文件
                string extension = Path.GetExtension(e.FullPath).ToLower();
                if (extension == ".mdb" || extension == ".accdb")
                {
                    _fileQueue.Enqueue(e.FullPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理文件重命名事件失败: {FileName}", e.Name);
            }
        }

        private void OnWatcherError(object sender, ErrorEventArgs e)
        {
            var ex = e.GetException();
            _logger.LogError(ex, "文件监控错误");

            // 重启监控（如果需要）
            Task.Delay(5000).ContinueWith(_ => RestartMonitoring());
        }

        private void ProcessQueuedFiles(object? sender, System.Timers.ElapsedEventArgs e)
        {
            if (_isProcessing || _cancellationTokenSource?.Token.IsCancellationRequested == true)
                return;

            _isProcessing = true;

            try
            {
                // 处理队列中的所有文件
                while (_fileQueue.TryDequeue(out string? filePath))
                {
                    if (_cancellationTokenSource?.Token.IsCancellationRequested == true || filePath == null)
                        break;

                    ProcessSingleFile(filePath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理队列文件失败");
            }
            finally
            {
                _isProcessing = false;
            }
        }

        private void OnBatchProcessing(object? sender, System.Timers.ElapsedEventArgs e)
        {
            if (_isProcessing || _cancellationTokenSource?.Token.IsCancellationRequested == true)
                return;

            _logger.LogInformation("开始批量处理已有文件...");

            try
            {
                ProcessExistingFiles();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "批量处理失败");
            }
        }

        private void ProcessExistingFiles()
        {
            try
            {
                var files = Directory.GetFiles(_sourceFolder, "*.mdb")
                    .Concat(Directory.GetFiles(_sourceFolder, "*.accdb"));

                int fileCount = 0;
                foreach (var file in files)
                {
                    if (_cancellationTokenSource?.Token.IsCancellationRequested == true)
                        break;

                    // 检查文件是否已处理
                    bool shouldProcess = ShouldProcessFile(file);

                    if (shouldProcess)
                    {
                        _fileQueue.Enqueue(file);
                        fileCount++;
                    }
                }

                _logger.LogInformation("已将 {FileCount} 个现有文件加入处理队列", fileCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理现有文件失败");
            }
        }

        private bool ShouldProcessFile(string filePath)
        {
            try
            {
                var fileInfo = new FileInfo(filePath);

                // 检查文件是否被锁定
                if (IsFileLocked(filePath))
                    return false;

                // 检查文件大小
                if (fileInfo.Length == 0)
                    return false;

                // 检查文件扩展名
                string ext = Path.GetExtension(filePath).ToLower();
                if (ext != ".mdb" && ext != ".accdb")
                    return false;

                // 检查文件修改时间（避免处理太旧的文件）
                DateTime cutoffTime = DateTime.Now.AddHours(-24);
                if (fileInfo.LastWriteTime < cutoffTime)
                {
                    // 可以处理，但记录
                    _logger.LogInformation("处理较旧的文件: {FileName} ({LastWriteTime})",
                        Path.GetFileName(filePath), fileInfo.LastWriteTime.ToString("yyyy-MM-dd HH:mm"));
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        private void ProcessSingleFile(string filePath)
        {
            string fileName = Path.GetFileName(filePath);

            try
            {
                _logger.LogInformation("开始处理文件: {FileName}", fileName);

                // 等待文件准备就绪
                if (!WaitForFileReady(filePath))
                {
                    _logger.LogWarning("文件未准备就绪，跳过处理: {FileName}", fileName);
                    return;
                }

                // 处理文件
                var result = _processor.ProcessFile(filePath);

                // 触发事件
                FileProcessed?.Invoke(this, new FileProcessedEventArgs
                {
                    FilePath = filePath,
                    Result = result,
                    ProcessTime = DateTime.Now
                });

                _logger.LogInformation("文件处理完成: {FileName} - {Status}", fileName, result.Status);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理文件失败: {FileName}", fileName);

                // 触发错误事件
                FileProcessingError?.Invoke(this, new FileProcessingErrorEventArgs
                {
                    FilePath = filePath,
                    Error = ex,
                    ErrorTime = DateTime.Now
                });
            }
        }

        private void ProcessRemainingFiles()
        {
            _logger.LogInformation("处理队列中剩余的文件...");

            int processedCount = 0;
            while (_fileQueue.TryDequeue(out string? filePath))
            {
                if (filePath == null) continue;

                try
                {
                    ProcessSingleFile(filePath);
                    processedCount++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "处理剩余文件失败: {FilePath}", filePath);
                }
            }

            _logger.LogInformation("已处理 {ProcessedCount} 个剩余文件", processedCount);
        }

        private bool IsFileReady(string filePath)
        {
            try
            {
                using (var stream = File.Open(filePath,
                    FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    return true;
                }
            }
            catch (IOException)
            {
                return false;
            }
        }

        private bool WaitForFileReady(string filePath, int maxWaitSeconds = 30)
        {
            DateTime startTime = DateTime.Now;

            while ((DateTime.Now - startTime).TotalSeconds < maxWaitSeconds)
            {
                if (IsFileReady(filePath))
                    return true;

                Thread.Sleep(1000);
            }

            return false;
        }

        private bool IsFileLocked(string filePath)
        {
            try
            {
                using (FileStream stream = File.Open(filePath,
                    FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                    return false;
                }
            }
            catch (IOException)
            {
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "检查文件锁定状态失败: {FilePath}", filePath);
                return true;
            }
        }

        private void LogFileInfo(string filePath, string action)
        {
            try
            {
                var fileInfo = new FileInfo(filePath);
                _logger.LogInformation("文件{Action}: {FileName} (大小: {FileSize}, 修改时间: {LastWriteTime})",
                    action, Path.GetFileName(filePath),
                    FormatFileSize(fileInfo.Length),
                    fileInfo.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss"));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "记录文件信息失败: {FilePath}", filePath);
            }
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

        private void RestartMonitoring()
        {
            if (!_isRunning)
                return;

            try
            {
                _logger.LogInformation("尝试重启文件监控...");

                _watcher.EnableRaisingEvents = false;
                Thread.Sleep(1000);
                _watcher.EnableRaisingEvents = true;

                _logger.LogInformation("文件监控已重启");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "重启文件监控失败");
            }
        }

        public void Dispose()
        {
            Stop();

            _watcher?.Dispose();
            _processingTimer?.Dispose();
            _batchTimer?.Dispose();
            _processor?.Dispose();
            _cancellationTokenSource?.Dispose();

            _logger.LogInformation("FileMonitorService已释放资源");
            GC.SuppressFinalize(this);
        }
    }
}