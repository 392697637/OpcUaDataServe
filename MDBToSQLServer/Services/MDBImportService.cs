using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MDBToSQLServer.Core;
using Quartz;

namespace MDBToSQLServer.Services
{
    public class MDBImportService : IJob, IDisposable
    {
        private readonly System.Timers.Timer? _timer;
        private readonly SmartFileProcessor _processor;
        private  int _intervalMinutes;
        private readonly bool _runAtStartup;
        private readonly bool _stopOnError;
        private readonly ILogger<MDBImportService> _logger;
        private bool _isRunning;
        private int _errorCount;
        private DateTime _lastRunTime;
        private readonly object _lock = new();

        public event EventHandler<ServiceStartedEventArgs>? ServiceStarted;
        public event EventHandler<ServiceStoppedEventArgs>? ServiceStopped;
        public event EventHandler<ImportCompletedEventArgs>? ImportCompleted;
        public event EventHandler<ServiceErrorEventArgs>? ServiceError;

        public class ServiceStartedEventArgs : EventArgs
        {
            public DateTime StartTime { get; set; }
            public int IntervalMinutes { get; set; }
        }

        public class ServiceStoppedEventArgs : EventArgs
        {
            public DateTime StopTime { get; set; }
            public string Reason { get; set; } = string.Empty;
        }

        public class ImportCompletedEventArgs : EventArgs
        {
            public DateTime RunTime { get; set; }
            public ProcessingResult? Result { get; set; }
            public TimeSpan Duration { get; set; }
        }

        public class ServiceErrorEventArgs : EventArgs
        {
            public Exception? Error { get; set; }
            public DateTime ErrorTime { get; set; }
            public string Operation { get; set; } = string.Empty;
        }

        public MDBImportService(
            IConfiguration configuration,
            ILogger<MDBImportService> logger,
            SmartFileProcessor processor)
        {
            var settings = configuration.GetSection("ApplicationSettings");

            _intervalMinutes = int.Parse(settings["ServiceIntervalMinutes"] ?? "60");
            _runAtStartup = bool.Parse(settings["ServiceRunAtStartup"] ?? "true");
            _stopOnError = bool.Parse(settings["ServiceStopOnError"] ?? "false");
            _logger = logger;

            _processor = processor;

            // 初始化定时器（仅用于非Quartz模式）
            _timer = new System.Timers.Timer(_intervalMinutes * 60 * 1000);
            _timer.Elapsed += OnTimerElapsed;
            _timer.AutoReset = true;

            _logger.LogInformation("MDBImportService初始化完成。执行间隔: {IntervalMinutes}分钟", _intervalMinutes);
        }

        // Quartz Job执行方法
        public async Task Execute(IJobExecutionContext context)
        {
            _logger.LogInformation("Quartz定时任务触发，开始执行导入...");
            await ExecuteImportAsync();
        }

        public void Start()
        {
            lock (_lock)
            {
                if (_isRunning)
                    return;

                try
                {
                    _logger.LogInformation("启动MDB导入服务...");

                    _isRunning = true;
                    _errorCount = 0;

                    // 启动定时器（非Quartz模式）
                    _timer?.Start();

                    // 触发服务启动事件
                    ServiceStarted?.Invoke(this, new ServiceStartedEventArgs
                    {
                        StartTime = DateTime.Now,
                        IntervalMinutes = _intervalMinutes
                    });

                    _logger.LogInformation("MDB导入服务已启动");

                    // 是否启动时立即执行
                    if (_runAtStartup)
                    {
                        Task.Run(() => ExecuteImportAsync());
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "启动MDB导入服务失败");
                    _isRunning = false;

                    ServiceError?.Invoke(this, new ServiceErrorEventArgs
                    {
                        Error = ex,
                        ErrorTime = DateTime.Now,
                        Operation = "Start"
                    });

                    throw;
                }
            }
        }

        public void Stop(string reason = "正常停止")
        {
            lock (_lock)
            {
                if (!_isRunning)
                    return;

                try
                {
                    _logger.LogInformation("停止MDB导入服务...");

                    _timer?.Stop();
                    _isRunning = false;

                    // 取消正在进行的处理
                    _processor.CancelProcessing();

                    // 触发服务停止事件
                    ServiceStopped?.Invoke(this, new ServiceStoppedEventArgs
                    {
                        StopTime = DateTime.Now,
                        Reason = reason
                    });

                    _logger.LogInformation("MDB导入服务已停止");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "停止MDB导入服务失败");

                    ServiceError?.Invoke(this, new ServiceErrorEventArgs
                    {
                        Error = ex,
                        ErrorTime = DateTime.Now,
                        Operation = "Stop"
                    });

                    throw;
                }
            }
        }

        private void OnTimerElapsed(object? sender, System.Timers.ElapsedEventArgs e)
        {
            if (!_isRunning)
                return;

            try
            {
                _logger.LogInformation("定时器触发，开始执行导入...");
                Task.Run(() => ExecuteImportAsync());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "定时导入执行失败");

                _errorCount++;

                ServiceError?.Invoke(this, new ServiceErrorEventArgs
                {
                    Error = ex,
                    ErrorTime = DateTime.Now,
                    Operation = "ScheduledImport"
                });

                // 如果配置了错误时停止，并且错误次数超过阈值
                if (_stopOnError && _errorCount >= 3)
                {
                    Stop($"错误次数过多 ({_errorCount})");
                }
            }
        }

        public async Task ExecuteImportAsync()
        {
            await Task.Run(() => ExecuteImport());
        }

        public void ExecuteImport()
        {
            lock (_lock)
            {
                if (!_isRunning)
                {
                    _logger.LogWarning("服务未运行，跳过导入");
                    return;
                }

                DateTime startTime = DateTime.Now;
                _lastRunTime = startTime;

                _logger.LogInformation("开始执行导入，开始时间: {StartTime}", startTime.ToString("yyyy-MM-dd HH:mm:ss"));

                try
                {
                    // 执行导入
                    var result = _processor.ProcessAllFiles();

                    DateTime endTime = DateTime.Now;
                    TimeSpan duration = endTime - startTime;

                    _logger.LogInformation("导入执行完成，结束时间: {EndTime}，耗时: {Duration:F1}分钟",
                        endTime.ToString("yyyy-MM-dd HH:mm:ss"), duration.TotalMinutes);

                    // 触发导入完成事件
                    ImportCompleted?.Invoke(this, new ImportCompletedEventArgs
                    {
                        RunTime = startTime,
                        Result = result,
                        Duration = duration
                    });

                    // 重置错误计数
                    _errorCount = 0;

                    // 记录执行统计
                    RecordExecutionStatistics(startTime, endTime, duration, result);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "执行导入失败");

                    _errorCount++;

                    ServiceError?.Invoke(this, new ServiceErrorEventArgs
                    {
                        Error = ex,
                        ErrorTime = DateTime.Now,
                        Operation = "ExecuteImport"
                    });

                    throw;
                }
            }
        }

        public void ExecuteNow()
        {
            if (!_isRunning)
            {
                _logger.LogWarning("服务未运行，无法执行立即导入");
                return;
            }

            _logger.LogInformation("收到立即执行请求");
            Task.Run(() => ExecuteImport());
        }

        private void RecordExecutionStatistics(DateTime startTime, DateTime endTime,
            TimeSpan duration, ProcessingResult result)
        {
            try
            {
                string statsPath = "logs/service_statistics.csv";
                string statsDir = Path.GetDirectoryName(statsPath) ?? "logs";

                if (!string.IsNullOrEmpty(statsDir) && !Directory.Exists(statsDir))
                {
                    Directory.CreateDirectory(statsDir);
                }

                // 如果文件不存在，创建表头
                if (!File.Exists(statsPath))
                {
                    File.WriteAllText(statsPath,
                        "执行时间,开始时间,结束时间,耗时(秒),总文件数,成功数,部分成功数,失败数,跳过数,结果消息\n");
                }

                // 追加统计记录
                string record = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}," +
                               $"{startTime:yyyy-MM-dd HH:mm:ss}," +
                               $"{endTime:yyyy-MM-dd HH:mm:ss}," +
                               $"{duration.TotalSeconds:F1}," +
                               $"{result.TotalFiles}," +
                               $"{result.SuccessCount}," +
                               $"{result.PartialSuccessCount}," +
                               $"{result.FailedCount}," +
                               $"{result.SkippedCount}," +
                               $"\"{result.Message?.Replace("\"", "\"\"")}\"\n";

                File.AppendAllText(statsPath, record);

                _logger.LogInformation("执行统计已记录: {StatsPath}", statsPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "记录执行统计失败");
            }
        }

        public ServiceStatus GetServiceStatus()
        {
            return new ServiceStatus
            {
                IsRunning = _isRunning,
                LastRunTime = _lastRunTime,
                NextRunTime = _isRunning ? _lastRunTime.AddMinutes(_intervalMinutes) : DateTime.MinValue,
                ErrorCount = _errorCount,
                IntervalMinutes = _intervalMinutes,
                Uptime = _isRunning ? DateTime.Now - _lastRunTime : TimeSpan.Zero
            };
        }

        public class ServiceStatus
        {
            public bool IsRunning { get; set; }
            public DateTime LastRunTime { get; set; }
            public DateTime NextRunTime { get; set; }
            public int ErrorCount { get; set; }
            public int IntervalMinutes { get; set; }
            public TimeSpan Uptime { get; set; }
            public string StatusDescription => IsRunning ? "运行中" : "已停止";
        }

        public void ChangeInterval(int newIntervalMinutes)
        {
            lock (_lock)
            {
                if (newIntervalMinutes < 1)
                {
                    throw new ArgumentException("时间间隔必须大于0分钟");
                }

                _intervalMinutes = newIntervalMinutes;
                _timer!.Interval = newIntervalMinutes * 60 * 1000;

                _logger.LogInformation("服务执行间隔已更改为: {NewIntervalMinutes}分钟", newIntervalMinutes);
            }
        }

        public void Dispose()
        {
            Stop();

            _timer?.Dispose();
            _processor?.Dispose();

            _logger.LogInformation("MDBImportService已释放资源");
            GC.SuppressFinalize(this);
        }
    }
}