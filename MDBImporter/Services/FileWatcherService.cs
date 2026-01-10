// Services/FileWatcherService.cs
using MDBImporter.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MDBImporter.Services
{
    public class FileWatcherService : IDisposable
    {
        private readonly MDBService _mdbService;
        private readonly List<NetworkComputer> _computers;
        private readonly ILogger<FileWatcherService> _logger;
        private readonly LogService _logService;
        private readonly Dictionary<string, FileSystemWatcher> _watchers;
        private readonly Timer _timer;
        private bool _isRunning;

        // 构造函数 - 支持3个参数
        public FileWatcherService(MDBService mdbService, List<NetworkComputer> computers, LogService logService)
        {
            _mdbService = mdbService;
            _computers = computers;
            _logService = logService;
            _watchers = new Dictionary<string, FileSystemWatcher>();

            // 设置定时器，每5分钟检查一次
            _timer = new Timer(CheckForNewFiles, null, Timeout.Infinite, Timeout.Infinite);

            // 创建日志记录器
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });
            _logger = loggerFactory.CreateLogger<FileWatcherService>();

            _logger.LogInformation("FileWatcherService 初始化完成");
        }

        // 构造函数 - 支持2个参数（向后兼容）
        public FileWatcherService(MDBService mdbService, List<NetworkComputer> computers)
            : this(mdbService, computers, null)
        {
        }

        // 启动监控服务
        public void Start()
        {
            if (_isRunning)
            {
                _logger.LogWarning("监控服务已经在运行中");
                return;
            }

            _isRunning = true;

            // 为每个启用的计算机设置文件监控
            foreach (var computer in _computers)
            {
                if (!computer.Enabled)
                {
                    _logger.LogInformation($"跳过已禁用的计算机: {computer.ComputerName}");
                    continue;
                }

                if (!Directory.Exists(computer.MDBFolder))
                {
                    _logger.LogWarning($"文件夹不存在: {computer.MDBFolder}");
                    continue;
                }

                try
                {
                    var watcher = new FileSystemWatcher(computer.MDBFolder)
                    {
                        Filter = "*.mdb;*.accdb",  // 支持两种格式
                        NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.CreationTime,
                        EnableRaisingEvents = true,
                        IncludeSubdirectories = true  // 监控子目录
                    };

                    // 绑定事件处理器
                    watcher.Created += async (sender, e) => await OnFileCreatedAsync(e, computer);
                    watcher.Changed += async (sender, e) => await OnFileChangedAsync(e, computer);
                    watcher.Renamed += async (sender, e) => await OnFileRenamedAsync(e, computer);
                    watcher.Error += OnError;

                    _watchers[computer.ComputerName] = watcher;

                    _logger.LogInformation($"已监控计算机 {computer.ComputerName} 的文件夹: {computer.MDBFolder}");
                    _logService?.Log($"开始监控: {computer.ComputerName} - {computer.MDBFolder}", LogServicegLevel.Info);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"创建文件监控失败: {computer.ComputerName}");
                }
            }

            // 启动定时器（每5分钟执行一次）
            _timer.Change(TimeSpan.Zero, TimeSpan.FromMinutes(5));

            _logger.LogInformation("文件监控服务已启动");
            _logService?.Log("文件监控服务已启动", LogServicegLevel.Info);
        }

        // 停止监控服务
        public void Stop()
        {
            if (!_isRunning)
            {
                _logger.LogWarning("监控服务已经停止");
                return;
            }

            _isRunning = false;
            _timer.Change(Timeout.Infinite, Timeout.Infinite);

            foreach (var kvp in _watchers)
            {
                try
                {
                    kvp.Value.EnableRaisingEvents = false;
                    kvp.Value.Dispose();
                    _logger.LogInformation($"已停止监控计算机: {kvp.Key}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"停止监控失败: {kvp.Key}");
                }
            }
            _watchers.Clear();

            _logger.LogInformation("文件监控服务已停止");
            _logService?.Log("文件监控服务已停止", LogServicegLevel.Info);
        }

        // 文件创建事件
        private async Task OnFileCreatedAsync(FileSystemEventArgs e, NetworkComputer computer)
        {
            try
            {
                _logger.LogInformation($"检测到新文件: {e.FullPath}");
                _logService?.Log($"检测到新文件: {Path.GetFileName(e.FullPath)}", LogServicegLevel.Info);

                // 等待文件完全写入（有些应用程序会分多次写入）
                await Task.Delay(1000);

                await ProcessMDBFileAsync(e.FullPath, computer);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"处理文件创建事件失败: {e.FullPath}");
                _logService?.Log($"处理文件创建失败: {Path.GetFileName(e.FullPath)} - {ex.Message}", LogServicegLevel.Error);
            }
        }

        // 文件修改事件
        private async Task OnFileChangedAsync(FileSystemEventArgs e, NetworkComputer computer)
        {
            // 过滤掉目录变更
            if (Directory.Exists(e.FullPath))
                return;

            // 避免重复处理（某些程序可能多次触发Changed事件）
            await Task.Delay(500);

            try
            {
                _logger.LogInformation($"检测到文件修改: {e.FullPath}");
                _logService?.Log($"检测到文件修改: {Path.GetFileName(e.FullPath)}", LogServicegLevel.Info);

                await ProcessMDBFileAsync(e.FullPath, computer);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"处理文件修改事件失败: {e.FullPath}");
                _logService?.Log($"处理文件修改失败: {Path.GetFileName(e.FullPath)} - {ex.Message}", LogServicegLevel.Error);
            }
        }

        // 文件重命名事件
        private async Task OnFileRenamedAsync(RenamedEventArgs e, NetworkComputer computer)
        {
            try
            {
                _logger.LogInformation($"检测到文件重命名: {e.OldFullPath} -> {e.FullPath}");
                _logService?.Log($"检测到文件重命名: {Path.GetFileName(e.OldFullPath)} -> {Path.GetFileName(e.FullPath)}", LogServicegLevel.Info);

                await ProcessMDBFileAsync(e.FullPath, computer);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"处理文件重命名事件失败: {e.FullPath}");
                _logService?.Log($"处理文件重命名失败: {Path.GetFileName(e.FullPath)} - {ex.Message}", LogServicegLevel.Error);
            }
        }

        // 错误事件
        private void OnError(object sender, ErrorEventArgs e)
        {
            _logger.LogError(e.GetException(), "文件监控服务发生错误");
            _logService?.Log($"文件监控错误: {e.GetException()?.Message}", LogServicegLevel.Error);
        }

        // 定时检查新文件
        private async void CheckForNewFiles(object state)
        {
            if (!_isRunning) return;

            _logger.LogDebug("定时检查新文件开始");

            foreach (var computer in _computers)
            {
                if (!computer.Enabled || !Directory.Exists(computer.MDBFolder))
                    continue;

                try
                {
                    // 查找所有MDB和ACCDB文件
                    var mdbFiles = Directory.GetFiles(computer.MDBFolder, "*.mdb", SearchOption.AllDirectories)
                        .Concat(Directory.GetFiles(computer.MDBFolder, "*.accdb", SearchOption.AllDirectories));

                    foreach (var file in mdbFiles)
                    {
                        // 检查文件是否需要处理（例如，根据修改时间）
                        var fileInfo = new FileInfo(file);
                        if (computer.LastSyncTime == null || fileInfo.LastWriteTime > computer.LastSyncTime)
                        {
                            _logger.LogInformation($"定时检查发现新文件: {file}");
                            await ProcessMDBFileAsync(file, computer);
                            computer.LastSyncTime = DateTime.Now;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"定时检查文件失败 {computer.ComputerName}: {ex.Message}");
                    _logService?.Log($"定时检查失败 {computer.ComputerName}: {ex.Message}", LogServicegLevel.Error);
                }
            }

            _logger.LogDebug("定时检查新文件完成");
        }

        // 处理MDB文件
        private async Task ProcessMDBFileAsync(string filePath, NetworkComputer computer)
        {
            try
            {
                _logger.LogInformation($"开始处理文件: {Path.GetFileName(filePath)}");
                _logService?.Log($"开始处理文件: {Path.GetFileName(filePath)}", LogServicegLevel.Info);

                // 检查文件是否被锁定
                if (IsFileLocked(filePath))
                {
                    _logger.LogWarning($"文件被锁定，稍后重试: {filePath}");
                    _logService?.Log($"文件被锁定: {Path.GetFileName(filePath)}", LogServicegLevel.Warning);
                    return;
                }

                // 测试文件连接
                if (!_mdbService.TestMDBConnection(filePath))
                {
                    _logger.LogError($"无法连接文件: {filePath}");
                    _logService?.Log($"无法连接文件: {Path.GetFileName(filePath)}", LogServicegLevel.Error);
                    return;
                }

                // 获取所有表
                var tables = _mdbService.GetTablesFromMDB(filePath);
                var fileNameWithoutExt = Path.GetFileNameWithoutExtension(filePath);

                if (tables.Count == 0)
                {
                    _logger.LogWarning($"文件中没有找到表: {filePath}");
                    _logService?.Log($"文件中没有找到表: {Path.GetFileName(filePath)}", LogServicegLevel.Warning);
                    return;
                }

                _logger.LogInformation($"文件包含 {tables.Count} 个表");

                // 为每个表导入数据
                foreach (var table in tables)
                {
                    var sqlTableName = $"{computer.ComputerName}_{table}"
                        .Replace(" ", "_")
                        .Replace("-", "_");

                    _logger.LogInformation($"导入表: {table} -> {sqlTableName}");

                    var records = await _mdbService.ImportDataFromMDBAsync(filePath, table, sqlTableName, computer.ComputerName);

                    _logService?.Log($"导入完成: {Path.GetFileName(filePath)}.{table} -> {records} 条记录",
                        records > 0 ? LogServicegLevel.Info : LogServicegLevel.Warning);
                }

                computer.LastSyncTime = DateTime.Now;
                _logger.LogInformation($"文件处理完成: {Path.GetFileName(filePath)}");
                _logService?.Log($"文件处理完成: {Path.GetFileName(filePath)}", LogServicegLevel.Info);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"处理文件失败 {filePath}");
                _logService?.Log($"处理文件失败 {Path.GetFileName(filePath)}: {ex.Message}", LogServicegLevel.Error);
            }
        }

        // 检查文件是否被锁定
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
                _logger.LogError(ex, $"检查文件锁定状态失败: {filePath}");
                return true;
            }
        }

        // 获取服务状态
        public bool IsRunning => _isRunning;

        // 获取监控的计算机数量
        public int GetMonitoredComputerCount()
        {
            return _watchers.Count;
        }

        // 获取监控的文件夹列表
        public List<string> GetMonitoredFolders()
        {
            var folders = new List<string>();
            foreach (var computer in _computers.Where(c => c.Enabled))
            {
                folders.Add(computer.MDBFolder);
            }
            return folders;
        }

        // 手动触发文件检查
        public async Task ManualCheckAsync()
        {
            _logger.LogInformation("手动触发文件检查");
            await Task.Run(() => CheckForNewFiles(null));
        }

        public void Dispose()
        {
            Stop();
            _timer?.Dispose();

            foreach (var watcher in _watchers.Values)
            {
                watcher?.Dispose();
            }

            _logger.LogInformation("FileWatcherService 已释放");
            GC.SuppressFinalize(this);
        }
    }
}