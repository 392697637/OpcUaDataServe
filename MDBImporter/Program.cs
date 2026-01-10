// Program.cs
using MDBImporter.Core;
using MDBImporter.Helpers;
using MDBImporter.Models;
using MDBImporter.Services;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MDBImporter
{
    class Program
    {
        private static IServiceProvider _serviceProvider;
        private static ILogger<Program> _logger;
        private static ConfigHelper _configHelper;
        private static SqlServerService _sqlServerService;
        private static MDBService _mdbService;
        private static FileWatcherService _fileWatcherService;
        private static LogService _logService;
        private static List<NetworkComputer> _networkComputers;
        private static bool _isRunning = true;
        private static bool _isMonitoring = false;

        static async Task Main(string[] args)
        {
            Console.Title = "MDB数据导入系统 v1.0";
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("==========================================");
            Console.WriteLine("         MDB数据导入系统 v1.0");
            Console.WriteLine("==========================================");
            Console.ResetColor();
            Console.WriteLine("正在初始化系统...\n");

            try
            {
                // 设置依赖注入
                SetupDependencyInjection();

                // 初始化服务
                await InitializeServicesAsync();

                // 显示主菜单
                await ShowMainMenuAsync();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"系统初始化失败: {ex.Message}");
                Console.ResetColor();
                _logger?.LogError(ex, "系统初始化失败");
            }
            finally
            {
                DisposeServices();
                Console.WriteLine("\n程序已退出。");
            }
        }

        //// 设置依赖注入
        private static void SetupDependencyInjection()
        {
            var services = new ServiceCollection();

            // 配置
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            services.AddSingleton<IConfiguration>(configuration);

            // 日志 - 使用自定义格式
            services.AddLogging(builder =>
            {
                builder.ClearProviders(); // 清除默认提供程序
                builder.AddSimpleConsole(options =>
                {
                    options.TimestampFormat = "HH:mm:ss ";
                    options.UseUtcTimestamp = false;
                    options.IncludeScopes = false;
                    options.SingleLine = true;
                    options.ColorBehavior = LoggerColorBehavior.Enabled;
                });
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddFilter("Microsoft", LogLevel.Warning);
                builder.AddFilter("System", LogLevel.Warning);
            });

            // 注册服务
            services.AddSingleton<ConfigHelper>();
            services.AddSingleton<ProviderHelper>();
            services.AddSingleton<SqlServerService>();
            services.AddSingleton<MDBService>();
            services.AddSingleton<LogService>();
            services.AddSingleton<FileWatcherService>();
            services.AddSingleton<Program>();

            _serviceProvider = services.BuildServiceProvider();
        }

        // 初始化服务
        private static async Task InitializeServicesAsync()
        {
            try
            {
                // 获取日志服务
                var loggerFactory = _serviceProvider.GetService<ILoggerFactory>();
                var logger = loggerFactory?.CreateLogger<Program>();

                if (logger != null)
                {
                    _logger = logger;
                }
                else
                {
                    // 备用日志创建
                    var consoleLoggerFactory = LoggerFactory.Create(builder =>
                    {
                        builder.AddConsole();
                    });
                    _logger = consoleLoggerFactory.CreateLogger<Program>();
                }

                Console.WriteLine("正在加载配置...");

                // 获取ConfigHelper
                _configHelper = _serviceProvider.GetRequiredService<ConfigHelper>();

                // 获取网络计算机配置
                _networkComputers = _configHelper.GetNetworkComputers();
                Console.WriteLine($"已加载 {_networkComputers.Count} 个计算机配置");

                // 获取SQL Server服务
                _sqlServerService = _serviceProvider.GetRequiredService<SqlServerService>();

                // 获取LogService
                _logService = _serviceProvider.GetRequiredService<LogService>();

                // 获取MDBService
                _mdbService = _serviceProvider.GetRequiredService<MDBService>();

                // 测试数据库连接
                Console.Write("测试SQL Server连接... ");
                var sqlConnected = await _sqlServerService.TestConnectionAsync();
                if (sqlConnected)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("成功");
                    Console.ResetColor();
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("失败（部分功能可能不可用）");
                    Console.ResetColor();
                }

                // 创建导入历史表
                try
                {
                    await _sqlServerService.CreateImportHistoryTableAsync();
                    Console.WriteLine("导入历史表检查完成");
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"创建历史表失败: {ex.Message}");
                    Console.ResetColor();
                }

                // 检查Provider
                var providerHelper = _serviceProvider.GetRequiredService<ProviderHelper>();
                providerHelper.CheckSystemRequirements();

                // 初始化文件监控服务
                _fileWatcherService = new FileWatcherService(_mdbService, _networkComputers, _logService);

                _logger.LogInformation("系统初始化完成");
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("\n系统初始化完成！");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"系统初始化失败: {ex.Message}");
                Console.WriteLine($"详细错误: {ex.StackTrace}");
                Console.ResetColor();
                throw;
            }
        }

        // 清理服务
        private static void DisposeServices()
        {
            if (_isMonitoring)
            {
                _fileWatcherService.Stop();
                _isMonitoring = false;
            }

            _fileWatcherService?.Dispose();
            (_serviceProvider as IDisposable)?.Dispose();
        }

        // 显示主菜单
        private static async Task ShowMainMenuAsync()
        {
            while (_isRunning)
            {
                Console.WriteLine("\n" + new string('=', 50));
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("                    主菜单");
                Console.ResetColor();
                Console.WriteLine(new string('=', 50));
                Console.WriteLine("1. 创建数据表获取全部数据（一次性导入）");
                Console.WriteLine("2. 删除数据表（一次性删除）");
                Console.WriteLine("3. 表统计和导入时间");
                Console.WriteLine("4. 同步上次到现在所有数据（手动同步）");
                Console.WriteLine("5. 文件数据自动定时入库（监控服务）");
                Console.WriteLine(new string('-', 50));
                Console.WriteLine("6. 清理日志文件");
                Console.WriteLine("7. 测试网络计算机连接MDB文件信息");
                Console.WriteLine("8. 测试数据库连接");
                Console.WriteLine("9. 系统配置检查");
                Console.WriteLine("10. 显示最近日志");
                Console.WriteLine("0. 退出程序");
                Console.WriteLine(new string('=', 50));
                Console.Write("请选择操作 (0-10): ");

                var choice = Console.ReadLine();
                Console.WriteLine();

                switch (choice)
                {
                    case "1":
                        await ImportAllDataAsync();
                        break;
                    case "2":
                        await DeleteAllTablesAsync();
                        break;
                    case "3":
                        await ShowTableStatisticsAsync();
                        break;
                    case "4":
                        await SyncNewDataAsync();
                        break;
                    case "5":
                        ToggleFileWatcherService();
                        break;
                    case "6":
                        CleanLogs();
                        break;
                    case "7":
                        TestMDBConnections();
                        break;
                    case "8":
                        await TestDatabaseConnectionAsync();
                        break;
                    case "9":
                        CheckSystemConfig();
                        break;
                    case "10":
                        ShowRecentLogs();
                        break;
                    case "0":
                        ExitProgram();
                        break;
                    default:
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("无效的选择，请重新输入");
                        Console.ResetColor();
                        break;
                }

                //Console.WriteLine("\n按任意键继续...");
                //Console.ReadKey();
            }
        }

        // 1. 创建数据表获取全部数据
        private static async Task ImportAllDataAsync()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("=== 导入全部数据 ===");
            Console.ResetColor();

            _logService.Log("开始导入全部数据", LogServicegLevel.Info);

            var totalRecords = 0;
            var totalFiles = 0;
            var totalTables = 0;

            // 显示要处理的计算机
            var enabledComputers = _networkComputers.Where(c => c.Enabled).ToList();
            Console.WriteLine($"将处理 {enabledComputers.Count} 个启用的计算机\n");

            foreach (var computer in enabledComputers)
            {
                Console.WriteLine($"处理计算机: {computer.ComputerName}");
                Console.WriteLine($"文件夹: {computer.MDBFolder}");

                if (!Directory.Exists(computer.MDBFolder))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"  错误: 文件夹不存在");
                    Console.ResetColor();
                    _logService.Log($"文件夹不存在: {computer.MDBFolder}", LogServicegLevel.Error);
                    continue;
                }

                // 查找所有MDB/ACCDB文件
                var mdbFiles = Directory.GetFiles(computer.MDBFolder, "*.mdb", SearchOption.AllDirectories)
                    .Concat(Directory.GetFiles(computer.MDBFolder, "*.accdb", SearchOption.AllDirectories))
                    .ToList();

                if (mdbFiles.Count == 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"  警告: 没有找到MDB/ACCDB文件");
                    Console.ResetColor();
                    continue;
                }

                Console.WriteLine($"  找到 {mdbFiles.Count} 个数据库文件\n");

                foreach (var file in mdbFiles)
                {
                    totalFiles++;
                    Console.WriteLine($"  [{totalFiles}] 处理文件: {Path.GetFileName(file)}");
                    Console.WriteLine($"      路径: {file}");

                    // 测试文件连接
                    if (!_mdbService.TestMDBConnection(file))
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"      错误: 无法连接文件");
                        Console.ResetColor();
                        continue;
                    }

                    var tables = _mdbService.GetTablesFromMDB(file);
                    var fileNameWithoutExt = Path.GetFileNameWithoutExtension(file);

                    if (tables.Count == 0)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"      警告: 文件中没有找到表");
                        Console.ResetColor();
                        continue;
                    }

                    Console.WriteLine($"      包含 {tables.Count} 个表");

                    foreach (var table in tables)
                    {
                        totalTables++;
                        var sqlTableName = $"{computer.ComputerName}_{table}"
                            .Replace(" ", "_")
                            .Replace("-", "_");

                        Console.Write($"      导入表 [{totalTables}]: {table} -> {sqlTableName}... ");

                        var records = await _mdbService.ImportDataFromMDBAsync(
                            file, table, sqlTableName, computer.ComputerName);

                        totalRecords += records;

                        if (records > 0)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"成功 ({records} 条记录)");
                            Console.ResetColor();
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine("完成 (0 条记录)");
                            Console.ResetColor();
                        }
                    }
                    Console.WriteLine();
                }
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("\n==========================================");
            Console.WriteLine($"全部数据导入完成！");
            Console.WriteLine($"  处理计算机: {enabledComputers.Count} 个");
            Console.WriteLine($"  处理文件: {totalFiles} 个");
            Console.WriteLine($"  处理表: {totalTables} 个");
            Console.WriteLine($"  总记录数: {totalRecords} 条");
            Console.WriteLine("==========================================");
            Console.ResetColor();

            _logService.Log($"全部数据导入完成: {totalFiles}文件, {totalTables}表, {totalRecords}记录", LogServicegLevel.Info);
        }

        // 2. 删除数据表
        private static async Task DeleteAllTablesAsync()
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("=== 删除所有数据表 ===");
            Console.ResetColor();

            Console.Write("确认删除所有导入的表吗？(y/n): ");
            var confirm = Console.ReadLine();

            if (confirm?.ToLower() != "y")
            {
                Console.WriteLine("操作已取消");
                return;
            }

            _logService.Log("开始删除所有数据表", LogServicegLevel.Warning);

            try
            {
                var sqlConnectionString = _configHelper.GetSqlServerConnectionString();
                using var connection = new SqlConnection(sqlConnectionString);
                await connection.OpenAsync();

                // 获取所有用户创建的表（排除系统表和ImportHistory）
                var sql = @"
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE' 
                    AND TABLE_NAME NOT IN ('ImportHistory')";

                using var command = new SqlCommand(sql, connection);
                using var reader = await command.ExecuteReaderAsync();

                var tablesToDelete = new List<string>();

                while (await reader.ReadAsync())
                {
                    tablesToDelete.Add(reader["TABLE_NAME"].ToString());
                }
                await reader.CloseAsync();

                if (tablesToDelete.Count == 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("没有找到要删除的表");
                    Console.ResetColor();
                    return;
                }

                Console.WriteLine($"找到 {tablesToDelete.Count} 个表需要删除:");
                foreach (var table in tablesToDelete)
                {
                    Console.WriteLine($"  - {table}");
                }

                Console.Write("\n确认删除以上所有表吗？(y/n): ");
                confirm = Console.ReadLine();

                if (confirm?.ToLower() != "y")
                {
                    Console.WriteLine("操作已取消");
                    return;
                }

                // 删除表
                int deletedCount = 0;
                foreach (var table in tablesToDelete)
                {
                    try
                    {
                        var dropSql = $"DROP TABLE [{table}]";
                        using var dropCommand = new SqlCommand(dropSql, connection);
                        await dropCommand.ExecuteNonQueryAsync();
                        Console.WriteLine($"  已删除表: {table}");
                        deletedCount++;
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"  删除表失败 {table}: {ex.Message}");
                        Console.ResetColor();
                    }
                }

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"\n删除完成，共删除 {deletedCount} 个表");
                Console.ResetColor();

                _logService.Log($"删除完成，共删除 {deletedCount} 个表", LogServicegLevel.Info);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"删除表失败: {ex.Message}");
                Console.ResetColor();
                _logService.Log($"删除表失败: {ex.Message}", LogServicegLevel.Error);
            }
        }

        // 3. 表统计和导入时间
        private static async Task ShowTableStatisticsAsync()
        {
            try
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("=== 表统计信息 ===");
                Console.ResetColor();

                // 获取表统计信息
                var stats = await _sqlServerService.GetTableStatisticsAsync();

                if (stats.Rows.Count == 0)
                {
                    Console.WriteLine("没有找到表数据");
                    return;
                }

                Console.WriteLine($"{"表名",-40} {"记录数",-12} {"总空间(KB)",-12} {"已用空间(KB)",-12}");
                Console.WriteLine(new string('-', 80));

                long totalRecords = 0;
                long totalSpace = 0;
                long usedSpace = 0;

                foreach (DataRow row in stats.Rows)
                {
                    var tableName = row["TableName"].ToString();
                    var rowCounts = Convert.ToInt64(row["RowCounts"]);
                    var totalSpaceKB = Convert.ToInt64(row["TotalSpaceKB"]);
                    var usedSpaceKB = Convert.ToInt64(row["UsedSpaceKB"]);

                    Console.WriteLine($"{tableName,-40} {rowCounts,-12:N0} {totalSpaceKB,-12:N0} {usedSpaceKB,-12:N0}");

                    totalRecords += rowCounts;
                    totalSpace += totalSpaceKB;
                    usedSpace += usedSpaceKB;
                }

                Console.WriteLine(new string('-', 80));
                Console.WriteLine($"{stats.Rows.Count.ToString(),-40} {totalRecords,-12:N0} {totalSpace,-12:N0} {usedSpace,-12:N0}");
                Console.WriteLine($"总计: {stats.Rows.Count} 个表, {totalRecords:N0} 条记录");

                // 获取最近导入历史
                Console.WriteLine("\n" + new string('=', 80));
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("最近导入历史");
                Console.ResetColor();

                var historyTable = await GetRecentImportHistoryAsync();

                if (historyTable.Rows.Count == 0)
                {
                    Console.WriteLine("没有导入历史记录");
                    return;
                }

                Console.WriteLine($"{"计算机",-15} {"表名",-30} {"导入时间",-20} {"记录数",-10} {"状态",-10}");
                Console.WriteLine(new string('-', 95));

                int successCount = 0;
                int failCount = 0;
                long totalImported = 0;

                foreach (DataRow row in historyTable.Rows)
                {
                    var computerName = row["ComputerName"].ToString();
                    var tableName = row["TableName"].ToString();
                    var importTime = Convert.ToDateTime(row["ImportTime"]).ToString("yyyy-MM-dd HH:mm:ss");
                    var recordsImported = Convert.ToInt32(row["RecordsImported"]);
                    var status = row["Status"].ToString();

                    if (status == "Success")
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                        successCount++;
                        totalImported += recordsImported;
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        failCount++;
                    }

                    Console.WriteLine($"{computerName,-15} {tableName,-30} {importTime,-20} {recordsImported,-10} {status,-10}");
                    Console.ResetColor();
                }

                Console.WriteLine(new string('-', 95));
                Console.WriteLine($"统计: 成功 {successCount} 次, 失败 {failCount} 次, 总计导入 {totalImported:N0} 条记录");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"获取统计信息失败: {ex.Message}");
                Console.ResetColor();
                _logService.Log($"获取统计信息失败: {ex.Message}", LogServicegLevel.Error);
            }
        }

        // 获取最近导入历史
        private static async Task<DataTable> GetRecentImportHistoryAsync()
        {
            var sqlConnectionString = _configHelper.GetSqlServerConnectionString();
            using var connection = new SqlConnection(sqlConnectionString);

            var historySql = @"
                SELECT TOP 20 
                    ComputerName, TableName, ImportTime, RecordsImported, Status, ErrorMessage
                FROM ImportHistory
                ORDER BY ImportTime DESC";

            using var command = new SqlCommand(historySql, connection);
            using var adapter = new SqlDataAdapter(command);
            var historyTable = new DataTable();

            await connection.OpenAsync();
            adapter.Fill(historyTable);

            return historyTable;
        }

        // 4. 同步上次到现在所有数据
        private static async Task SyncNewDataAsync()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("=== 手动同步新数据 ===");
            Console.ResetColor();

            _logService.Log("开始手动同步新数据", LogServicegLevel.Info);

            var totalRecords = 0;
            var totalFiles = 0;

            foreach (var computer in _networkComputers)
            {
                if (!computer.Enabled)
                    continue;

                Console.WriteLine($"同步计算机: {computer.ComputerName}");

                if (!Directory.Exists(computer.MDBFolder))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"  错误: 文件夹不存在");
                    Console.ResetColor();
                    continue;
                }

                var mdbFiles = Directory.GetFiles(computer.MDBFolder, "*.mdb", SearchOption.AllDirectories)
                    .Concat(Directory.GetFiles(computer.MDBFolder, "*.accdb", SearchOption.AllDirectories));

                foreach (var file in mdbFiles)
                {
                    var fileInfo = new FileInfo(file);

                    // 只处理自上次同步后修改过的文件
                    if (computer.LastSyncTime == null || fileInfo.LastWriteTime > computer.LastSyncTime)
                    {
                        totalFiles++;
                        Console.WriteLine($"  同步文件 [{totalFiles}]: {Path.GetFileName(file)}");
                        Console.WriteLine($"    修改时间: {fileInfo.LastWriteTime:yyyy-MM-dd HH:mm:ss}");

                        // 测试文件连接
                        if (!_mdbService.TestMDBConnection(file))
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"    错误: 无法连接文件");
                            Console.ResetColor();
                            continue;
                        }

                        var tables = _mdbService.GetTablesFromMDB(file);
                        var fileNameWithoutExt = Path.GetFileNameWithoutExtension(file);

                        if (tables.Count == 0)
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine($"    警告: 文件中没有找到表");
                            Console.ResetColor();
                            continue;
                        }

                        foreach (var table in tables)
                        {
                            var sqlTableName = $"{computer.ComputerName}_{table}"
                                .Replace(" ", "_")
                                .Replace("-", "_");

                            Console.Write($"    导入表: {table}... ");

                            var records = await _mdbService.ImportDataFromMDBAsync(
                                file, table, sqlTableName, computer.ComputerName);

                            totalRecords += records;

                            if (records > 0)
                            {
                                Console.ForegroundColor = ConsoleColor.Green;
                                Console.WriteLine($"成功 ({records} 条记录)");
                                Console.ResetColor();
                            }
                            else
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine("完成 (0 条记录)");
                                Console.ResetColor();
                            }
                        }

                        computer.LastSyncTime = DateTime.Now;
                    }
                    else
                    {
                        Console.WriteLine($"  跳过未修改文件: {Path.GetFileName(file)}");
                    }
                }
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"\n同步完成，共处理 {totalFiles} 个文件，导入 {totalRecords} 条新记录");
            Console.ResetColor();

            _logService.Log($"手动同步完成: {totalFiles}文件, {totalRecords}记录", LogServicegLevel.Info);
        }

        // 5. 切换文件监控服务
        private static void ToggleFileWatcherService()
        {
            if (!_isMonitoring)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("=== 启动文件监控服务 ===");
                Console.ResetColor();

                Console.WriteLine("监控的计算机:");
                foreach (var computer in _networkComputers.Where(c => c.Enabled))
                {
                    Console.WriteLine($"  {computer.ComputerName}: {computer.MDBFolder}");
                }

                Console.Write("\n确认启动监控服务吗？(y/n): ");
                var confirm = Console.ReadLine();

                if (confirm?.ToLower() == "y")
                {
                    _fileWatcherService.Start();
                    _isMonitoring = true;
                    _logService.Log("文件监控服务已启动", LogServicegLevel.Info);

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("\n文件监控服务已启动！");
                    Console.WriteLine("服务将在后台运行，自动监控文件变化并导入数据。");
                    Console.WriteLine("要停止服务，请再次选择此选项。");
                    Console.ResetColor();
                }
                else
                {
                    Console.WriteLine("操作已取消");
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("=== 停止文件监控服务 ===");
                Console.ResetColor();

                Console.Write("确认停止监控服务吗？(y/n): ");
                var confirm = Console.ReadLine();

                if (confirm?.ToLower() == "y")
                {
                    _fileWatcherService.Stop();
                    _isMonitoring = false;
                    _logService.Log("文件监控服务已停止", LogServicegLevel.Info);

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("文件监控服务已停止");
                    Console.ResetColor();
                }
                else
                {
                    Console.WriteLine("操作已取消");
                }
            }
        }

        // 6. 清理日志文件
        private static void CleanLogs()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("=== 清理日志文件 ===");
            Console.ResetColor();

            Console.Write("请输入要保留的天数 (默认30天): ");
            var input = Console.ReadLine();

            if (!int.TryParse(input, out int keepDays) || keepDays < 1)
            {
                keepDays = 30;
            }

            _logService.CleanLogs(keepDays);
        }

        // 7. 测试网络计算机连接MDB文件信息
        private static void TestMDBConnections()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("=== 测试MDB文件连接 ===");
            Console.ResetColor();

            var providerHelper = _serviceProvider.GetRequiredService<ProviderHelper>();
            var availableProviders = providerHelper.GetAvailableProviders();

            Console.WriteLine("\n系统信息:");
            Console.WriteLine($"  系统架构: {providerHelper.GetSystemArchitecture()}");
            Console.WriteLine($"  .NET版本: {Environment.Version}");

            Console.WriteLine("\n可用的OLEDB提供程序:");
            foreach (var provider in availableProviders)
            {
                if (provider.IsAvailable)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"  ✓ {provider.ProviderName}: {provider.Description}");
                    Console.ResetColor();
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"  ✗ {provider.ProviderName}: {provider.Description}");
                    Console.ResetColor();
                }
            }

            Console.WriteLine("\n" + new string('=', 60));

            foreach (var computer in _networkComputers)
            {
                if (!computer.Enabled)
                {
                    Console.ForegroundColor = ConsoleColor.DarkGray;
                    Console.WriteLine($"[{computer.ComputerName}] 已禁用");
                    Console.ResetColor();
                    continue;
                }

                Console.WriteLine($"\n计算机: {computer.ComputerName} ({computer.IPAddress})");
                Console.WriteLine($"描述: {computer.Description}");
                Console.WriteLine($"文件夹: {computer.MDBFolder}");

                if (!Directory.Exists(computer.MDBFolder))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("  ✗ 文件夹不存在");
                    Console.ResetColor();
                    continue;
                }

                var mdbFiles = Directory.GetFiles(computer.MDBFolder, "*.mdb", SearchOption.AllDirectories)
                    .Concat(Directory.GetFiles(computer.MDBFolder, "*.accdb", SearchOption.AllDirectories))
                    .ToList();

                if (mdbFiles.Count == 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("  ⓘ 没有找到MDB/ACCDB文件");
                    Console.ResetColor();
                    continue;
                }

                Console.WriteLine($"  找到 {mdbFiles.Count} 个数据库文件:\n");

                foreach (var file in mdbFiles)
                {
                    var fileInfo = new FileInfo(file);
                    var isConnected = _mdbService.TestMDBConnection(file);
                    var tables = _mdbService.GetTablesFromMDB(file);
                    var fileType = providerHelper.GetFileTypeDescription(file);

                    if (isConnected)
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"  ✓ {Path.GetFileName(file)}");
                        Console.ResetColor();
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"  ✗ {Path.GetFileName(file)}");
                        Console.ResetColor();
                    }

                    Console.WriteLine($"      类型: {fileType}");
                    Console.WriteLine($"      大小: {FormatFileSize(fileInfo.Length)}");
                    Console.WriteLine($"      修改时间: {fileInfo.LastWriteTime:yyyy-MM-dd HH:mm:ss}");
                    Console.WriteLine($"      包含表: {tables.Count} 个");

                    if (tables.Count > 0)
                    {
                        Console.WriteLine($"      前5个表: {string.Join(", ", tables.Take(5))}");
                        if (tables.Count > 5)
                            Console.WriteLine($"      ... 还有 {tables.Count - 5} 个表");
                    }

                    // 显示文件属性
                    var properties = providerHelper.GetFileProperties(file);
                    if (properties.Count > 0)
                    {
                        Console.WriteLine($"      状态: {properties.FirstOrDefault(p => p.StartsWith("状态:"))?.Replace("状态: ", "")}");
                    }

                    Console.WriteLine();
                }
            }
        }

        // 格式化文件大小
        private static string FormatFileSize(long bytes)
        {
            string[] sizes = ["B", "KB", "MB", "GB", "TB"];
            double len = bytes;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len /= 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }

        // 8. 测试数据库连接
        private static async Task TestDatabaseConnectionAsync()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("=== 测试数据库连接 ===");
            Console.ResetColor();

            Console.WriteLine("正在测试数据库连接...");

            var isConnected = await _sqlServerService.TestConnectionAsync();

            if (isConnected)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("✓ 数据库连接成功");
                Console.ResetColor();

                // 测试查询
                try
                {
                    var sqlConnectionString = _configHelper.GetSqlServerConnectionString();
                    using var connection = new SqlConnection(sqlConnectionString);
                    await connection.OpenAsync();

                    using var command = new SqlCommand("SELECT @@VERSION", connection);
                    var version = await command.ExecuteScalarAsync() as string;

                    if (!string.IsNullOrEmpty(version))
                    {
                        Console.WriteLine("数据库版本:");
                        Console.WriteLine(version);
                    }

                    // 检查表数量
                    using var command2 = new SqlCommand(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'",
                        connection);
                    var tableCount = await command2.ExecuteScalarAsync();
                    Console.WriteLine($"用户表数量: {tableCount}");
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"查询测试失败: {ex.Message}");
                    Console.ResetColor();
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("✗ 数据库连接失败");
                Console.ResetColor();
            }
        }

        // 9. 系统配置检查
        private static void CheckSystemConfig()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("=== 系统配置检查 ===");
            Console.ResetColor();

            Console.WriteLine("\n1. 应用程序配置:");
            Console.WriteLine($"   工作目录: {Environment.CurrentDirectory}");
            Console.WriteLine($"   配置文件: {Path.Combine(Environment.CurrentDirectory, "appsettings.json")}");

            // 检查SQL Server连接字符串
            try
            {
                var sqlConnString = _configHelper.GetSqlServerConnectionString();
                Console.WriteLine("   ✓ SQL Server连接字符串配置正确");

                // 隐藏密码显示
                var safeConnStr = sqlConnString;
                var pwdIndex = safeConnStr.IndexOf("pwd=", StringComparison.OrdinalIgnoreCase);
                if (pwdIndex > 0)
                {
                    var endIndex = safeConnStr.IndexOf(';', pwdIndex);
                    if (endIndex > 0)
                    {
                        safeConnStr = safeConnStr.Substring(0, pwdIndex + 4) + "*****" + safeConnStr.Substring(endIndex);
                    }
                }
                Console.WriteLine($"   连接字符串: {safeConnStr}");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"   ✗ SQL Server连接字符串配置错误: {ex.Message}");
                Console.ResetColor();
            }

            // 检查网络计算机配置
            Console.WriteLine($"\n2. 网络计算机配置: {_networkComputers.Count} 个");

            int enabledCount = 0;
            foreach (var computer in _networkComputers)
            {
                var status = computer.Enabled ? "✓ 启用" : "✗ 禁用";
                var color = computer.Enabled ? ConsoleColor.Green : ConsoleColor.Red;

                Console.ForegroundColor = color;
                Console.WriteLine($"   {status} {computer.ComputerName} ({computer.IPAddress})");
                Console.ResetColor();
                Console.WriteLine($"       文件夹: {computer.MDBFolder}");
                Console.WriteLine($"       描述: {computer.Description}");
                Console.WriteLine($"       同步间隔: {computer.SyncIntervalMinutes} 分钟");
                Console.WriteLine($"       最后同步: {computer.LastSyncTime?.ToString("yyyy-MM-dd HH:mm:ss") ?? "从未同步"}");

                if (computer.Enabled) enabledCount++;
            }

            Console.WriteLine($"   启用的计算机: {enabledCount}/{_networkComputers.Count}");

            // 检查日志目录
            var logDir = Path.Combine(Environment.CurrentDirectory, "Logs");
            Console.WriteLine($"\n3. 日志配置:");
            if (Directory.Exists(logDir))
            {
                Console.WriteLine($"   ✓ 日志目录存在: {logDir}");
                var logFiles = Directory.GetFiles(logDir, "*.txt");
                Console.WriteLine($"       日志文件数: {logFiles.Length}");

                if (logFiles.Length > 0)
                {
                    var totalSize = logFiles.Sum(f => new FileInfo(f).Length);
                    Console.WriteLine($"       总大小: {FormatFileSize(totalSize)}");

                    var todayLog = Path.Combine(logDir, $"log_{DateTime.Now:yyyyMMdd}.txt");
                    if (File.Exists(todayLog))
                    {
                        var lines = File.ReadAllLines(todayLog);
                        Console.WriteLine($"       今日日志行数: {lines.Length}");
                    }
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"   ⓘ 日志目录不存在，将在需要时创建");
                Console.ResetColor();
            }

            // 检查系统要求
            Console.WriteLine($"\n4. 系统要求检查:");
            var providerHelper = _serviceProvider.GetRequiredService<ProviderHelper>();

            Console.WriteLine($"   系统架构: {providerHelper.GetSystemArchitecture()}");
            Console.WriteLine($"   .NET版本: {Environment.Version}");

            var providers = providerHelper.GetAvailableProviders();
            bool hasAvailableProvider = providers.Any(p => p.IsAvailable);

            if (hasAvailableProvider)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("   ✓ 有可用的OLEDB提供程序");
                Console.ResetColor();
                foreach (var provider in providers.Where(p => p.IsAvailable))
                {
                    Console.WriteLine($"      - {provider.ProviderName}");
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("   ✗ 没有可用的OLEDB提供程序，需要安装Microsoft Access Database Engine");
                Console.ResetColor();
            }

            Console.WriteLine("\n5. 运行状态:");
            Console.WriteLine($"   监控服务: {(_isMonitoring ? "运行中" : "已停止")}");
            Console.WriteLine($"   程序运行时间: {DateTime.Now - Process.GetCurrentProcess().StartTime:hh\\:mm\\:ss}");
        }

        // 10. 显示最近日志
        private static void ShowRecentLogs()
        {
            _logService.ShowRecentLogs(20);
        }

        // 0. 退出程序
        private static void ExitProgram()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("\n确认退出程序吗？(y/n): ");
            Console.ResetColor();

            var confirm = Console.ReadLine();

            if (confirm?.ToLower() == "y")
            {
                Console.WriteLine("正在退出程序...");

                // 停止监控服务
                if (_isMonitoring)
                {
                    _fileWatcherService.Stop();
                    Console.WriteLine("已停止文件监控服务");
                }

                // 记录退出日志
                _logService.Log("程序正常退出", LogServicegLevel.Info);

                _isRunning = false;
            }
            else
            {
                Console.WriteLine("继续运行程序");
            }
        }

        // 添加 Process 引用（需要在文件顶部添加 using）
        // 注意：需要在文件顶部添加 using System.Diagnostics;
    }
}