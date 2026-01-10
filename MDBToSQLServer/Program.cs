using MDBToSQLServer.Core;
using MDBToSQLServer.Services;
using MDBToSQLServer.Utilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

// 创建主机构建器
var builder = Host.CreateApplicationBuilder(args);

// 配置日志
builder.Logging.ClearProviders();
builder.Logging.AddConsole(options =>
{
    options.FormatterName = "simple";
    options.LogToStandardErrorThreshold = LogLevel.Error;
});

builder.Logging.AddDebug();
builder.Logging.SetMinimumLevel(LogLevel.Information);

// 添加配置
builder.Configuration
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: true)
    .AddEnvironmentVariables()
    .AddCommandLine(args);

// 注册服务
builder.Services.AddSingleton<ProviderHelper>();
builder.Services.AddSingleton<TableManager>();
builder.Services.AddSingleton<FileStatusManager>();
builder.Services.AddSingleton<MDBAnalyzer>();
builder.Services.AddSingleton<ConfigHelper>();

builder.Services.AddTransient<MDBImporter>();
builder.Services.AddTransient<SmartFileProcessor>();
builder.Services.AddSingleton<FileMonitorService>();
builder.Services.AddSingleton<MDBImportService>();

// 添加Quartz定时服务
builder.Services.AddQuartz(q =>
{
    q.UseMicrosoftDependencyInjectionJobFactory();
});

builder.Services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);

// 构建主机
using var host = builder.Build();

// 获取服务
var logger = host.Services.GetRequiredService<ILogger<Program>>();
var config = host.Services.GetRequiredService<IConfiguration>();
var processor = host.Services.GetRequiredService<SmartFileProcessor>();
var monitor = host.Services.GetRequiredService<FileMonitorService>();

logger.LogInformation("=== MDB文件导入SQL Server工具 ===");
logger.LogInformation("版本: 2.0.0 | 失败文件保留模式");
logger.LogInformation("启动时间: {StartTime}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
logger.LogInformation("=".PadRight(50, '='));

// 检查并创建必要的文件夹
InitializeFolders(config);

// 处理命令行参数
if (args.Length > 0)
{
    await ProcessCommandLineArgs(args, host);
}
else
{
    await ShowInteractiveMenu(host);
}

return 0;

static void InitializeFolders(IConfiguration config)
{
    var settings = config.GetSection("ApplicationSettings");
    string[] folders = [
        settings["SourceFolder"] ?? @"D:\MDBFiles\Source\",
        settings["ArchiveFolder"] ?? @"D:\MDBFiles\Archive\",
        settings["RetryFolder"] ?? @"D:\MDBFiles\Retry\",
        settings["ErrorFolder"] ?? @"D:\MDBFiles\Error\",
        settings["LogFolder"] ?? @"logs\"
    ];

    foreach (var folder in folders)
    {
        if (!string.IsNullOrEmpty(folder))
        {
            Directory.CreateDirectory(folder);
            Console.WriteLine($"已创建/确认文件夹: {folder}");
        }
    }
}

static async Task ProcessCommandLineArgs(string[] args, IHost host)
{
    var logger = host.Services.GetRequiredService<ILogger<Program>>();
    var processor = host.Services.GetRequiredService<SmartFileProcessor>();
    var monitor = host.Services.GetRequiredService<FileMonitorService>();

    switch (args[0].ToLower())
    {
        case "-import":
        case "-run":
            await RunSingleImport(processor, logger);
            break;
        case "-monitor":
        case "-watch":
            await StartMonitorService(monitor, logger);
            break;
        case "-retry":
            await RetryFailedFiles(processor, logger);
            break;
        case "-analyze":
            await AnalyzeMDBFiles(host, logger);
            break;
        case "-script":
            await GenerateCreateScripts(host, logger);
            break;
        case "-report":
            await GenerateStatusReport(host, logger);
            break;
        case "-cleanup":
            await CleanupOldFiles(processor, logger);
            break;
        case "-test":
            await TestDatabaseConnections(host, logger);
            break;
        case "-config":
            CheckSystemConfiguration(host);
            break;
        case "-service":
            await RunAsService(host);
            break;
        case "-help":
        case "/?":
            ShowCommandLineHelp();
            break;
        default:
            Console.WriteLine("未知命令，使用 -help 查看帮助");
            break;
    }
}

static async Task ShowInteractiveMenu(IHost host)
{
    while (true)
    {
        Console.WriteLine("\n=== 主菜单 ===");
        Console.WriteLine("1. 执行一次导入（保留所有源文件）");
        Console.WriteLine("2. 启动监控服务（自动处理新文件）");
        Console.WriteLine("3. 重试失败文件");
        Console.WriteLine("4. 分析MDB文件结构");
        Console.WriteLine("5. 生成SQL创建脚本");
        Console.WriteLine("6. 查看导入状态报告");
        Console.WriteLine("7. 清理旧文件");
        Console.WriteLine("8. 测试数据库连接");
        Console.WriteLine("9. 系统配置检查");
        Console.WriteLine("0. 退出程序");
        Console.Write("请选择 (0-9): ");

        var choice = Console.ReadLine();

        switch (choice)
        {
            case "1":
                await RunSingleImport(
                    host.Services.GetRequiredService<SmartFileProcessor>(),
                    host.Services.GetRequiredService<ILogger<Program>>());
                break;
            case "2":
                await StartMonitorService(
                    host.Services.GetRequiredService<FileMonitorService>(),
                    host.Services.GetRequiredService<ILogger<Program>>());
                break;
            case "3":
                await RetryFailedFiles(
                    host.Services.GetRequiredService<SmartFileProcessor>(),
                    host.Services.GetRequiredService<ILogger<Program>>());
                break;
            case "4":
                await AnalyzeMDBFiles(host, host.Services.GetRequiredService<ILogger<Program>>());
                break;
            case "5":
                await GenerateCreateScripts(host, host.Services.GetRequiredService<ILogger<Program>>());
                break;
            case "6":
                await GenerateStatusReport(host, host.Services.GetRequiredService<ILogger<Program>>());
                break;
            case "7":
                await CleanupOldFiles(
                    host.Services.GetRequiredService<SmartFileProcessor>(),
                    host.Services.GetRequiredService<ILogger<Program>>());
                break;
            case "8":
                await TestDatabaseConnections(host, host.Services.GetRequiredService<ILogger<Program>>());
                break;
            case "9":
                CheckSystemConfiguration(host);
                break;
            case "0":
                return;
            default:
                Console.WriteLine("无效选择，请重新输入。");
                break;
        }
    }
}

static async Task RunSingleImport(SmartFileProcessor processor, ILogger logger)
{
    Console.WriteLine("\n=== 单次导入模式 ===");

    try
    {
        var result = await Task.Run(() => processor.ProcessAllFiles());

        Console.WriteLine("\n=== 导入完成 ===");
        Console.WriteLine($"成功: {result.SuccessCount}");
        Console.WriteLine($"部分成功: {result.PartialSuccessCount}");
        Console.WriteLine($"失败: {result.FailedCount}");
        Console.WriteLine($"跳过: {result.SkippedCount}");

        if (result.FailedCount > 0)
        {
            Console.WriteLine("\n失败文件已保留在源文件夹中。");
            Console.WriteLine("错误日志已保存到 Error 文件夹。");
        }
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "单次导入失败");
        Console.WriteLine($"\n导入过程出错: {ex.Message}");
    }
}

static async Task StartMonitorService(FileMonitorService monitor, ILogger logger)
{
    Console.WriteLine("\n=== 文件监控模式 ===");
    Console.WriteLine("将监控源文件夹的变化，自动处理新文件。");
    Console.WriteLine("按 Ctrl+C 停止服务");
    Console.WriteLine("正在启动...");

    try
    {
        monitor.Start();

        Console.WriteLine("\n监控服务已启动。");
        Console.WriteLine("正在运行中...");

        // 等待Ctrl+C
        var cancellationTokenSource = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            cancellationTokenSource.Cancel();
        };

        await Task.Delay(Timeout.Infinite, cancellationTokenSource.Token);
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("\n正在停止监控服务...");
        monitor.Stop();
        Console.WriteLine("监控服务已停止。");
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "监控服务错误");
        Console.WriteLine($"监控服务错误: {ex.Message}");
    }
}

static async Task RetryFailedFiles(SmartFileProcessor processor, ILogger logger)
{
    Console.WriteLine("\n=== 重试失败文件 ===");

    try
    {
        var result = await Task.Run(() => processor.ProcessFailedFiles());

        Console.WriteLine("\n=== 重试完成 ===");
        Console.WriteLine($"成功重试: {result.SuccessCount}");
        Console.WriteLine($"仍然失败: {result.FailedCount}");
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "重试失败");
        Console.WriteLine($"重试失败: {ex.Message}");
    }
}

static async Task AnalyzeMDBFiles(IHost host, ILogger logger)
{
    Console.WriteLine("\n=== MDB文件分析 ===");

    var config = host.Services.GetRequiredService<IConfiguration>();
    var analyzer = host.Services.GetRequiredService<MDBAnalyzer>();

    var sourceFolder = config.GetSection("ApplicationSettings")["SourceFolder"]
        ?? @"D:\MDBFiles\Source\";

    var files = Directory.GetFiles(sourceFolder, "*.mdb");
    if (files.Length == 0)
    {
        Console.WriteLine("未找到MDB文件！");
        return;
    }

    await Task.Run(() =>
    {
        foreach (var file in files)
        {
            Console.WriteLine($"\n分析文件: {Path.GetFileName(file)}");
            Console.WriteLine("=".PadRight(50, '='));

            try
            {
                analyzer.AnalyzeMDB(file);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "分析MDB文件失败");
                Console.WriteLine($"分析失败: {ex.Message}");
            }
        }
    });
}

static async Task GenerateCreateScripts(IHost host, ILogger logger)
{
    Console.WriteLine("\n=== 生成SQL创建脚本 ===");

    var config = host.Services.GetRequiredService<IConfiguration>();
    var analyzer = host.Services.GetRequiredService<MDBAnalyzer>();

    var sourceFolder = config.GetSection("ApplicationSettings")["SourceFolder"]
        ?? @"D:\MDBFiles\Source\";

    var files = Directory.GetFiles(sourceFolder, "*.mdb");
    if (files.Length == 0)
    {
        Console.WriteLine("未找到MDB文件！");
        return;
    }

    await Task.Run(() =>
    {
        string outputFolder = Path.Combine(sourceFolder, "SQLScripts");
        Directory.CreateDirectory(outputFolder);

        foreach (var file in files)
        {
            try
            {
                Console.WriteLine($"为文件生成脚本: {Path.GetFileName(file)}");
                analyzer.GenerateCreateTableScripts(file, outputFolder);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "生成SQL脚本失败");
                Console.WriteLine($"生成脚本失败: {ex.Message}");
            }
        }

        Console.WriteLine($"\n脚本已保存到: {outputFolder}");
    });
}

static async Task GenerateStatusReport(IHost host, ILogger logger)
{
    Console.WriteLine("\n=== 生成状态报告 ===");

    try
    {
        var config = host.Services.GetRequiredService<IConfiguration>();
        var archiveFolder = config.GetSection("ApplicationSettings")["ArchiveFolder"]
            ?? @"D:\MDBFiles\Archive\";

        var statusManager = host.Services.GetRequiredService<FileStatusManager>();

        await Task.Run(() =>
        {
            string reportPath = Path.Combine(archiveFolder, $"Report_{DateTime.Now:yyyyMMdd_HHmmss}.txt");
            statusManager.GenerateReport(reportPath);

            Console.WriteLine($"报告已生成: {reportPath}");

            // 显示报告摘要
            var reportContent = File.ReadAllLines(reportPath);
            Console.WriteLine("\n=== 报告摘要 ===");
            foreach (var line in reportContent.Take(20))
            {
                Console.WriteLine(line);
            }
        });
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "生成状态报告失败");
        Console.WriteLine($"生成报告失败: {ex.Message}");
    }
}

static async Task CleanupOldFiles(SmartFileProcessor processor, ILogger logger)
{
    Console.WriteLine("\n=== 清理旧文件 ===");

    try
    {
        Console.Write("请输入保留天数 (默认30天): ");
        string input = Console.ReadLine() ?? "30";

        if (!int.TryParse(input, out int daysToKeep) || daysToKeep <= 0)
        {
            daysToKeep = 30;
        }

        await Task.Run(() => processor.CleanupOldFiles(daysToKeep));

        Console.WriteLine($"\n已清理 {daysToKeep} 天前的归档文件。");
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "清理旧文件失败");
        Console.WriteLine($"清理失败: {ex.Message}");
    }
}

static async Task TestDatabaseConnections(IHost host, ILogger logger)
{
    Console.WriteLine("\n=== 数据库连接测试 ===");

    try
    {
        var providerHelper = host.Services.GetRequiredService<ProviderHelper>();

        await Task.Run(() =>
        {
            Console.WriteLine("1. 测试SQL Server连接...");
            bool sqlTest = providerHelper.TestSqlServerConnection();
            Console.WriteLine($"   SQL Server: {(sqlTest ? "✓ 连接成功" : "✗ 连接失败")}");

            Console.WriteLine("\n2. 测试MDB提供程序...");
            var providers = providerHelper.GetAvailableProviders();

            foreach (var provider in providers)
            {
                Console.WriteLine($"   {provider.ProviderName}: {(provider.IsAvailable ? "✓ 可用" : "✗ 不可用")}");
            }
        });
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "数据库连接测试失败");
        Console.WriteLine($"测试失败: {ex.Message}");
    }
}

static void CheckSystemConfiguration(IHost host)
{
    Console.WriteLine("\n=== 系统配置检查 ===");

    var config = host.Services.GetRequiredService<IConfiguration>();
    var settings = config.GetSection("ApplicationSettings");

    Console.WriteLine("1. 文件夹配置:");
    Console.WriteLine($"   源文件夹: {settings["SourceFolder"]}");
    Console.WriteLine($"   归档文件夹: {settings["ArchiveFolder"]}");
    Console.WriteLine($"   重试文件夹: {settings["RetryFolder"]}");

    Console.WriteLine("\n2. 数据库配置:");
    var connectionString = config.GetConnectionString("SqlServer");
    if (!string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine($"   连接字符串: [已配置]");
    }
    else
    {
        Console.WriteLine($"   连接字符串: [未配置]");
    }

    Console.WriteLine("\n3. 导入配置:");
    Console.WriteLine($"   最大重试次数: {settings["MaxRetryCount"]}");
    Console.WriteLine($"   批次大小: {settings["BatchSize"]}");
    Console.WriteLine($"   保留源文件: {settings["KeepSourceFiles"]}");
}

static async Task RunAsService(IHost host)
{
    Console.WriteLine("运行在服务模式...");

    var importService = host.Services.GetRequiredService<MDBImportService>();
    importService.Start();

    // 等待Ctrl+C
    var cancellationTokenSource = new CancellationTokenSource();
    Console.CancelKeyPress += (sender, e) =>
    {
        e.Cancel = true;
        cancellationTokenSource.Cancel();
    };

    try
    {
        await Task.Delay(Timeout.Infinite, cancellationTokenSource.Token);
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("\n正在停止服务...");
        importService.Stop("用户停止");
    }
}

static void ShowCommandLineHelp()
{
    Console.WriteLine(@"
MDB文件导入SQL Server工具 - 命令行参数

基本命令:
  -import, -run     执行一次导入
  -monitor, -watch  启动监控服务
  -retry            重试失败文件

分析和工具:
  -analyze          分析MDB文件结构
  -script           生成SQL创建脚本
  -report           生成状态报告
  -cleanup          清理旧文件
  -test             测试数据库连接
  -config           查看系统配置

服务模式:
  -service          以服务模式运行

其他:
  -help, /?         显示此帮助信息

示例:
  MDBToSQLServer.exe -import
  MDBToSQLServer.exe -monitor
  MDBToSQLServer.exe -analyze

配置文件: appsettings.json
日志文件: logs\MDBImport_YYYYMMDD.log
");
}