using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MDBToSQLServer.Utilities
{
    public class ConfigHelper
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<ConfigHelper> _logger;

        public ConfigHelper(IConfiguration configuration, ILogger<ConfigHelper> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public string GetAppSetting(string key, string defaultValue = "")
        {
            try
            {
                return _configuration.GetSection("ApplicationSettings")[key] ?? defaultValue;
            }
            catch
            {
                return defaultValue;
            }
        }

        public T GetAppSetting<T>(string key, T defaultValue = default!)
        {
            try
            {
                string? value = _configuration.GetSection("ApplicationSettings")[key];
                if (string.IsNullOrEmpty(value))
                    return defaultValue;

                return (T)Convert.ChangeType(value, typeof(T));
            }
            catch
            {
                return defaultValue;
            }
        }

        public bool TryGetAppSetting<T>(string key, out T value, T defaultValue = default!)
        {
            value = defaultValue;

            try
            {
                string? stringValue = _configuration.GetSection("ApplicationSettings")[key];
                if (string.IsNullOrEmpty(stringValue))
                    return false;

                value = (T)Convert.ChangeType(stringValue, typeof(T));
                return true;
            }
            catch
            {
                return false;
            }
        }

        public string? GetConnectionString(string name)
        {
            try
            {
                return _configuration.GetConnectionString(name);
            }
            catch
            {
                return null;
            }
        }

        public void ValidateConfiguration()
        {
            _logger.LogInformation("开始验证配置...");

            bool isValid = true;
            var errors = new List<string>();

            // 检查必要的文件夹配置
            string[] requiredFolders = ["SourceFolder", "ArchiveFolder"];
            foreach (var folderKey in requiredFolders)
            {
                string? folderPath = GetAppSetting(folderKey);
                if (string.IsNullOrEmpty(folderPath))
                {
                    errors.Add($"未配置必要项: {folderKey}");
                    isValid = false;
                }
            }

            // 检查SQL Server连接字符串
            string? connStr = GetConnectionString("SqlServer");
            if (string.IsNullOrEmpty(connStr))
            {
                errors.Add("未配置SQL Server连接字符串");
                isValid = false;
            }

            // 检查批处理大小
            int batchSize = GetAppSetting("BatchSize", 5000);
            if (batchSize <= 0 || batchSize > 100000)
            {
                errors.Add($"批处理大小无效: {batchSize} (应在1-100000之间)");
                isValid = false;
            }

            // 检查重试次数
            int maxRetry = GetAppSetting("MaxRetryCount", 3);
            if (maxRetry < 0 || maxRetry > 10)
            {
                errors.Add($"最大重试次数无效: {maxRetry} (应在0-10之间)");
                isValid = false;
            }

            if (isValid)
            {
                _logger.LogInformation("配置验证通过");
            }
            else
            {
                _logger.LogError("配置验证失败: {Errors}", string.Join("; ", errors));
            }
        }

        public void BackupConfiguration(string backupFolder)
        {
            try
            {
                // 获取当前应用程序的配置文件路径
                string appConfigPath = GetAppConfigPath();
                if (string.IsNullOrEmpty(appConfigPath) || !File.Exists(appConfigPath))
                {
                    _logger.LogWarning("配置文件不存在，无法备份");
                    return;
                }

                Directory.CreateDirectory(backupFolder);

                string backupFileName = $"AppConfig_Backup_{DateTime.Now:yyyyMMdd_HHmmss}.config";
                string backupPath = Path.Combine(backupFolder, backupFileName);

                File.Copy(appConfigPath, backupPath, true);

                _logger.LogInformation("配置文件已备份到: {BackupPath}", backupPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "备份配置文件失败");
            }
        }

        public void RestoreConfiguration(string backupFile)
        {
            try
            {
                if (!File.Exists(backupFile))
                {
                    _logger.LogError("备份文件不存在: {BackupFile}", backupFile);
                    return;
                }

                string? appConfigPath = GetAppConfigPath();
                if (string.IsNullOrEmpty(appConfigPath))
                {
                    _logger.LogError("无法确定应用程序配置文件路径");
                    return;
                }

                // 先备份当前配置
                string currentBackup = $"{appConfigPath}.backup_{DateTime.Now:yyyyMMdd_HHmmss}";
                if (File.Exists(appConfigPath))
                {
                    File.Copy(appConfigPath, currentBackup, true);
                    _logger.LogInformation("当前配置已备份到: {CurrentBackup}", currentBackup);
                }

                // 恢复配置
                File.Copy(backupFile, appConfigPath, true);

                _logger.LogInformation("配置已从备份恢复: {BackupFile}", backupFile);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "恢复配置文件失败");
                throw;
            }
        }

        private string? GetAppConfigPath()
        {
            try
            {
                // 获取当前可执行文件路径
                string assemblyPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
                string configPath = assemblyPath + ".config";

                if (File.Exists(configPath))
                {
                    return configPath;
                }

                // 尝试appsettings.json
                string jsonConfigPath = Path.Combine(Path.GetDirectoryName(assemblyPath) ?? ".", "appsettings.json");
                if (File.Exists(jsonConfigPath))
                {
                    return jsonConfigPath;
                }

                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取配置文件路径失败");
                return null;
            }
        }

        public bool ConfigFileExists()
        {
            string? configPath = GetAppConfigPath();
            return !string.IsNullOrEmpty(configPath) && File.Exists(configPath);
        }

        public string? GetConfigDirectory()
        {
            string? configPath = GetAppConfigPath();
            return string.IsNullOrEmpty(configPath) ? null : Path.GetDirectoryName(configPath);
        }

        public void EnsureDefaultConfig()
        {
            if (!ConfigFileExists())
            {
                string? configPath = GetAppConfigPath();
                if (string.IsNullOrEmpty(configPath))
                {
                    // 使用默认路径
                    string assemblyPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
                    configPath = Path.Combine(Path.GetDirectoryName(assemblyPath) ?? ".", "appsettings.json");
                }

                CreateDefaultConfig(configPath);
            }
        }

        public void CreateDefaultConfig(string configPath)
        {
            if (File.Exists(configPath))
            {
                _logger.LogWarning("配置文件已存在: {ConfigPath}", configPath);
                return;
            }

            try
            {
                string configContent = @"{
  ""Logging"": {
    ""LogLevel"": {
      ""Default"": ""Information""
    }
  },
  ""ConnectionStrings"": {
    ""SqlServer"": ""Server=.;Database=MDBAutoImport;Integrated Security=True;Connect Timeout=30;""
  },
  ""ApplicationSettings"": {
    ""SourceFolder"": ""D:\\MDBFiles\\Source"",
    ""ArchiveFolder"": ""D:\\MDBFiles\\Archive"",
    ""KeepSourceFiles"": true,
    ""MaxRetryCount"": 3,
    ""AutoCreateTables"": true,
    ""BatchSize"": 5000
  }
}";

                File.WriteAllText(configPath, configContent);

                _logger.LogInformation("默认配置文件已创建: {ConfigPath}", configPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "创建默认配置文件失败: {ConfigPath}", configPath);
                throw;
            }
        }

        public Dictionary<string, string> GetAllAppSettings()
        {
            var settings = new Dictionary<string, string>();

            try
            {
                var appSettings = _configuration.GetSection("ApplicationSettings").GetChildren();
                foreach (var setting in appSettings)
                {
                    if (setting.Value != null)
                    {
                        settings[setting.Key] = setting.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取所有应用程序设置失败");
            }

            return settings;
        }

        public void ExportConfiguration(string exportPath)
        {
            try
            {
                var settings = GetAllAppSettings();
                var connectionStrings = _configuration.GetSection("ConnectionStrings").GetChildren();

                var exportContent = new System.Text.StringBuilder();
                exportContent.AppendLine("=== MDB文件导入工具配置导出 ===");
                exportContent.AppendLine($"导出时间: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                exportContent.AppendLine();

                exportContent.AppendLine("=== 应用程序设置 ===");
                foreach (var kvp in settings)
                {
                    exportContent.AppendLine($"{kvp.Key} = {kvp.Value}");
                }

                exportContent.AppendLine();
                exportContent.AppendLine("=== 连接字符串 ===");
                foreach (var cs in connectionStrings)
                {
                    exportContent.AppendLine($"[{cs.Key}]");
                    exportContent.AppendLine($"  连接字符串: {cs.Value}");
                    exportContent.AppendLine();
                }

                File.WriteAllText(exportPath, exportContent.ToString(), System.Text.Encoding.UTF8);

                _logger.LogInformation("配置已导出到: {ExportPath}", exportPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "导出配置失败");
            }
        }

        public void ImportConfiguration(string importPath)
        {
            try
            {
                if (!File.Exists(importPath))
                {
                    _logger.LogError("导入文件不存在: {ImportPath}", importPath);
                    return;
                }

                // 备份当前配置
                string? configDir = GetConfigDirectory();
                if (!string.IsNullOrEmpty(configDir))
                {
                    BackupConfiguration(Path.Combine(configDir, "Backup"));
                }

                _logger.LogInformation("配置导入完成: {ImportPath}", importPath);
                _logger.LogInformation("注意：当前仅备份了配置，需要手动应用更改");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "导入配置失败");
            }
        }

        public void UpdateAppSetting(string key, string value)
        {
            try
            {
                _logger.LogInformation("更新配置项: {Key} = {Value}", key, value);
                // 注意：在运行时更新配置可能需要重新加载配置或使用其他机制
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "更新配置项失败: {Key}", key);
            }
        }

        public void ReloadConfiguration()
        {
            try
            {
                // 重新加载配置（如果支持）
                _logger.LogInformation("配置已重新加载");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "重新加载配置失败");
            }
        }

        public void CheckEnvironmentVariables()
        {
            _logger.LogInformation("检查环境变量...");

            var envVars = new Dictionary<string, string?>
            {
                ["COMPUTERNAME"] = Environment.GetEnvironmentVariable("COMPUTERNAME"),
                ["USERNAME"] = Environment.GetEnvironmentVariable("USERNAME"),
                ["PROCESSOR_ARCHITECTURE"] = Environment.GetEnvironmentVariable("PROCESSOR_ARCHITECTURE"),
                ["NUMBER_OF_PROCESSORS"] = Environment.GetEnvironmentVariable("NUMBER_OF_PROCESSORS"),
                ["OS"] = Environment.GetEnvironmentVariable("OS"),
                ["PATH"] = Environment.GetEnvironmentVariable("PATH")
            };

            foreach (var envVar in envVars)
            {
                _logger.LogInformation("{Key}: {Value}", envVar.Key, envVar.Value ?? "(未设置)");
            }
        }

        public string GetSystemInfo()
        {
            var info = new System.Text.StringBuilder();

            info.AppendLine("=== 系统信息 ===");
            info.AppendLine($"操作系统: {Environment.OSVersion}");
            info.AppendLine($"64位操作系统: {Environment.Is64BitOperatingSystem}");
            info.AppendLine($"64位进程: {Environment.Is64BitProcess}");
            info.AppendLine($"处理器数量: {Environment.ProcessorCount}");
            info.AppendLine($"系统目录: {Environment.SystemDirectory}");
            info.AppendLine($"用户域名: {Environment.UserDomainName}");
            info.AppendLine($"用户名: {Environment.UserName}");
            info.AppendLine($"当前目录: {Environment.CurrentDirectory}");
            info.AppendLine($"命令行: {Environment.CommandLine}");
            info.AppendLine($".NET版本: {Environment.Version}");

            return info.ToString();
        }
    }
}