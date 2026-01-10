using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MDBImporter.Core
{
    public class ProviderHelper
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<ProviderHelper> _logger;

        private static readonly string[] _providers = [
            "Microsoft.ACE.OLEDB.12.0",  // Access 2007+
            "Microsoft.Jet.OLEDB.4.0",   // Access 2003
        ];

        public class ProviderInfo
        {
            public string ProviderName { get; set; } = string.Empty;
            public bool IsAvailable { get; set; }
            public string Description { get; set; } = string.Empty;
        }

        public ProviderHelper(IConfiguration configuration, ILogger<ProviderHelper> logger)
        {
            _configuration = configuration;
            _logger = logger;
            _logger.LogInformation("ProviderHelper初始化");
        }

        public string GetConnectionString(string filePath)
        {
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"MDB文件不存在: {filePath}");
            }

            // 检查文件扩展名
            string ext = Path.GetExtension(filePath).ToLower();

            // 确定使用哪个提供程序
            string provider = DetermineProvider(filePath, ext);

            // 构建连接字符串
            string connStr = BuildConnectionString(provider, filePath);

            _logger.LogInformation("使用提供程序: {Provider} 连接文件: {FileName}",
                provider, Path.GetFileName(filePath));
            return connStr;
        }

        private string DetermineProvider(string filePath, string extension)
        {
            var settings = _configuration.GetSection("ApplicationSettings");
            bool useAutoDetect = bool.Parse(settings["UseAutoDetectProvider"] ?? "true");

            if (!useAutoDetect)
            {
                // 使用配置的提供程序
                string configuredProvider = settings["MDBProvider"]
                    ?? "Microsoft.ACE.OLEDB.12.0";
                return configuredProvider;
            }

            // 根据文件扩展名选择提供程序
            if (extension == ".accdb")
            {
                // Access 2007+格式必须使用ACE
                return "Microsoft.ACE.OLEDB.12.0";
            }
            else if (extension == ".mdb")
            {
                // 尝试检测可用的提供程序
                foreach (var provider in _providers)
                {
                    if (TestProvider(provider, filePath))
                    {
                        return provider;
                    }
                }
            }

            throw new InvalidOperationException(
                "找不到可用的OLEDB提供程序。请安装Microsoft Access Database Engine。");
        }

        private string BuildConnectionString(string provider, string filePath)
        {
            // 基本连接字符串
            string connStr = $"Provider={provider};Data Source={filePath};";

            // 添加密码（如果有）
            var settings = _configuration.GetSection("ApplicationSettings");
            string password = settings["MDBPassword"] ?? string.Empty;
            if (!string.IsNullOrEmpty(password))
            {
                connStr += $"Jet OLEDB:Database Password={password};";
            }

            // 添加其他选项
            connStr += "Persist Security Info=False;";

            return connStr;
        }

        public bool TestProvider(string provider, string? testFile = null)
        {
            try
            {
                // 如果是测试特定文件
                if (!string.IsNullOrEmpty(testFile) && File.Exists(testFile))
                {
                    string testConnStr = $"Provider={provider};Data Source={testFile};";
                    using (var conn = new OleDbConnection(testConnStr))
                    {
                        conn.Open();
                        return true;
                    }
                }
                else
                {
                    // 测试提供程序是否可用
                    string testConnStr = $"Provider={provider};Data Source=;";
                    using (var conn = new OleDbConnection(testConnStr))
                    {
                        // 不打开连接，只测试提供程序是否可用
                        return true;
                    }
                }
            }
            catch
            {
                return false;
            }
        }

        public ProviderInfo[] GetAvailableProviders()
        {
            return _providers.Select(provider => new ProviderInfo
            {
                ProviderName = provider,
                IsAvailable = TestProvider(provider),
                Description = GetProviderDescription(provider)
            }).ToArray();
        }

        private string GetProviderDescription(string provider)
        {
            return provider switch
            {
                "Microsoft.ACE.OLEDB.12.0" => "Access 2007及更高版本 (32/64位)",
                "Microsoft.Jet.OLEDB.4.0" => "Access 2003及更早版本 (32位)",
                _ => "未知提供程序"
            };
        }

        public bool IsFileLocked(string filePath)
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

        public bool TestSqlServerConnection()
        {
            try
            {
                string? connStr = _configuration.GetConnectionString("SqlServer");
                if (string.IsNullOrEmpty(connStr))
                {
                    _logger.LogError("SQL Server连接字符串未配置");
                    return false;
                }

                using (var conn = new SqlConnection(connStr))
                {
                    conn.Open();

                    // 执行简单的查询测试
                    using (var cmd = new SqlCommand("SELECT 1", conn))
                    {
                        var result = cmd.ExecuteScalar();
                        return result != null;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "测试SQL Server连接失败");
                return false;
            }
        }

        public string GetSystemArchitecture()
        {
            return Environment.Is64BitProcess ? "64位" : "32位";
        }

        public void CheckSystemRequirements()
        {
            _logger.LogInformation("检查系统要求...");

            // 检查.NET版本
            string netVersion = Environment.Version.ToString();
            _logger.LogInformation("当前.NET版本: {NetVersion}", netVersion);

            // 检查系统架构
            string architecture = GetSystemArchitecture();
            _logger.LogInformation("当前进程架构: {Architecture}", architecture);

            // 检查可用的提供程序
            var providers = GetAvailableProviders();
            foreach (var provider in providers)
            {
                _logger.LogInformation("{ProviderName}: {Status}",
                    provider.ProviderName, provider.IsAvailable ? "可用" : "不可用");
            }

            // 检查SQL Server连接
            bool sqlConnected = TestSqlServerConnection();
            _logger.LogInformation("SQL Server连接: {Status}",
                sqlConnected ? "成功" : "失败");
        }

        public string? DetectBestProvider()
        {
            foreach (var provider in _providers)
            {
                if (TestProvider(provider))
                {
                    _logger.LogInformation("检测到最佳提供程序: {Provider}", provider);
                    return provider;
                }
            }

            return null;
        }

        public bool CanConnectToFile(string filePath)
        {
            try
            {
                string connStr = GetConnectionString(filePath);
                using (var conn = new OleDbConnection(connStr))
                {
                    conn.Open();
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "无法连接到文件: {FilePath}", filePath);
                return false;
            }
        }

        public List<string> GetFileProperties(string filePath)
        {
            var properties = new List<string>();

            try
            {
                if (File.Exists(filePath))
                {
                    var fileInfo = new FileInfo(filePath);
                    properties.Add($"文件名: {fileInfo.Name}");
                    properties.Add($"完整路径: {fileInfo.FullName}");
                    properties.Add($"大小: {FormatFileSize(fileInfo.Length)}");
                    properties.Add($"创建时间: {fileInfo.CreationTime:yyyy-MM-dd HH:mm:ss}");
                    properties.Add($"修改时间: {fileInfo.LastWriteTime:yyyy-MM-dd HH:mm:ss}");
                    properties.Add($"最后访问时间: {fileInfo.LastAccessTime:yyyy-MM-dd HH:mm:ss}");
                    properties.Add($"只读: {fileInfo.IsReadOnly}");
                    properties.Add($"隐藏: {fileInfo.Attributes.HasFlag(FileAttributes.Hidden)}");

                    // 尝试连接并获取更多信息
                    if (CanConnectToFile(filePath))
                    {
                        properties.Add("状态: 可连接");
                    }
                    else
                    {
                        properties.Add("状态: 不可连接");
                    }
                }
                else
                {
                    properties.Add("文件不存在");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取文件属性失败: {FilePath}", filePath);
                properties.Add($"错误: {ex.Message}");
            }

            return properties;
        }

        private string FormatFileSize(long bytes)
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

        public bool IsAccess2007OrLater(string filePath)
        {
            string ext = Path.GetExtension(filePath).ToLower();
            return ext == ".accdb";
        }

        public bool IsAccess2003OrEarlier(string filePath)
        {
            string ext = Path.GetExtension(filePath).ToLower();
            return ext == ".mdb";
        }

        public string GetFileTypeDescription(string filePath)
        {
            string ext = Path.GetExtension(filePath).ToLower();
            return ext switch
            {
                ".mdb" => "Microsoft Access 2003及更早版本数据库",
                ".accdb" => "Microsoft Access 2007及更高版本数据库",
                _ => "未知文件类型"
            };
        }
    }
}