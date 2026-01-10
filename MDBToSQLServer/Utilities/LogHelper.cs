using System;
using System.Configuration;
using System.IO;
using System.Text;

namespace MDBToSQLServer.Utilities
{
    public static class LogHelper
    {
        private static readonly object _lock = new object();
        private static readonly string _logDirectory;
        private static readonly bool _enableConsoleLog;
        private static readonly bool _enableFileLog;
        private static readonly LogLevel _minLogLevel;

        public enum LogLevel
        {
            Debug,
            Info,
            Warning,
            Error,
            Critical
        }

        static LogHelper()
        {
            _logDirectory = ConfigurationManager.AppSettings["LogDirectory"] ?? @"logs\";
            _enableConsoleLog = bool.Parse(ConfigurationManager.AppSettings["EnableConsoleLog"] ?? "true");
            _enableFileLog = bool.Parse(ConfigurationManager.AppSettings["EnableFileLog"] ?? "true");

            string logLevelStr = ConfigurationManager.AppSettings["MinLogLevel"] ?? "Info";
            _minLogLevel = (LogLevel)Enum.Parse(typeof(LogLevel), logLevelStr);

            Directory.CreateDirectory(_logDirectory);
        }

        public static void LogDebug(string message)
        {
            WriteLog(LogLevel.Debug, message);
        }

        public static void LogInfo(string message)
        {
            WriteLog(LogLevel.Info, message);
        }

        public static void LogWarning(string message)
        {
            WriteLog(LogLevel.Warning, message);
        }

        public static void LogError(string message, Exception ex = null)
        {
            string errorMessage = message;
            if (ex != null)
            {
                errorMessage += $"\r\n异常类型: {ex.GetType().Name}\r\n异常消息: {ex.Message}\r\n堆栈跟踪: {ex.StackTrace}";

                // 包括内部异常
                Exception innerEx = ex.InnerException;
                int depth = 0;
                while (innerEx != null && depth < 5)
                {
                    errorMessage += $"\r\n内部异常{depth + 1}类型: {innerEx.GetType().Name}\r\n内部异常{depth + 1}消息: {innerEx.Message}";
                    innerEx = innerEx.InnerException;
                    depth++;
                }
            }
            WriteLog(LogLevel.Error, errorMessage);
        }

        public static void LogCritical(string message, Exception ex = null)
        {
            string errorMessage = message;
            if (ex != null)
            {
                errorMessage += $"\r\n异常: {ex.Message}\r\n堆栈: {ex.StackTrace}";
            }
            WriteLog(LogLevel.Critical, errorMessage);
        }

        private static void WriteLog(LogLevel level, string message)
        {
            if (level < _minLogLevel)
                return;

            string logFile = Path.Combine(_logDirectory, $"MDBImport_{DateTime.Now:yyyyMMdd}.log");
            string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            string logMessage = $"{timestamp} [{level.ToString().ToUpper()}] {message}";

            lock (_lock)
            {
                // 写入文件
                if (_enableFileLog)
                {
                    try
                    {
                        File.AppendAllText(logFile, logMessage + Environment.NewLine, Encoding.UTF8);
                    }
                    catch (Exception ex)
                    {
                        // 如果写入文件失败，尝试写入备用位置
                        try
                        {
                            string backupLog = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop),
                                $"MDBImport_Error_{DateTime.Now:yyyyMMdd}.log");
                            File.AppendAllText(backupLog, $"{timestamp} [ERROR] 写入日志文件失败: {ex.Message}" + Environment.NewLine);
                            File.AppendAllText(backupLog, logMessage + Environment.NewLine);
                        }
                        catch
                        {
                            // 如果备份也失败，忽略
                        }
                    }
                }

                // 控制台输出
                if (_enableConsoleLog)
                {
                    ConsoleColor originalColor = Console.ForegroundColor;

                    switch (level)
                    {
                        case LogLevel.Critical:
                        case LogLevel.Error:
                            Console.ForegroundColor = ConsoleColor.Red;
                            break;
                        case LogLevel.Warning:
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            break;
                        case LogLevel.Info:
                            Console.ForegroundColor = ConsoleColor.Green;
                            break;
                        case LogLevel.Debug:
                            Console.ForegroundColor = ConsoleColor.Gray;
                            break;
                    }

                    Console.WriteLine(logMessage);
                    Console.ForegroundColor = originalColor;
                }
            }
        }

        public static string GetLogDirectory()
        {
            return _logDirectory;
        }

        public static void ArchiveOldLogs(int daysToKeep)
        {
            try
            {
                var logFiles = Directory.GetFiles(_logDirectory, "MDBImport_*.log");
                DateTime cutoffDate = DateTime.Now.AddDays(-daysToKeep);

                foreach (var logFile in logFiles)
                {
                    try
                    {
                        var fileInfo = new FileInfo(logFile);
                        if (fileInfo.LastWriteTime < cutoffDate)
                        {
                            // 压缩或移动旧日志
                            string archiveFolder = Path.Combine(_logDirectory, "Archive");
                            Directory.CreateDirectory(archiveFolder);

                            string destFile = Path.Combine(archiveFolder, fileInfo.Name);
                            File.Move(logFile, destFile);

                            LogInfo($"已归档日志文件: {fileInfo.Name}");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError($"归档日志文件失败: {logFile}", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("归档旧日志失败", ex);
            }
        }
    }
}