// Services/LogService.cs
using System;
using System.IO;
using System.Linq;

namespace MDBImporter.Services
{
    public class LogService
    {
        private readonly string _logDirectory;

        public LogService()
        {
            _logDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");

            if (!Directory.Exists(_logDirectory))
                Directory.CreateDirectory(_logDirectory);
        }

        // 记录日志
        public void Log(string message, LogServicegLevel level = LogServicegLevel.Info)
        {
            var logFile = Path.Combine(_logDirectory, $"log_{DateTime.Now:yyyyMMdd}.txt");
            var logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{level}] - {message}";

            File.AppendAllText(logFile, logMessage + Environment.NewLine);
            Console.WriteLine(logMessage);
        }

        // 清理日志文件（保留最近N天的日志）
        public void CleanLogs(int keepDays = 30)
        {
            try
            {
                var cutoffDate = DateTime.Now.AddDays(-keepDays);

                foreach (var file in Directory.GetFiles(_logDirectory, "log_*.txt"))
                {
                    var fileInfo = new FileInfo(file);
                    if (fileInfo.CreationTime < cutoffDate)
                    {
                        File.Delete(file);
                        Log($"已删除旧日志文件: {file}", LogServicegLevel.Info);
                    }
                }

                Console.WriteLine($"日志清理完成，保留最近{keepDays}天的日志");
            }
            catch (Exception ex)
            {
                Log($"清理日志失败: {ex.Message}", LogServicegLevel.Error);
            }
        }

        // 显示最近的日志
        public void ShowRecentLogs(int count = 20)
        {
            var logFile = Path.Combine(_logDirectory, $"log_{DateTime.Now:yyyyMMdd}.txt");

            if (File.Exists(logFile))
            {
                var lines = File.ReadAllLines(logFile);
                var recentLines = lines.TakeLast(count);

                Console.WriteLine($"=== 最近 {count} 条日志 ===");
                foreach (var line in recentLines)
                {
                    Console.WriteLine(line);
                }
            }
            else
            {
                Console.WriteLine("今天没有日志记录");
            }
        }
    }

    public enum LogServicegLevel
    {
        Info,
        Warning,
        Error,
        Information
    }
}