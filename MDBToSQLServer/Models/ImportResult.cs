namespace MDBToSQLServer.Models
{
    public class ImportResultModel
    {
        public string FileName { get; set; } = string.Empty;
        public string FilePath { get; set; } = string.Empty;
        public ImportStatus Status { get; set; }
        public string Message { get; set; } = string.Empty;
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public TimeSpan Duration { get; set; }

        // 表统计
        public int TotalTables { get; set; }
        public int SuccessTables { get; set; }
        public int FailedTables { get; set; }

        // 行统计
        public long TotalRows { get; set; }
        public long SuccessRows { get; set; }
        public long FailedRows { get; set; }

        // 错误信息
        public List<TableError> TableErrors { get; set; } = new();
        public List<RowError> RowErrors { get; set; } = new();

        // 性能指标
        public long MemoryUsage { get; set; }
        public double RowsPerSecond { get; set; }
        public double TablesPerSecond { get; set; }

        // 目标信息
        public string? DestinationDatabase { get; set; }
        public List<string> CreatedTables { get; set; } = new();

        public enum ImportStatus
        {
            NotStarted,
            Processing,
            Success,
            PartialSuccess,
            Failed,
            Cancelled
        }

        public class TableError
        {
            public string TableName { get; set; } = string.Empty;
            public string ErrorMessage { get; set; } = string.Empty;
            public DateTime ErrorTime { get; set; }
            public int ErrorRows { get; set; }
            public string ErrorType { get; set; } = string.Empty;

            public TableError Clone()
            {
                return new TableError
                {
                    TableName = this.TableName,
                    ErrorMessage = this.ErrorMessage,
                    ErrorTime = this.ErrorTime,
                    ErrorRows = this.ErrorRows,
                    ErrorType = this.ErrorType
                };
            }
        }

        public class RowError
        {
            public string TableName { get; set; } = string.Empty;
            public int RowNumber { get; set; }
            public string ColumnName { get; set; } = string.Empty;
            public string ErrorMessage { get; set; } = string.Empty;
            public string? RowData { get; set; }

            public RowError Clone()
            {
                return new RowError
                {
                    TableName = this.TableName,
                    RowNumber = this.RowNumber,
                    ColumnName = this.ColumnName,
                    ErrorMessage = this.ErrorMessage,
                    RowData = this.RowData
                };
            }
        }

        public ImportResultModel()
        {
            StartTime = DateTime.Now;
            Status = ImportStatus.NotStarted;
        }

        public void StartProcessing()
        {
            StartTime = DateTime.Now;
            Status = ImportStatus.Processing;
            Message = "开始处理";
        }

        public void CompleteSuccess()
        {
            EndTime = DateTime.Now;
            Duration = EndTime - StartTime;
            Status = ImportStatus.Success;
            Message = "导入成功";
            CalculatePerformanceMetrics();
        }

        public void CompletePartialSuccess(string message)
        {
            EndTime = DateTime.Now;
            Duration = EndTime - StartTime;
            Status = ImportStatus.PartialSuccess;
            Message = message;
            CalculatePerformanceMetrics();
        }

        public void CompleteFailed(string errorMessage)
        {
            EndTime = DateTime.Now;
            Duration = EndTime - StartTime;
            Status = ImportStatus.Failed;
            Message = errorMessage;
        }

        public void Cancel()
        {
            EndTime = DateTime.Now;
            Duration = EndTime - StartTime;
            Status = ImportStatus.Cancelled;
            Message = "导入已取消";
        }

        private void CalculatePerformanceMetrics()
        {
            if (Duration.TotalSeconds > 0)
            {
                RowsPerSecond = SuccessRows / Duration.TotalSeconds;
                TablesPerSecond = SuccessTables / Duration.TotalSeconds;
            }

            // 获取内存使用情况
            MemoryUsage = GC.GetTotalMemory(false);
        }

        public double GetSuccessRate()
        {
            if (TotalTables == 0) return 0;
            return (double)SuccessTables / TotalTables * 100;
        }

        public string GetFormattedDuration()
        {
            if (Duration.TotalHours >= 1)
                return $"{Duration.TotalHours:F2}小时";
            else if (Duration.TotalMinutes >= 1)
                return $"{Duration.TotalMinutes:F2}分钟";
            else
                return $"{Duration.TotalSeconds:F2}秒";
        }

        public string GetStatusDisplayName()
        {
            return Status switch
            {
                ImportStatus.NotStarted => "未开始",
                ImportStatus.Processing => "处理中",
                ImportStatus.Success => "成功",
                ImportStatus.PartialSuccess => "部分成功",
                ImportStatus.Failed => "失败",
                ImportStatus.Cancelled => "已取消",
                _ => "未知"
            };
        }

        public void AddTableError(string tableName, string errorMessage, string errorType = "General")
        {
            TableErrors.Add(new TableError
            {
                TableName = tableName,
                ErrorMessage = errorMessage,
                ErrorTime = DateTime.Now,
                ErrorType = errorType
            });

            FailedTables++;
        }

        public void AddRowError(string tableName, int rowNumber, string columnName,
            string errorMessage, string? rowData = null)
        {
            RowErrors.Add(new RowError
            {
                TableName = tableName,
                RowNumber = rowNumber,
                ColumnName = columnName,
                ErrorMessage = errorMessage,
                RowData = rowData
            });

            FailedRows++;
        }

        public ImportResultModel Clone()
        {
            var clone = new ImportResultModel
            {
                FileName = this.FileName,
                FilePath = this.FilePath,
                Status = this.Status,
                Message = this.Message,
                StartTime = this.StartTime,
                EndTime = this.EndTime,
                Duration = this.Duration,
                TotalTables = this.TotalTables,
                SuccessTables = this.SuccessTables,
                FailedTables = this.FailedTables,
                TotalRows = this.TotalRows,
                SuccessRows = this.SuccessRows,
                FailedRows = this.FailedRows,
                MemoryUsage = this.MemoryUsage,
                RowsPerSecond = this.RowsPerSecond,
                TablesPerSecond = this.TablesPerSecond,
                DestinationDatabase = this.DestinationDatabase
            };

            // 深度复制集合
            foreach (var error in this.TableErrors)
            {
                clone.TableErrors.Add(error.Clone());
            }

            foreach (var error in this.RowErrors)
            {
                clone.RowErrors.Add(error.Clone());
            }

            clone.CreatedTables.AddRange(this.CreatedTables);

            return clone;
        }

        public string GenerateSummary()
        {
            var summary = new System.Text.StringBuilder();

            summary.AppendLine("=== 导入结果摘要 ===");
            summary.AppendLine($"文件名: {FileName}");
            summary.AppendLine($"状态: {GetStatusDisplayName()}");
            summary.AppendLine($"消息: {Message}");
            summary.AppendLine($"开始时间: {StartTime:yyyy-MM-dd HH:mm:ss}");
            summary.AppendLine($"结束时间: {EndTime:yyyy-MM-dd HH:mm:ss}");
            summary.AppendLine($"耗时: {GetFormattedDuration()}");
            summary.AppendLine();

            summary.AppendLine("📊 统计信息:");
            summary.AppendLine($"   表总数: {TotalTables}");
            summary.AppendLine($"   成功表: {SuccessTables}");
            summary.AppendLine($"   失败表: {FailedTables}");
            summary.AppendLine($"   成功率: {GetSuccessRate():F1}%");
            summary.AppendLine($"   行总数: {TotalRows:N0}");
            summary.AppendLine($"   成功行: {SuccessRows:N0}");
            summary.AppendLine($"   失败行: {FailedRows:N0}");
            summary.AppendLine($"   行/秒: {RowsPerSecond:F1}");
            summary.AppendLine($"   表/秒: {TablesPerSecond:F1}");
            summary.AppendLine($"   内存使用: {FormatMemorySize(MemoryUsage)}");
            summary.AppendLine();

            if (FailedTables > 0)
            {
                summary.AppendLine("⚠️ 表级错误:");
                foreach (var error in TableErrors)
                {
                    summary.AppendLine($"   {error.TableName}: {error.ErrorMessage}");
                }
                summary.AppendLine();
            }

            if (CreatedTables.Count > 0)
            {
                summary.AppendLine("✅ 创建的表:");
                foreach (var table in CreatedTables)
                {
                    summary.AppendLine($"   {table}");
                }
            }

            return summary.ToString();
        }

        private string FormatMemorySize(long bytes)
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

        public bool HasErrors()
        {
            return FailedTables > 0 || FailedRows > 0 || TableErrors.Count > 0 || RowErrors.Count > 0;
        }

        public int GetTotalErrors()
        {
            return FailedTables + (int)FailedRows + TableErrors.Count + RowErrors.Count;
        }

        public string GetErrorSummary()
        {
            if (!HasErrors())
                return "没有错误";

            var errors = new List<string>();

            if (FailedTables > 0)
                errors.Add($"{FailedTables}个表失败");

            if (FailedRows > 0)
                errors.Add($"{FailedRows:N0}行失败");

            return string.Join("，", errors);
        }

        public void SaveToFile(string filePath)
        {
            string json = System.Text.Json.JsonSerializer.Serialize(this,
                new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(filePath, json);
        }

        public static ImportResultModel? LoadFromFile(string filePath)
        {
            if (!File.Exists(filePath))
                return null;

            string json = File.ReadAllText(filePath);
            return System.Text.Json.JsonSerializer.Deserialize<ImportResultModel>(json);
        }
    }
}