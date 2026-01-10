namespace MDBToSQLServer.Models
{
    public class FileInfoModel
    {
        public string FileName { get; set; } = string.Empty;
        public string FullPath { get; set; } = string.Empty;
        public long FileSize { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime ModifiedDate { get; set; }
        public DateTime LastAccessDate { get; set; }
        public string Extension { get; set; } = string.Empty;
        public string? DirectoryName { get; set; }
        public bool IsReadOnly { get; set; }
        public bool IsHidden { get; set; }
        public bool IsSystem { get; set; }
        public string Attributes { get; set; } = string.Empty;

        // 导入相关属性
        public FileStatus Status { get; set; }
        public DateTime? ImportTime { get; set; }
        public int? ImportedRows { get; set; }
        public int? ImportedTables { get; set; }
        public string? ImportError { get; set; }
        public int RetryCount { get; set; }

        public enum FileStatus
        {
            Unknown,
            Pending,
            Processing,
            Success,
            PartialSuccess,
            Failed,
            Skipped
        }

        public static FileInfoModel FromSystemFile(FileInfo fileInfo)
        {
            return new FileInfoModel
            {
                FileName = fileInfo.Name,
                FullPath = fileInfo.FullName,
                FileSize = fileInfo.Length,
                CreatedDate = fileInfo.CreationTime,
                ModifiedDate = fileInfo.LastWriteTime,
                LastAccessDate = fileInfo.LastAccessTime,
                Extension = fileInfo.Extension,
                DirectoryName = fileInfo.DirectoryName,
                IsReadOnly = fileInfo.IsReadOnly,
                IsHidden = (fileInfo.Attributes & FileAttributes.Hidden) != 0,
                IsSystem = (fileInfo.Attributes & FileAttributes.System) != 0,
                Attributes = fileInfo.Attributes.ToString(),
                Status = FileStatus.Pending
            };
        }

        public string GetFormattedSize()
        {
            string[] sizes = ["B", "KB", "MB", "GB", "TB"];
            double len = FileSize;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len /= 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }

        public string GetStatusDisplayName()
        {
            return Status switch
            {
                FileStatus.Pending => "待处理",
                FileStatus.Processing => "处理中",
                FileStatus.Success => "成功",
                FileStatus.PartialSuccess => "部分成功",
                FileStatus.Failed => "失败",
                FileStatus.Skipped => "跳过",
                _ => "未知"
            };
        }

        public bool IsMDBFile()
        {
            return Extension.ToLower() == ".mdb" || Extension.ToLower() == ".accdb";
        }

        public bool IsFileLocked()
        {
            try
            {
                using (var stream = File.Open(FullPath,
                    FileMode.Open, FileAccess.Read,
                    FileShare.None))
                {
                    return false;
                }
            }
            catch (IOException)
            {
                return true;
            }
        }

        public bool CanBeProcessed()
        {
            return IsMDBFile() &&
                   !IsFileLocked() &&
                   FileSize > 0 &&
                   Status != FileStatus.Processing;
        }

        public TimeSpan? GetAge()
        {
            if (ModifiedDate == DateTime.MinValue)
                return null;

            return DateTime.Now - ModifiedDate;
        }

        public string GetFormattedAge()
        {
            var age = GetAge();
            if (!age.HasValue)
                return "未知";

            if (age.Value.TotalDays >= 1)
                return $"{age.Value.TotalDays:F1}天";
            else if (age.Value.TotalHours >= 1)
                return $"{age.Value.TotalHours:F1}小时";
            else if (age.Value.TotalMinutes >= 1)
                return $"{age.Value.TotalMinutes:F1}分钟";
            else
                return $"{age.Value.TotalSeconds:F1}秒";
        }

        public bool ShouldArchive()
        {
            // 根据状态决定是否应该归档
            return Status == FileStatus.Success ||
                   Status == FileStatus.PartialSuccess ||
                   Status == FileStatus.Failed;
        }

        public string GetArchiveFileName()
        {
            string baseName = Path.GetFileNameWithoutExtension(FileName);
            string extension = Extension;
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string status = Status.ToString().ToLower();

            return $"{baseName}_{timestamp}_{status}{extension}";
        }
    }
}