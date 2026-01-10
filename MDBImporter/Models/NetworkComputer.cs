// Models/NetworkComputer.cs
using System;

namespace MDBImporter.Models
{
    public class NetworkComputer
    {
        public string ComputerName { get; set; } = string.Empty;
        public string IPAddress { get; set; } = string.Empty;
        public string MDBFolder { get; set; } = string.Empty;
        public bool Enabled { get; set; } = true;
        public string Description { get; set; } = string.Empty;
        public int SyncIntervalMinutes { get; set; } = 5; // 同步间隔分钟
        public DateTime? LastSyncTime { get; set; }
    }

    public class ImportHistory
    {
        public int Id { get; set; }
        public string ComputerName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public DateTime ImportTime { get; set; }
        public int RecordsImported { get; set; }
        public string Status { get; set; } = string.Empty; // Success, Failed
        public string ErrorMessage { get; set; } = string.Empty;
    }
}