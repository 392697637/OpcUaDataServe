using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MDBImporter.Models
{
    /// <summary>
    /// 批量复制设置（用于SQL批量插入操作）
    /// </summary>
    public class BulkCopySettings
    {
        public int BatchSize { get; set; } = 1000;
        public int TimeoutSeconds { get; set; } = 300;
        public int NotifyAfter { get; set; } = 1000;
        public bool EnableStreaming { get; set; } = true;
    }
}
