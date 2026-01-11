using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MDBImporter.Models
{
    /// <summary>
    /// 导入历史表
    /// </summary>
    public class ImportHistory
    {
        /// <summary>
        /// Id 唯一标识导入任务的自增主键。
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// 含义：执行导入操作的计算机名称。
        /// 作用：用于追踪和区分是来自哪台机器或服务器的导入任务。
        /// </summary>
        public string ComputerName { get; set; } = string.Empty;
        /// <summary>
        /// 含义：数据被导入到的目标数据库表名。
        /// 作用：记录本次导入影响了数据库中的哪个表。
        /// </summary>
        public string TableName { get; set; } = string.Empty;
        /// <summary>
        ///含义：导入任务开始执行的时间戳。
        ///作用：记录操作发生的具体时间，用于排序、排查问题和生成报告。
        /// </summary>
        public DateTime ImportTime { get; set; }
        /// <summary>
        ///含义：成功导入到目标表中的数据行数。
        ///作用：量化导入结果，用于验证导入是否完整（可与源文件行数对比）。
        /// </summary>
        public int RecordsImported { get; set; }
        /// <summary>
        ///含义：导入任务执行的最终状态。
        ///作用：通常用枚举值表示，
        ///如：‘Success’： 成功‘Failed’： 失败‘Partial’： 部分成功（可能因数据问题跳过了一些记录）
        //这是快速筛选成功或失败任务的关键字段。
        /// </summary>
        public string Status { get; set; } = string.Empty;
        /// <summary>
        ///含义：当Status为“失败”或“部分成功”时，存储详细的错误信息或异常内容。
        ///作用：用于问题诊断和错误分析。成功时此字段通常为NULL或空字符串。
        /// </summary>
        public string ErrorMessage { get; set; } = string.Empty;
        /// <summary>
        ///含义：被导入的源文件的完整名称（包括扩展名，如 data_20231027.csv）。
        ///作用：追踪数据来源文件，便于回溯和文件管理。
        /// </summary>
        public string FileName { get; set; } = string.Empty;
        /// <summary>
        ///含义：被导入的源文件的大小。 单位：通常是字节。
        ///作用：用于性能分析、监控文件大小趋势，或作为导入任务规模的参考指标。
        /// </summary>
        public string FileSize { get; set; } = string.Empty;
        /// <summary>
        ///含义：完成整个导入过程所花费的时间。单位：通常是毫秒或秒。
        ///作用：监控导入性能的核心指标。通过分析此时间，可以评估系统效率、发现性能瓶颈。
        /// </summary>
        public string ImportDuration { get; set; } = string.Empty;
    }
}
