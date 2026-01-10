using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MDBToSQLServer.Core
{
    public class TableManager
    {
        private readonly string _connectionString;
        private readonly ILogger<TableManager> _logger;

        public TableManager(IConfiguration configuration, ILogger<TableManager> logger)
        {
            _connectionString = configuration.GetConnectionString("SqlServer")
                ?? throw new InvalidOperationException("未配置SQL Server连接字符串");
            _logger = logger;
        }

        public bool TableExists(string tableName)
        {
            string sql = @"
                SELECT COUNT(*) 
                FROM sys.tables 
                WHERE name = @TableName 
                AND schema_id = SCHEMA_ID('dbo')";

            try
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    conn.Open();
                    cmd.Parameters.AddWithValue("@TableName", tableName);
                    return (int)(cmd.ExecuteScalar() ?? 0) > 0;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "检查表是否存在失败: {TableName}", tableName);
                throw;
            }
        }

        //public void CreateTableFromReader(IDataReader reader, string tableName)
        //{
        //    try
        //    {
        //        string createSql = GenerateCreateTableSql(reader, tableName);
        //        ExecuteSql(createSql);

        //        _logger.LogInformation("创建表成功: {TableName}", tableName);
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex, "创建表失败: {TableName}", tableName);
        //        throw;
        //    }
        //}
        public bool CreateTableFromReader(IDataReader reader, string tableName)
        {
            try
            {
                // 检查表是否已存在
                if (TableExists(tableName))
                {
                    _logger.LogInformation($"表 {tableName} 已存在，跳过创建");
                    return true;
                }

                // 生成建表SQL
                var createTableSql = GenerateCreateTableSql(reader, tableName);

                // 执行建表
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();

                    using (var command = new SqlCommand(createTableSql, connection))
                    {
                        command.ExecuteNonQuery();
                        _logger.LogInformation($"成功创建表: {tableName}");

                        // 验证表是否创建成功
                        if (TableExists(tableName))
                        {
                            _logger.LogInformation($"验证: 表 {tableName} 创建成功");
                            return true;
                        }
                        else
                        {
                            _logger.LogWarning($"验证: 表 {tableName} 创建后未找到");
                            return false;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"创建表失败: {tableName}");
                throw;
            }
        }
     
      
        //private string GenerateCreateTableSql(IDataReader reader, string tableName)
        //{
        //    var sql = new System.Text.StringBuilder();
        //    sql.AppendLine($"CREATE TABLE [dbo].[{tableName}] (");

        //    var columns = new List<string>();
        //    DataTable? schema = null;

        //    try
        //    {
        //        schema = reader.GetSchemaTable();
        //    }
        //    catch
        //    {
        //        // 如果不能获取schema，使用默认设置
        //    }

        //    // 添加原有列
        //    for (int i = 0; i < reader.FieldCount; i++)
        //    {
        //        string columnName = reader.GetName(i);
        //        Type columnType = reader.GetFieldType(i);
        //        bool isNullable = true;
        //        bool isIdentity = false;

        //        if (schema != null)
        //        {
        //            DataRow[] rows = schema.Select($"ColumnName = '{columnName}'");
        //            if (rows.Length > 0)
        //            {
        //                var row = rows[0];
        //                isNullable = row["AllowDBNull"] != DBNull.Value && (bool)row["AllowDBNull"];
        //                isIdentity = row["IsIdentity"] != DBNull.Value && (bool)row["IsIdentity"];
        //            }
        //        }

        //        string sqlType = MapToSqlType(columnType, columnName);
        //        string nullable = isNullable ? "NULL" : "NOT NULL";

        //        if (isIdentity && columnName.Equals("ID", StringComparison.OrdinalIgnoreCase))
        //        {
        //            columns.Add($"[{columnName}] {sqlType} IDENTITY(1,1) NOT NULL");
        //        }
        //        else
        //        {
        //            columns.Add($"[{columnName}] {sqlType} {nullable}");
        //        }
        //    }

        //    // 添加额外列
        //    bool addSourceFile = true; // 可以从配置读取
        //    bool addImportTime = true; // 可以从配置读取

        //    if (addSourceFile)
        //    {
        //        columns.Add("[__SourceFile] NVARCHAR(500) NULL");
        //    }
        //    if (addImportTime)
        //    {
        //        columns.Add("[__ImportTime] DATETIME NULL");
        //    }

        //    sql.AppendLine(string.Join(",\r\n", columns));
        //    sql.AppendLine(")");

        //    // 如果有ID列，添加主键
        //    if (HasColumn(reader, "ID"))
        //    {
        //        sql.AppendLine($"\nALTER TABLE [dbo].[{tableName}] ADD CONSTRAINT PK_{tableName}_ID PRIMARY KEY (ID)");
        //    }

        //    // 添加索引
        //    sql.AppendLine($"\nCREATE INDEX IX_{tableName}_ImportTime ON [dbo].[{tableName}] (__ImportTime)");

        //    return sql.ToString();
        //}

        public string GenerateCreateTableSql(IDataReader reader, string tableName)
        {
            try
            {
                // 先调试查看架构
                DebugSchemaTable(reader);

                var schemaTable = reader.GetSchemaTable();

                if (schemaTable == null)
                {
                    throw new InvalidOperationException("无法获取架构表信息");
                }

                var columns = new List<string>();

                foreach (DataRow row in schemaTable.Rows)
                {
                    // 获取列名
                    var columnName = row["ColumnName"].ToString();
                    var dataType = row["DataType"].ToString();
                    var isNullable = Convert.ToBoolean(row["AllowDBNull"]);

                    // 获取列大小（对于字符串和二进制类型）
                    var columnSize = row["ColumnSize"] as int?;

                    // 检测是否为自增列 - 修复这里的错误
                    bool isIdentity = false;

                    // 方法1：尝试使用 AutoIncrement 属性
                    if (row.Table.Columns.Contains("IsAutoIncrement"))
                    {
                        isIdentity = Convert.ToBoolean(row["IsAutoIncrement"]);
                    }
                    // 方法2：尝试使用 IsIdentity 属性（旧方法）
                    else if (row.Table.Columns.Contains("IsIdentity"))
                    {
                        isIdentity = Convert.ToBoolean(row["IsIdentity"]);
                    }

                    // 方法3：使用其他方式检测自增列
                    if (!isIdentity && row.Table.Columns.Contains("IsKey") && row.Table.Columns.Contains("IsAutoIncrement"))
                    {
                        isIdentity = Convert.ToBoolean(row["IsKey"]) &&
                                   Convert.ToBoolean(row["IsAutoIncrement"]);
                    }

                    // 转换为 SQL Server 数据类型
                    var sqlType = GetSqlDataType(dataType, columnSize, isNullable, isIdentity);

                    columns.Add($"[{columnName}] {sqlType}");
                }

                var createTableSql = $"CREATE TABLE [{tableName}] (\n    {string.Join(",\n    ", columns)}\n)";

                _logger.LogDebug($"生成的建表SQL: {createTableSql}");
                return createTableSql;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"生成表 {tableName} 的创建SQL失败");
                throw;
            }
        }
        public void DebugSchemaTable(IDataReader reader)
        {
            try
            {
                var schemaTable = reader.GetSchemaTable();

                if (schemaTable == null)
                {
                    _logger.LogWarning("架构表为空");
                    return;
                }

                _logger.LogInformation("架构表结构：");

                // 列出所有列名
                _logger.LogInformation($"架构表有 {schemaTable.Columns.Count} 列:");
                foreach (DataColumn column in schemaTable.Columns)
                {
                    _logger.LogInformation($"  - {column.ColumnName} ({column.DataType})");
                }

                // 显示前几行的数据
                _logger.LogInformation($"架构表有 {schemaTable.Rows.Count} 行:");
                for (int i = 0; i < Math.Min(schemaTable.Rows.Count, 3); i++)
                {
                    var row = schemaTable.Rows[i];
                    _logger.LogInformation($"行 {i}:");
                    foreach (DataColumn column in schemaTable.Columns)
                    {
                        _logger.LogInformation($"  {column.ColumnName}: {row[column]}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "调试架构表失败");
            }
        }



        private string GetSqlDataType(string clrType, int? columnSize, bool isNullable, bool isIdentity)
        {
            string sqlType;

            // 根据 CLR 类型映射到 SQL Server 类型
            switch (clrType.ToLower())
            {
                case "system.int32":
                    sqlType = isIdentity ? "INT IDENTITY(1,1)" : "INT";
                    break;
                case "system.int64":
                    sqlType = isIdentity ? "BIGINT IDENTITY(1,1)" : "BIGINT";
                    break;
                case "system.string":
                    var size = columnSize > 0 && columnSize < 8000 ? columnSize.Value : 255;
                    sqlType = size <= 255 ? $"NVARCHAR({size})" : "NVARCHAR(MAX)";
                    break;
                case "system.boolean":
                    sqlType = "BIT";
                    break;
                case "system.datetime":
                    sqlType = "DATETIME";
                    break;
                case "system.decimal":
                    sqlType = "DECIMAL(18,2)";
                    break;
                case "system.double":
                    sqlType = "FLOAT";
                    break;
                case "system.single":
                    sqlType = "REAL";
                    break;
                case "system.guid":
                    sqlType = "UNIQUEIDENTIFIER";
                    break;
                case "system.byte[]":
                    sqlType = "VARBINARY(MAX)";
                    break;
                default:
                    sqlType = "NVARCHAR(MAX)"; // 默认类型
                    break;
            }

            // 添加可为空性
            if (!sqlType.Contains("IDENTITY") && !isNullable)
            {
                sqlType += " NOT NULL";
            }
            else if (isNullable)
            {
                sqlType += " NULL";
            }

            return sqlType;
        }

        private bool HasColumn(IDataReader reader, string columnName)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        private string MapToSqlType(Type type, string columnName)
        {
            string columnNameLower = columnName.ToLower();

            // 根据列名智能推断类型
            if (columnNameLower.Contains("date") || columnNameLower.Contains("time"))
                return "DATETIME";

            if (columnNameLower.Contains("amount") || columnNameLower.Contains("price") ||
                columnNameLower.Contains("money") || columnNameLower.Contains("cost"))
                return "DECIMAL(18, 2)";

            if (columnNameLower.Contains("quantity") || columnNameLower.Contains("qty") ||
                columnNameLower.Contains("weight") || columnNameLower.Contains("volume"))
                return "DECIMAL(18, 4)";

            if (columnNameLower.Contains("id") && type == typeof(string))
                return "NVARCHAR(50)";

            if (columnNameLower.Contains("code") || columnNameLower.Contains("no") ||
                columnNameLower.Contains("num"))
                return "NVARCHAR(50)";

            if (columnNameLower.Contains("name") || columnNameLower.Contains("title"))
                return "NVARCHAR(200)";

            if (columnNameLower.Contains("description") || columnNameLower.Contains("remark") ||
                columnNameLower.Contains("note") || columnNameLower.Contains("comment"))
                return "NVARCHAR(MAX)";

            // 根据.NET类型映射
            if (type == typeof(string))
                return "NVARCHAR(MAX)";
            else if (type == typeof(int))
                return "INT";
            else if (type == typeof(long))
                return "BIGINT";
            else if (type == typeof(decimal))
                return "DECIMAL(18, 6)";
            else if (type == typeof(float))
                return "REAL";
            else if (type == typeof(double))
                return "FLOAT";
            else if (type == typeof(DateTime))
                return "DATETIME";
            else if (type == typeof(bool))
                return "BIT";
            else if (type == typeof(byte[]))
                return "VARBINARY(MAX)";
            else if (type == typeof(Guid))
                return "UNIQUEIDENTIFIER";
            else
                return "NVARCHAR(MAX)";
        }

        public void SyncTableStructure(string tableName, IDataReader reader)
        {
            if (!TableExists(tableName))
            {
                CreateTableFromReader(reader, tableName);
                return;
            }

            try
            {
                // 获取现有表的列
                var existingColumns = GetTableColumns(tableName);

                // 获取源表的列
                var sourceColumns = new List<string>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    sourceColumns.Add(reader.GetName(i));
                }

                // 添加缺失的列
                foreach (var column in sourceColumns)
                {
                    if (!existingColumns.Contains(column, StringComparer.OrdinalIgnoreCase))
                    {
                        AddColumn(tableName, column, reader);
                    }
                }

                _logger.LogInformation("同步表结构完成: {TableName}", tableName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "同步表结构失败: {TableName}", tableName);
                throw;
            }
        }

        private List<string> GetTableColumns(string tableName)
        {
            var columns = new List<string>();

            string sql = @"
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = @TableName
                AND TABLE_SCHEMA = 'dbo'";

            using (SqlConnection conn = new SqlConnection(_connectionString))
            using (SqlCommand cmd = new SqlCommand(sql, conn))
            {
                conn.Open();
                cmd.Parameters.AddWithValue("@TableName", tableName);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(reader.GetString(0));
                    }
                }
            }

            return columns;
        }

        private void AddColumn(string tableName, string columnName, IDataReader reader)
        {
            // 找到列的索引
            int columnIndex = -1;
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    columnIndex = i;
                    break;
                }
            }

            if (columnIndex < 0)
                return;

            Type columnType = reader.GetFieldType(columnIndex);
            string sqlType = MapToSqlType(columnType, columnName);

            string sql = $"ALTER TABLE [dbo].[{tableName}] ADD [{columnName}] {sqlType} NULL";
            ExecuteSql(sql);

            _logger.LogInformation("为表 {TableName} 添加列: {ColumnName}", tableName, columnName);
        }

        private void ExecuteSql(string sql)
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            using (SqlCommand cmd = new SqlCommand(sql, conn))
            {
                conn.Open();
                cmd.ExecuteNonQuery();
            }
        }

        public void DropTable(string tableName)
        {
            try
            {
                string sql = $"DROP TABLE IF EXISTS [dbo].[{tableName}]";
                ExecuteSql(sql);

                _logger.LogInformation("删除表成功: {TableName}", tableName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "删除表失败: {TableName}", tableName);
                throw;
            }
        }

        public bool TestConnection()
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand("SELECT 1", conn))
                    {
                        var result = cmd.ExecuteScalar();
                        return result != null;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "测试数据库连接失败");
                return false;
            }
        }

        public List<string> GetDatabaseTables()
        {
            var tables = new List<string>();

            try
            {
                string sql = @"
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE' 
                    AND TABLE_SCHEMA = 'dbo'
                    ORDER BY TABLE_NAME";

                using (SqlConnection conn = new SqlConnection(_connectionString))
                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    conn.Open();

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tables.Add(reader.GetString(0));
                        }
                    }
                }

                return tables;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "获取数据库表列表失败");
                return tables;
            }
        }

        public bool TruncateTable(string tableName)
        {
            try
            {
                string sql = $"TRUNCATE TABLE [dbo].[{tableName}]";
                ExecuteSql(sql);

                _logger.LogInformation("清空表成功: {TableName}", tableName);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "清空表失败: {TableName}", tableName);
                return false;
            }
        }
    }
}