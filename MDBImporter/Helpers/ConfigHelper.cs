using MDBImporter.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;

namespace MDBImporter.Helpers
{
    public class ConfigHelper
    {
        private readonly IConfiguration _configuration;

        public ConfigHelper()
        {
            _configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();
        }

        // 获取SQL Server连接字符串
        public string GetSqlServerConnectionString()
        {
            return _configuration.GetConnectionString("SqlServer") ??
                   throw new InvalidOperationException("SQL Server连接字符串未配置");
        }

        // 获取网络计算机配置
        public List<NetworkComputer> GetNetworkComputers()
        {
            var computers = new List<NetworkComputer>();
            _configuration.GetSection("ApplicationSettings:NetworkComputers").Bind(computers);
            return computers;
        }
        public BulkCopySettings GetBulkCopySettings()
        {
            var computers = new BulkCopySettings();
            _configuration.GetSection("ApplicationSettings:BulkCopySettings").Bind(computers);
            return computers;
        }

        // 获取应用程序设置
        public T? GetAppSetting<T>(string key)
        {
            return _configuration.GetValue<T>($"ApplicationSettings:{key}");
        }
    }
}