# MDB文件导入SQL Server工具

## 项目简介

一个强大的MDB（Microsoft Access）文件导入工具，支持自动将MDB文件数据导入到SQL Server数据库。

**核心特性：**
- ✅ 导入失败时保留源文件不删除
- ✅ 自动创建和同步表结构
- ✅ 支持定时监控和自动导入
- ✅ 详细的错误处理和重试机制
- ✅ 完整的日志和报告系统
- ✅ 支持32位和64位系统

## 系统要求

- Windows 7/8/10/11 或 Windows Server 2008 R2+
- .NET Framework 4.7.2 或更高版本
- Microsoft Access Database Engine 2016 Redistributable（64位）
- SQL Server 2008 或更高版本

## 快速开始

### 1. 安装驱动程序

运行以下脚本安装必要的驱动程序：

```bash
scripts\InstallDrivers.bat