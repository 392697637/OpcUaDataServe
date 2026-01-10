@echo off
chcp 65001 >nul
title MDB文件导入工具 - 失败文件保留模式

echo ===================================================
echo   MDB文件导入SQL Server工具
echo   版本: 2.0.0 | 失败文件保留模式
echo ===================================================
echo.

REM 检查.NET Framework版本
reg query "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full" /v Release | find "Release" >nul
if %errorlevel% neq 0 (
    echo 错误: 需要.NET Framework 4.7.2或更高版本
    pause
    exit /b 1
)

REM 设置环境变量
set APP_PATH=%~dp0
set LOG_PATH=%APP_PATH%logs

REM 创建必要的文件夹
if not exist "%APP_PATH%logs" mkdir "%APP_PATH%logs"
if not exist "%APP_PATH%config" mkdir "%APP_PATH%config"

echo 程序路径: %APP_PATH%
echo 日志路径: %LOG_PATH%
echo.

if "%1"=="" (
    echo 使用方法:
    echo   run.bat -import        执行一次导入
    echo   run.bat -monitor       启动监控服务
    echo   run.bat -retry         重试失败文件
    echo   run.bat -analyze       分析MDB文件结构
    echo   run.bat -script        生成SQL创建脚本
    echo   run.bat -report        生成状态报告
    echo   run.bat -cleanup       清理旧文件
    echo   run.bat -test          测试数据库连接
    echo   run.bat -config        查看系统配置
    echo   run.bat -help          显示帮助
    echo.
    pause
    goto :end
)

echo 正在启动程序...
echo 开始时间: %date% %time%
echo.

REM 运行主程序
MDBToSQLServer.exe %*

:end
echo.
echo 程序结束时间: %date% %time%
pause