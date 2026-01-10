@echo off
chcp 65001 >nul
title MDB文件导入工具 - 驱动程序安装程序

echo ===================================================
echo   Microsoft Access Database Engine 安装程序
echo   用于MDB文件导入工具
echo ===================================================
echo.

setlocal enabledelayedexpansion

REM 检测系统位数
echo 正在检测系统架构...
wmic os get osarchitecture | find "64" >nul
if %errorlevel% equ 0 (
    echo 检测到64位操作系统
    set "ARCH=x64"
    set "DRIVER_FILE=AccessDatabaseEngine_X64.exe"
    set "DOWNLOAD_URL=https://download.microsoft.com/download/3/5/C/35C84C36-661A-44E6-9324-8786B8DBE231/AccessDatabaseEngine_X64.exe"
) else (
    echo 检测到32位操作系统
    set "ARCH=x86"
    set "DRIVER_FILE=AccessDatabaseEngine_X32.exe"
    set "DOWNLOAD_URL=https://download.microsoft.com/download/3/5/C/35C84C36-661A-44E6-9324-8786B8DBE231/AccessDatabaseEngine_X32.exe"
)

echo.
echo 系统架构: %ARCH%
echo 驱动程序: %DRIVER_FILE%
echo.

REM 检查是否已安装
echo 检查是否已安装Microsoft Access Database Engine...
reg query "HKLM\SOFTWARE\Microsoft\Office\14.0\Common\FilesPaths" /v "mso.dll" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Microsoft Access Database Engine 已安装
    goto :check_sqlserver
)

REM 下载驱动程序
echo 正在下载驱动程序...
echo 下载地址: %DOWNLOAD_URL%
echo.

REM 使用PowerShell下载
powershell -Command "& {
    $ProgressPreference = 'SilentlyContinue';
    Invoke-WebRequest -Uri '%DOWNLOAD_URL%' -OutFile '%DRIVER_FILE%';
    if (Test-Path '%DRIVER_FILE%') {
        Write-Host '下载完成' -ForegroundColor Green;
    } else {
        Write-Host '下载失败' -ForegroundColor Red;
        exit 1;
    }
}"

if %errorlevel% neq 0 (
    echo.
    echo 下载失败，请手动下载驱动程序：
    echo 64位系统: %DOWNLOAD_URL%
    echo 32位系统: https://download.microsoft.com/download/3/5/C/35C84C36-661A-44E6-9324-8786B8DBE231/AccessDatabaseEngine_X32.exe
    echo.
    echo 下载后，请手动运行安装程序。
    pause
    exit /b 1
)

REM 安装驱动程序
echo.
echo 正在安装驱动程序...
echo 这可能需要几分钟时间，请稍候...
echo.

REM 静默安装
start /wait %DRIVER_FILE% /quiet

REM 检查安装结果
timeout /t 5 /nobreak >nul

reg query "HKLM\SOFTWARE\Microsoft\Office\14.0\Common\FilesPaths" /v "mso.dll" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Microsoft Access Database Engine 安装成功
) else (
    echo ✗ Microsoft Access Database Engine 安装失败
    echo 请尝试手动安装驱动程序
    pause
    exit /b 1
)

:check_sqlserver
echo.
echo ===================================================
echo   检查SQL Server配置
echo ===================================================
echo.

REM 检查SQL Server Native Client
echo 检查SQL Server Native Client...
reg query "HKLM\SOFTWARE\Microsoft\Microsoft SQL Server Native Client 11.0" >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ SQL Server Native Client 11.0 已安装
) else (
    echo ! SQL Server Native Client 11.0 未安装
    echo 建议安装SQL Server Native Client以提高性能
)

REM 检查ODBC驱动
echo.
echo 检查ODBC驱动...
odbcad32.exe
echo 如果ODBC数据源管理器打开，请检查是否配置了正确的驱动

echo.
echo ===================================================
echo   创建必要的文件夹结构
echo ===================================================
echo.

set "BASE_FOLDER=D:\MDBFiles"
set "FOLDERS=Source Archive Retry Error logs\archive"

echo 创建基础文件夹: %BASE_FOLDER%
if not exist "%BASE_FOLDER%" mkdir "%BASE_FOLDER%"

for %%f in (%FOLDERS%) do (
    set "FULL_PATH=%BASE_FOLDER%\%%f"
    echo 创建文件夹: !FULL_PATH!
    if not exist "!FULL_PATH!" mkdir "!FULL_PATH!"
)

echo.
echo ===================================================
echo   配置环境变量
echo ===================================================
echo.

REM 设置环境变量（可选）
echo 设置临时环境变量...
setx MDB_IMPORT_HOME "%CD%" /M >nul 2>&1
setx MDB_SOURCE_FOLDER "%BASE_FOLDER%\Source" /M >nul 2>&1

echo ✓ 环境变量已设置
echo.

echo ===================================================
echo   安装完成！
echo ===================================================
echo.
echo 安装摘要:
echo   1. Microsoft Access Database Engine: 已安装 (%ARCH%)
echo   2. 文件夹结构: 已创建
echo   3. 环境变量: 已配置
echo.
echo 现在可以运行MDB文件导入工具了。
echo.
echo 使用方法:
echo   1. 将MDB文件放入: %BASE_FOLDER%\Source
echo   2. 运行: scripts\Run.bat -import
echo   3. 查看日志: logs\MDBImport_*.log
echo.
echo 按任意键退出...
pause >nul