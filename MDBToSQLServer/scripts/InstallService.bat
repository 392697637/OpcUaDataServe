@echo off
chcp 65001 >nul
title MDB文件导入工具 - 服务安装程序

echo ===================================================
echo   MDB文件导入工具 Windows服务安装程序
echo ===================================================
echo.

setlocal

REM 检查管理员权限
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误: 需要管理员权限运行此脚本
    echo 请右键点击脚本，选择"以管理员身份运行"
    pause
    exit /b 1
)

set "APP_NAME=MDBImportService"
set "APP_DISPLAY_NAME=MDB文件导入服务"
set "APP_DESCRIPTION=定时将MDB文件导入到SQL Server数据库"
set "APP_PATH=%~dp0..\MDBToSQLServer.exe"
set "SERVICE_ARGS=-service"
set "LOG_PATH=%~dp0..\logs"

REM 检查应用程序是否存在
if not exist "%APP_PATH%" (
    echo 错误: 找不到应用程序 %APP_PATH%
    echo 请确保MDBToSQLServer.exe存在于正确的位置
    pause
    exit /b 1
)

echo 应用程序路径: %APP_PATH%
echo 服务参数: %SERVICE_ARGS%
echo 日志路径: %LOG_PATH%
echo.

REM 创建日志文件夹
if not exist "%LOG_PATH%" mkdir "%LOG_PATH%"

echo 正在创建Windows服务...
echo.

REM 使用sc命令创建服务
sc create %APP_NAME% binPath= "\"%APP_PATH%\" %SERVICE_ARGS%" DisplayName= "%APP_DISPLAY_NAME%" start= auto

if %errorlevel% neq 0 (
    echo 错误: 创建服务失败
    goto :cleanup
)

echo ✓ 服务创建成功
echo.

REM 设置服务描述
sc description %APP_NAME% "%APP_DESCRIPTION%"

if %errorlevel% neq 0 (
    echo ! 设置服务描述失败（忽略此错误）
) else (
    echo ✓ 服务描述已设置
)

echo.

REM 配置服务恢复选项
echo 配置服务恢复选项...
sc failure %APP_NAME% reset= 86400 actions= restart/5000/restart/10000/restart/30000

if %errorlevel% neq 0 (
    echo ! 配置恢复选项失败（忽略此错误）
) else (
    echo ✓ 恢复选项已配置
)

echo.

REM 创建事件日志源（可选）
echo 创建事件日志源...
eventcreate /L APPLICATION /T INFORMATION /SO %APP_NAME% /ID 1000 /D "MDB文件导入服务安装成功" >nul 2>&1

echo.

REM 创建服务配置文件
echo 创建服务配置文件...
set "CONFIG_FILE=%WINDIR%\system32\config\systemprofile\AppData\Local\%APP_NAME%\service.config"

mkdir "%WINDIR%\system32\config\systemprofile\AppData\Local\%APP_NAME%" >nul 2>&1

echo [Service] > "%CONFIG_FILE%"
echo Name=%APP_NAME% >> "%CONFIG_FILE%"
echo DisplayName=%APP_DISPLAY_NAME% >> "%CONFIG_FILE%"
echo Description=%APP_DESCRIPTION% >> "%CONFIG_FILE%"
echo Executable=%APP_PATH% >> "%CONFIG_FILE%"
echo Arguments=%SERVICE_ARGS% >> "%CONFIG_FILE%"
echo LogPath=%LOG_PATH% >> "%CONFIG_FILE%"
echo InstallDate=%DATE% %TIME% >> "%CONFIG_FILE%"

echo ✓ 服务配置文件已创建: %CONFIG_FILE%
echo.

echo ===================================================
echo   服务安装完成！
echo ===================================================
echo.
echo 服务信息:
echo   名称: %APP_NAME%
echo   显示名称: %APP_DISPLAY_NAME%
echo   描述: %APP_DESCRIPTION%
echo   启动类型: 自动
echo   执行文件: %APP_PATH%
echo.
echo 服务管理命令:
echo   net start %APP_NAME%    启动服务
echo   net stop %APP_NAME%     停止服务
echo   sc query %APP_NAME%     查询服务状态
echo   sc delete %APP_NAME%    删除服务
echo.
echo 服务日志位置:
echo   %LOG_PATH%\
echo   Windows事件查看器 -> 应用程序和服务日志
echo.
echo 是否立即启动服务？(Y/N)
set /p START_SERVICE=

if /i "%START_SERVICE%"=="Y" (
    echo.
    echo 正在启动服务...
    net start %APP_NAME%
    
    if %errorlevel% equ 0 (
        echo ✓ 服务启动成功
        sc query %APP_NAME%
    ) else (
        echo ✗ 服务启动失败
        echo 请检查事件查看器获取详细信息
    )
)

echo.
echo 安装完成！
echo 按任意键退出...
pause >nul
exit /b 0

:cleanup
echo.
echo 安装失败，正在清理...
sc delete %APP_NAME% >nul 2>&1
echo 已清理创建的服务
pause
exit /b 1