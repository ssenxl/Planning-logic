@echo off
echo ============================================================
echo   BUILD KnitPlan.exe
echo ============================================================
echo.

cd /d "%~dp0"

echo [1/3] Building exe...
.venv\Scripts\pyinstaller.exe --onedir --name "KnitPlan" --noconfirm ^
    --add-data "*.py;." ^
    --add-data "data;data" ^
    --add-data "Estimate_Core;Estimate_Core" ^
    --collect-all pandas ^
    --collect-all numpy ^
    --collect-all openpyxl ^
    --collect-all oracledb ^
    --collect-all cryptography ^
    run_all.py

if errorlevel 1 (
    echo.
    echo BUILD FAILED
    pause
    exit /b 1
)

echo.
echo [2/3] Copying config and data files...
copy /y "config.ini" "dist\KnitPlan\config.ini" >nul
copy /y "run.bat" "dist\KnitPlan\run.bat" >nul
xcopy /e /i /y "data" "dist\KnitPlan\data" >nul
xcopy /e /i /y "Estimate_Core" "dist\KnitPlan\Estimate_Core" >nul

echo [3/4] Creating output folders...
if not exist "dist\KnitPlan\Booking"   mkdir "dist\KnitPlan\Booking"
if not exist "dist\KnitPlan\Stock"     mkdir "dist\KnitPlan\Stock"
if not exist "dist\KnitPlan\Order"     mkdir "dist\KnitPlan\Order"
if not exist "dist\KnitPlan\data_plan" mkdir "dist\KnitPlan\data_plan"

echo [4/4] Copying to KnitPlan_Release...
xcopy /e /i /y "dist\KnitPlan" "KnitPlan_Release" >nul

echo.
echo ============================================================
echo   Build complete!  Output: KnitPlan_Release\
echo ============================================================
echo.
pause
