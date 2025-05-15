@echo off
echo Running ETL process in manual test mode...
python run_etl.py --manual
echo.
echo Press any key to exit
pause > nul
