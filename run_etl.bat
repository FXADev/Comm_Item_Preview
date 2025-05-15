@echo off
echo Running Commission Item Preview ETL Process
echo Started at: %date% %time%

cd "c:\Users\FrankAhearn\Bridgepointe Technologies, Inc\BPT Business Operations - Documents\Documents\SQL\Comm_Item_Preview"
python run_etl.py

echo Completed at: %date% %time%
echo Check logs folder for detailed execution log
