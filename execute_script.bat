@ECHO OFF 
TITLE Execute python script on anaconda environment
ECHO Please Wait...
:: Section 1: Activate the environment.
ECHO ============================
ECHO Conda Activate
ECHO ============================
@CALL "D:\Data\Software\Anaconda\Scripts\activate.bat" base
:: Section 2: Execute python script.
ECHO ============================
ECHO Python script running
ECHO ============================
python "D:\Data\Projects\Telegram_informer\market_informer.py"

ECHO ============================
ECHO End
ECHO ============================
