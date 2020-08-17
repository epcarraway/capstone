Invoke-WebRequest https://github.com/mozilla/geckodriver/releases/download/v0.27.0/geckodriver-v0.27.0-win64.zip -OutFile geckodriver.zip
Expand-Archive -LiteralPath geckodriver.zip -Force
Copy-Item -Path geckodriver\geckodriver.exe -Destination scrapers\geckodriver.exe
Remove-Item geckodriver\*.*
Remove-Item geckodriver.zip
Remove-Item geckodriver
pip3 install -r requirements.txt