sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get dist-upgrade
pip3 install -r requirements.txt
sudo pip3 install -r requirements.txt
sudo apt-get install firefox-esr -y
rm geckodriver*
wget https://github.com/mozilla/geckodriver/releases/download/v0.23.0/geckodriver-v0.23.0-arm7hf.tar.gz
tar -xvzf geckodriver*
chmod +x geckodriver
mv geckodriver scrapers/geckodriver_23_arm7
rm geckodriver-*