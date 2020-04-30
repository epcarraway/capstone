# Import modules
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import logging
from selenium.webdriver.remote.remote_connection import LOGGER
from datetime import datetime
from datetime import timedelta
import glob
import random
import os
import sys
import time
import platform
import requests
import socket
import re


def headless_browser(fn, demo=False):
    """Creates headless browser"""
    # Set directories for browser and logs
    workingdir = os.path.dirname(os.path.realpath(fn))
    print('Appending {} to PATH...'.format(workingdir))
    sys.path.append(workingdir)
    final_directory = os.path.join(workingdir, r'log')
    # Create log folder if it doesn't exist and remove old logs
    if not os.path.exists(final_directory):
        os.makedirs(final_directory)
    for logfile in glob.glob(os.path.join(final_directory, r'*.log')):
        try:
            os.remove(logfile)
        except Exception:
            pass
    # Set selenium browser and profile defaults
    logname = os.path.join(final_directory, str(int(random.random() * 1000000)) + '.log')
    print('Saving browser log to {}'.format(logname))
    LOGGER.setLevel(logging.WARNING)
    options = Options()
    # Set browser to private browsing, headless and block rendering of images and CSS
    options.add_argument("--headless")
    firefox_profile = webdriver.FirefoxProfile()
    firefox_profile.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0")
    if not demo:
        firefox_profile.set_preference("permissions.default.stylesheet", 2)
        firefox_profile.set_preference("permissions.default.image", 2)
    firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
    # Determine platform to select geckodriver executable
    plat = platform.platform()
    if re.compile(r'Linux.*armv7.*').match(plat):
        exec_name = 'geckodriver_23_arm7'
    elif re.compile(r'Linux.*').match(plat):
        exec_name = 'geckodriver'
    else:
        exec_name = 'geckodriver.exe'
    print('Platform: {}... Using {} geckodriver executable...'.format(plat, exec_name))
    # Start browser
    browser = webdriver.Firefox(firefox_profile=firefox_profile, options=options,
                                service_log_path=logname, 
                                executable_path=os.path.join(workingdir, exec_name))
    browser.implicitly_wait(10)
    return browser


def update_time(start_time, end_seconds=0, wait_time=60):
    """Print elapsed time message in seconds, minutes or hours and quit script if limit exceeded"""
    if time.time() - start_time > 3600:
        message = '{} hours elapsed...'.format(int((time.time() - start_time)/3600))
    if time.time() - start_time > 120:
        message = '{} minutes elapsed...'.format(int((time.time() - start_time)/60))
    else:
        message = '{} seconds elapsed...'.format(int(time.time() - start_time))
    print(message)
    if end_seconds != 0 and (time.time() - start_time) > end_seconds:
        print('Time limit exceeded. Terminating script...')
        time.sleep(5)
        quit()
    else:
        time.sleep(wait_time)


def date_parse(dateraw):
    """Parse raw text date range strings from Google"""
    d1 = dateraw.split(' – ')[0]
    d2 = dateraw.split(' – ')[-1]
    d1 = d1.replace('Mon, ', '').replace('Tue, ', '').replace('Wed, ', '').replace(
        'Thu, ', '').replace('Fri, ', '').replace('Sat, ', '').replace('Sun, ', '')
    d2 = d2.replace('Mon, ', '').replace('Tue, ', '').replace('Wed, ', '').replace(
        'Thu, ', '').replace('Fri, ', '').replace('Sat, ', '').replace('Sun, ', '')
    if ',' in d2 and ('PM' in d2 or 'AM' in d2):
        try:
            year2 = int(d2.split(', ')[-2].strip())
        except Exception:
            year2 = int(datetime.now().strftime("%Y"))
    else:
        try:
            year2 = int(d2.split(', ')[-1].strip())
        except Exception:
            year2 = int(datetime.now().strftime("%Y"))
    if ' ' in d1:
        mon1 = d1.split(' ')[0].split(',')[0]
        day1 = d1.split(' ')[1].split(',')[0]
        if ', ' in d1:
            if '20' in d1.split(', ')[1]:
                try:
                    year1 = int(d1.split(', ')[1])
                except Exception:
                    year1 = year2
            else:
                year1 = year2
        else:
            year1 = year2
    if ' ' in d2:
        try:
            try:
                day2 = int(d2.split(',')[0].split(' ')[1])
            except Exception:
                day2 = int(d2.split(',')[0].split(' ')[0])
            try:
                mon2 = d2.split(',')[0].split(' ')[0]
            except Exception:
                mon2 = mon1
        except Exception:
            day2 = day1
            mon2 = mon1
    test1 = False
    try:
        test1 = int(mon2) < 60
    except Exception:
        test1 = False
    if test1 is True:
        mon2 = mon1
    if 'PM' in d2.split(',')[0] or 'AM' in d2.split(',')[0]:
        day2 = day1
        mon2 = mon1
    if datetime.strptime(str(day1) + ' ' + str(mon1) + ' ' + str(year1), '%d %b %Y') - datetime.now() < timedelta(days=-10):
        year2 += 1
        year1 += 1
    startDate = datetime.strptime(str(
        day1) + ' ' + str(mon1) + ' ' + str(year1), '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
    endDate = datetime.strptime(str(
        day2) + ' ' + str(mon2) + ' ' + str(year2), '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
    return(startDate, endDate)


def scraper_info(fn, demo=False):
    """Sets scraper metadata"""
    print('Getting scraper info...')
    try:
        scraperip = requests.get('https://api.ipify.org/').content.decode('utf8')
    except Exception:
        scraperip = ''
    try:
        hostname = socket.gethostname()
    except Exception:
        hostname = ''
    try:
        scriptname = os.path.basename(fn)
    except Exception:
        scriptname = ''
    dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    if demo:
        print('IP: XXX.XXX.XXX.XXX; Host: XXXX; script: {}; DTG: {}'.format(scriptname, dtg))
    else:
        print('IP: {}; Host: {}; script: {}; DTG: {}'.format(scraperip, hostname, scriptname, dtg))
    return scraperip, hostname, scriptname, dtg, start_time
