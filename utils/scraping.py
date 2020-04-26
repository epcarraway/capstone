# Import modules
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import logging
from selenium.webdriver.remote.remote_connection import LOGGER
import glob
import random
import os
import sys
import time
import platform
import re

def headless_browser():
    """Creates headless browser"""
    # Set directories for browser and logs
    workingdir = os.path.dirname(os.path.realpath(__file__))
    print('Appending {} to PATH...'.format(workingdir))
    sys.path.append(workingdir)
    current_directory = os.getcwd()
    final_directory = os.path.join(current_directory, r'log')
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
                                executable_path=os.path.join(current_directory, exec_name))
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
