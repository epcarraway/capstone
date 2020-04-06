# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from selenium import webdriver
from datetime import datetime
import os
import glob
import random
from random import shuffle
import sys
from bs4 import BeautifulSoup
import time
import socket
import requests
from selenium.webdriver.firefox.options import Options
import logging
from selenium.webdriver.remote.remote_connection import LOGGER

# Notes on 10Times

# Url Structure:
# Conference type: https://10times.com/technology
# location: https://10times.com/washington-us OR https://10times.com/washington-us/technology
# Event: https://10times.com/mpls-sdn-world-congress

# create list of months and countries to iterate through
months = ['january',
          'february',
          'march',
          'april',
          'may',
          'june',
          'july',
          'august',
          'september',
          'october',
          'november',
          'december']

countries = ['usa',
             'unitedkingdom',
             'germany',
             'india',
             'australia',
             'canada',
             'singapore',
             'italy',
             'netherlands',
             'argentina',
             'austria',
             'azerbaijan',
             'bahrain',
             'bangladesh',
             'belgium',
             'brazil',
             'bulgaria',
             'cambodia',
             'chile',
             'colombia',
             'croatia',
             'czech-republic',
             'denmark',
             'egypt',
             'estonia',
             'finland',
             'france',
             'ghana',
             'greece',
             'hongkong',
             'hungary',
             'indonesia',
             'ireland',
             'israel',
             'japan',
             'jordan',
             'kazakhstan',
             'kenya',
             'kuwait',
             'latvia',
             'lithuania',
             'luxembourg',
             'malaysia',
             'mexico',
             'morocco',
             'nepal',
             'new-zealand',
             'nigeria',
             'norway',
             'oman',
             'peru',
             'philippines',
             'poland',
             'portugal',
             'qatar',
             'romania',
             'serbia',
             'slovakia',
             'slovenia',
             'southafrica',
             'korea',
             'spain',
             'sri-lanka',
             'sweden',
             'switzerland',
             'taiwan',
             'thailand',
             'turkey',
             'unitedarabemirates',
             'ukraine',
             'vietnam']

shuffle(months)
shuffle(countries)
start_time = time.time()
workingdir = os.path.dirname(os.path.realpath(__file__))
if '/' in workingdir:
    workingdir = workingdir + '/'
else:
    workingdir = workingdir + '\\'

sys.path.append(workingdir)

current_directory = os.getcwd()
final_directory = os.path.join(current_directory, r'log')
if not os.path.exists(final_directory):
    os.makedirs(final_directory)

for logfile in glob.glob(workingdir + 'log\\geckodriver_*.log'):
    try:
        os.remove(logfile)
    except Exception:
        pass

# establish configuration json and establish connection to CosmosDB
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Get scraper info
try:
    scraperip = requests.get('https://api.ipify.org/').content.decode('utf8')
except Exception:
    pass
try:
    hostname = socket.gethostname()
except Exception:
    pass
try:
    scriptname = os.path.basename(__file__)
except Exception:
    pass

print(str(len(countries)) + ' countries found.')
print('scraping 10times.com for events')
eventurls = []

# Query these items in SQL
query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "10times_event_search.py" \
                   OR s.scriptname = "10times_event_detailed.py"'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = -1

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
eventurls = []
for item in iter(result_iterable):
    try:
        eventurls += [str(item['eventurl'])]
    except Exception:
        pass

# Deduplicate URL list
eventurls = list(set(eventurls))
print(str(len(eventurls)) + ' URLs found.')

# iterate through countries
for c1 in countries:
    # set selenium browser and profile defaults
    LOGGER.setLevel(logging.WARNING)
    options = Options()
    options.add_argument("--headless")
    firefox_profile = webdriver.FirefoxProfile()
    firefox_profile.set_preference("permissions.default.stylesheet", 2)
    firefox_profile.set_preference("permissions.default.image", 2)
    firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
    browser = webdriver.Firefox(firefox_profile=firefox_profile, options=options,
                                service_log_path=workingdir + 'log\\geckodriver_' + str(int(random.random() * 1000000)) + '.log')
    useragent = browser.execute_script("return navigator.userAgent;")
    # iterate through months
    for m1 in months:
        dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        srcurl = 'https://10times.com/' + c1 + '/technology/conferences?month=' + m1
        print(str(dtg) + ': ' + srcurl)
        time.sleep(3)
        try:
            browser.get(srcurl)
        except Exception:
            pass
        time.sleep(3)
        # scroll to bottom of page
        for t1 in range(1, 100):
            browser.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(.05)
        # retrieve page source code and begin parsing
        src = browser.page_source
        soup = BeautifulSoup(src, 'html.parser')
        pagetitle = soup.title.text
        elapsed_time = int(time.time() - start_time)
        print(str(elapsed_time) + ' seconds elapsed.')
        # find and iterate through individual result entry blocks
        boxes = soup.find_all("tr", attrs={"class": "box"})
        print(str(len(boxes)) + ' events found on page...')
        for box in boxes:
            name = ''
            description = ''
            startDate = ''
            endDate = ''
            locality = ''
            eventurl = ''
            country = ''
            try:
                name = box.find_all('td')[1].find('a').text.strip()
            except Exception:
                pass
            if name == '':
                try:
                    name = box.find_all('td')[1].find('h2').text.strip()
                except Exception:
                    pass
            try:
                eventurl = box.find_all('td')[1].find('a')['href']
            except Exception:
                pass
            try:
                description = box.find_all('td')[3].text.strip()
            except Exception:
                pass
            try:
                t0 = str(box.find_all('td')[0]).strip().split(
                    '</td>')[0].split('<small class')[0].split('>')[-1]
                t1 = t0.split('-')[0].split(', ')[-1].split(' ')[0]
                t2 = t0.split('-')[0].split(', ')[-1].split(' ')[1]
                t3 = t0.split('-')[0].split(', ')[-1].split(' ')[2]
                t4 = t0.split('-')[-1].split(', ')[-1].split(' ')[0]
                t5 = t0.split('-')[-1].split(', ')[-1].split(' ')[1]
                t6 = t0.split('-')[-1].split(', ')[-1].split(' ')[2]
                if t1 == '':
                    t1 = t4
                if t2 == '':
                    t2 = t5
                if t3 == '':
                    t3 = t6
                startDate = datetime.strptime(
                    t1 + ' ' + t2 + ' ' + t3, '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
                endDate = datetime.strptime(
                    t4 + ' ' + t5 + ' ' + t6, '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
            try:
                locality = box.find_all('td')[2].text.strip()
                country = box.find_all('td')[2].find(
                    'a')['href'].split('/')[-2].split('-')[1].upper()
            except Exception:
                pass
            # store found item in CosmosDB if correctly parsed and not already scraped
            if name != '' and eventurl != '':
                if eventurl not in eventurls:
                    eventurls = eventurls + [eventurl]
                    item1 = client.CreateItem(os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), {
                        'name': name,
                        'description': description,
                        'startDate': startDate,
                        'endDate': endDate,
                        'locality': locality,
                        'country': country,
                        'eventurl': eventurl,
                        'srcurl': srcurl,
                        'pagetitle': pagetitle,
                        'scraperip': scraperip,
                        'useragent': useragent,
                        'hostname': hostname,
                        'scriptname': scriptname,
                        'dtg': dtg})
    print(str(len(eventurls)) + ' total records found.')
    time.sleep(4)
    browser.quit()
    time.sleep(4)

for logfile in glob.glob(workingdir + 'log//geckodriver_*.txt'):
    try:
        os.remove(logfile)
    except Exception:
        pass
