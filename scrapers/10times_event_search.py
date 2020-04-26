# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime
from bs4 import BeautifulSoup
from random import shuffle
import os
import sys
import time
import socket
import requests

# Get local folder and add project folder to PATH
start_time = time.time()
workingdir = os.getcwd()
sys.path.insert(0, workingdir)
parentdir = os.path.dirname(workingdir)
sys.path.insert(0, parentdir)

# Import custom modules
from utils.scraping import headless_browser

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
    # Start headless browser and fetch page info
    browser = headless_browser()
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
            if startDate == '':
                try:
                    startDate = box.find('data', attrs={"class": "eventTime"})['data-start-date'] + ' 00:00:00'
                    endDate = box.find('data', attrs={"class": "eventTime"})['data-end-date'] + ' 00:00:00'
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
