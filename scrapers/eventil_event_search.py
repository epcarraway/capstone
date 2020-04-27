# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime
from bs4 import BeautifulSoup
import os
import glob
import random
import sys
import time
import requests

# Set parameters
TIME_LIMIT = 7200
WAIT_TIME = 4
HISTORIC = False

# Get local folder and add project folder to PATH
workingdir = os.getcwd()
sys.path.insert(0, workingdir)
parentdir = os.path.dirname(workingdir)
sys.path.insert(0, parentdir)

# Import custom modules
from utils.scraping import update_time, scraper_info

# Get scraper info
scraperip, hostname, scriptname, dtg, start_time = scraper_info(__file__)

# establish configuration json and establish connection to CosmosDB
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

print('scraping eventil.com for events')
eventurls = []

# Query these items in SQL
query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "eventil_event_search.py" \
                   OR s.scriptname = "eventil_event_detailed.py"'}

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

if HISTORIC is True:
    srcurl = 'https://eventil.com/events?q%5Bpast%5D=true'
else:
    srcurl = 'https://eventil.com/events/'
print(srcurl)

try:
    event_page = requests.get(srcurl).content
except Exception:
    time.sleep(20)
    event_page = requests.get(srcurl).content
soup = BeautifulSoup(event_page, 'html.parser')
result = {}

# Get page count
t1 = soup.find_all("a")
num1 = 1
for t2 in t1:
    try:
        num2 = int(t2['href'].split('?page=')[-1].split('&')[0])
    except Exception:
        num2 = 1
    if num2 > num1:
        num1 = num2
print(str(num1) + ' total pages...')

# Iterate through pages
for c1 in range(1, num1):
    dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(str(dtg) + ': ' + srcurl)
    time.sleep(2)
    pagetitle = soup.title.text
    elapsed_time = int(time.time() - start_time)
    print(str(elapsed_time) + ' seconds elapsed.')
    # find and iterate through individual result entry list items
    lis = soup.find_all("li", attrs={"class": "events-list__item"})
    print(str(len(lis)) + ' events found on page...')
    for li1 in lis:
        name = ''
        rawdate = ''
        startDate = ''
        eventurl = ''
        lowPrice = ''
        highPrice = ''
        streetAddress = ''
        try:
            name = li1.find(
                "h5", attrs={"class": "events-list__item--title"}).text.strip()
        except Exception:
            pass
        try:
            eventurl = 'https://eventil.com' + li1.div.a['href']
        except Exception:
            pass
        try:
            price = li1.find("span", attrs={"class": "price"}).text.strip()
            if '$' in price:
                lowPrice = price.split(
                    ' - ')[0].replace(',', '').replace('$CA', 'CAD ')
                lowPrice = price.split(
                    ' - ')[0].replace(',', '').replace('$', 'USD ')
                lowPrice = price.split(
                    ' - ')[0].replace(',', '').replace('USUSD', 'USD ')
                highPrice = price.split(
                    ' - ')[-1].replace(',', '').replace('$CA', 'USD ')
                highPrice = price.split(
                    ' - ')[-1].replace(',', '').replace('$', 'USD ')
                highPrice = price.split(
                    ' - ')[-1].replace(',', '').replace('USUSD', 'USD ')
        except Exception:
            pass
        try:
            rawdate = li1.find("h6", attrs={
                               "class": "events-list__item--date"}).text.strip().split(', ')[-1].split('  ')[0]
            startDate = datetime.strptime(
                rawdate, '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        try:
            streetAddress = li1.find(
                "p", attrs={"class": "events-list__item--location pl-3 pr-3"}).text.strip()
            if 'Online' in streetAddress:
                name = ''
        except Exception:
            pass
        # store found item in CosmosDB if correctly parsed and not already scraped
        if name != '' and eventurl != '':
            if eventurl not in eventurls:
                eventurls = eventurls + [eventurl]
                item1 = client.CreateItem(os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), {
                    'name': name,
                    'startDate': startDate,
                    'streetAddress': streetAddress,
                    'lowPrice': lowPrice,
                    'highPrice': highPrice,
                    'eventurl': eventurl,
                    'srcurl': srcurl,
                    'pagetitle': pagetitle,
                    'scraperip': scraperip,
                    'hostname': hostname,
                    'scriptname': scriptname,
                    'dtg': dtg})
    if HISTORIC is True:
        srcurl = 'https://eventil.com/events?page=' + str(c1 + 1) + '&q%5Bpast%5D=true'
    else:
        srcurl = 'https://eventil.com/events/?page=' + str(c1 + 1)
    try:
        event_page = requests.get(srcurl).content
    except Exception:
        time.sleep(20)
        event_page = requests.get(srcurl).content
    soup = BeautifulSoup(event_page, 'html.parser')
    print(str(len(eventurls)) + ' total records found.')
    # Increment and show elapsed time until limit reached
    update_time(start_time, TIME_LIMIT, WAIT_TIME)

# Clean up and end script
print('Script complete...')
