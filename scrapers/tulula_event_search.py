# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime
from bs4 import BeautifulSoup
import os
import glob
import json
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

# Establish configuration json and establish connection to CosmosDB
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

print('scraping tulu.la for events')
eventurls = []

# Query these items in SQL
query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "tulula_event_search.py" \
                   OR s.scriptname = "tulula_event_detailed.py"'}

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
    srcurl = 'https://tulu.la/events/?past=show'
else:
    srcurl = 'https://tulu.la/events/'

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
        num2 = int(t2['href'].split('page=')[-1])
    except Exception:
        num2 = 1
    if num2 > num1:
        num1 = num2
print(str(num1) + ' total pages...')

# Iterate through pages
for c1 in range(num1):
    dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(str(dtg) + ': ' + srcurl)
    time.sleep(2)
    pagetitle = soup.title.text
    elapsed_time = int(time.time() - start_time)
    print(str(elapsed_time) + ' seconds elapsed.')
    # Find and iterate through individual result entry JSONs
    jsons = soup.find_all("script", attrs={"type": "application/ld+json"})
    print(str(len(jsons) - 1) + ' events found on page...')
    for json1 in jsons[1:]:
        name = ''
        description = ''
        startDate = ''
        endDate = ''
        locality = ''
        eventurl = ''
        country = ''
        locality = ''
        streetAddress = ''
        venuename = ''
        zipcode = ''
        try:
            name = json.loads(json1.text)['name'].strip()
        except Exception:
            pass
        try:
            eventurl = json.loads(json1.text)['url'].strip()
        except Exception:
            pass
        try:
            description = json.loads(json1.text)['description'].strip()
        except Exception:
            pass
        try:
            startDate = datetime.strptime(json.loads(
                json1.text)['startDate'].strip(), '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")
            endDate = datetime.strptime(json.loads(
                json1.text)['endDate'].strip(), '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        try:
            country = json.loads(json1.text)[
                'location']['address']['addressCountry'].strip()
            locality = json.loads(json1.text)[
                'location']['address']['addressLocality'].strip()
            streetAddress = json.loads(json1.text)[
                'location']['address']['streetAddress'].strip()
            venuename = json.loads(json1.text)['location']['name'].strip()
            zipcode = json.loads(json1.text)[
                'location']['address']['postalCode'].strip()
        except Exception:
            pass
        # Store found item in CosmosDB if correctly parsed and not already scraped
        if name != '' and eventurl != '':
            if eventurl not in eventurls:
                eventurls = eventurls + [eventurl]
                item1 = client.CreateItem(os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), {
                    'name': name,
                    'description': description,
                    'startDate': startDate,
                    'endDate': endDate,
                    'streetAddress': streetAddress,
                    'venuename': venuename,
                    'zipcode': zipcode,
                    'locality': locality,
                    'country': country,
                    'eventurl': eventurl,
                    'srcurl': srcurl,
                    'pagetitle': pagetitle,
                    'scraperip': scraperip,
                    'hostname': hostname,
                    'scriptname': scriptname,
                    'dtg': dtg})
    if HISTORIC is True:
        srcurl = 'https://tulu.la/events/?page=' + str(c1 + 1) + '&past=show'
    else:
        srcurl = 'https://tulu.la/events/?page=' + str(c1 + 1)
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
