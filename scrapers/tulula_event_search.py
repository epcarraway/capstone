# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime
import os
import glob
import json
import sys
from bs4 import BeautifulSoup
import time
import socket
from urllib import request
import requests

historic = True

# Set working directory
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

# create base metadata for scraping output
dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
scraperip = requests.get('https://api.ipify.org/').content.decode('utf8')
hostname = socket.gethostname()
scriptname = os.path.basename(__file__)

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

if historic is True:
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
    # find and iterate through individual result entry JSONs
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
        # store found item in CosmosDB if correctly parsed and not already scraped
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
    if historic is True:
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
    time.sleep(4)

for logfile in glob.glob(workingdir + 'log//geckodriver_*.txt'):
    try:
        os.remove(logfile)
    except Exception:
        pass
