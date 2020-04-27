# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from bs4 import BeautifulSoup
import country_converter as coco
import requests
import sys
import os
import re
import datetime
import time
import random
import json
import geopy

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

geo1 = geopy.geocoders.Nominatim(user_agent='contour')

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Query these items in SQL
if HISTORIC is True:
    query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "tulula_event_search.py"'}
else:
    query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "tulula_event_search.py" AND s.startDate > "' + dtg + '"'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 100

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
urllist = []
for item in iter(result_iterable):
    try:
        urllist += [str(item['eventurl'])]
    except Exception:
        pass

# Deduplicate URL list
urllist = list(set(urllist))
print(str(len(urllist)) + ' URLs found.')

# Query these items in SQL
if HISTORIC is True:
    query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "tulula_event_detailed.py"'}
else:
    query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "tulula_event_detailed.py" AND s.startDate > "' + dtg + '"'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 100

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
urllist2 = []
for item in iter(result_iterable):
    try:
        urllist2 += [str(item['eventurl'])]
    except Exception:
        pass

# Deduplicate URL list
urllist2 = list(set(urllist2))
print(str(len(urllist2)) + ' URLs already scraped.')

urllist = list(set(urllist) - set(urllist2))
print(str(len(urllist)) + ' URLs to be scraped.')

random.shuffle(urllist)
tc = 0

# Loop through URLs
for url in urllist:
    tc += 1
    print(str(tc) + ': ' + url)
    time.sleep(5 + int(random.random() * 5))
    try:
        event_page = requests.get(url).content
    except Exception:
        time.sleep(20)
        event_page = requests.get(url).content
    soup = BeautifulSoup(event_page, 'html.parser')
    result = {}
    result['srcurl'] = url
    result['eventurl'] = url
    result['dtg'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Find field values using BeautifulSoup
    try:
        result['scraperip'] = scraperip
    except Exception:
        pass
    try:
        result['hostname'] = hostname
    except Exception:
        pass
    try:
        result['scriptname'] = scriptname
    except Exception:
        pass
    result['name'] = ''
    result['latitude'] = ''
    result['longitude'] = ''
    result['streetAddress'] = ''
    result['locality'] = ''
    result['region'] = ''
    result['country'] = ''
    try:
        tempjsons = soup.find_all("script", attrs={"type": "application/ld+json"})
        for i in tempjsons:
            try:
                if json.loads(i.text, strict=False)['url'] == result['eventurl']:
                    tempjson = json.loads(i.text, strict=False)
            except Exception:
                pass
    except Exception:
        tempjson = {}
    try:
        result['name'] = tempjson['name'].strip()
    except Exception:
        pass
    try:
        result['description'] = tempjson['description'][:2000].strip()
    except Exception:
        pass
    try:
        result['startDate'] = tempjson['startDate'].split('T')[0] + ' 00:00:00'
    except Exception:
        pass
    try:
        result['endDate'] = tempjson['endDate'].split('T')[0] + ' 00:00:00'
    except Exception:
        pass
    try:
        result['venuename'] = tempjson['location']['name']
    except Exception:
        pass
    try:
        result['streetAddress'] = tempjson['location']['address']['streetAddress']
    except Exception:
        pass
    try:
        result['locality'] = tempjson['location']['address']['addressLocality']
    except Exception:
        pass
    try:
        result['region'] = tempjson['location']['address']['addressRegion']
    except Exception:
        pass
    try:
        result['country'] = tempjson['location']['address']['addressCountry']
    except Exception:
        pass
    try:
        result['zipcode'] = tempjson['location']['address']['postalCode'].strip()
    except Exception:
        pass
    result['category'] = ''
    categories2 = []
    categories = soup.find_all("a", href=re.compile("\/events\/\?about="))
    for category in categories:
        categories2 += [category.text]
    result['category'] = '|'.join(categories2)
    print(result['category'])
    try:
        j1 = json.loads(soup.find_all("script", string=re.compile(r'window.__APOLLO_STATE__={'))[
                        0].text.split('__APOLLO_STATE__=')[-1], strict=False)
        result['eid'] = list(j1.keys())[0].split(':')[-1]
        result['latitude'] = j1['$Event:{}.venue.coordinates'.format(
            result['eid'])]['latitude']
        result['longitude'] = j1['$Event:{}.venue.coordinates'.format(
            result['eid'])]['longitude']
        result['description'] = BeautifulSoup(j1['Event:{}'.format(
            result['eid'])]['description'], features="html.parser").get_text(separator="\n")[:2000].strip()
        result['website'] = j1['Event:{}'.format(result['eid'])]['url']
        result['twitter'] = j1['Event:{}'.format(result['eid'])]['twitter']
    except Exception:
        pass
    # Reverse geocode address/country information from lat/long if missing
    if (result['locality'] == '' or result['streetAddress'] == '') and result['longitude'] != '':
        print(result['latitude'], result['longitude'])
        try:
            tempadd = geo1.reverse('{}, {}'.format(
                result['latitude'], result['longitude']), language='en').raw['address']
        except Exception:
            pass
        if result['locality'] == '':
            try:
                result['locality'] = tempadd['city']
            except Exception:
                pass
            print(result['locality'])
        if result['streetAddress'] == '':
            try:
                result['streetAddress'] = tempadd['building']
            except Exception:
                pass
        if result['region'] == '':
            try:
                result['region'] = tempadd['state']
            except Exception:
                pass
        if result['country'] == '':
            try:
                result['country'] = tempadd['country_code'].upper()
            except Exception:
                pass
    if len(result['country']) > 2:
        try:
            result['country'] = coco.convert(result['country'], to='ISO2')
        except Exception:
            pass
    if result['name'] != '' and result['eventurl'] != '':
        print(result['name'])
        try:
            item1 = client.CreateItem(
                os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), result)
        except Exception:
            time.sleep(20)
            item1 = client.CreateItem(
                os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), result)
    # Increment and show elapsed time until limit reached
    update_time(start_time, TIME_LIMIT, WAIT_TIME)

# Clean up and end script
print('Script complete...')
