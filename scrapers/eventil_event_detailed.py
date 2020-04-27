# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import sys
import os
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
    query = {'query': 'SELECT distinct s.eventurl, s.lowPrice, s.highPrice FROM server s WHERE s.scriptname = "eventil_event_search.py" and s.startDate >= "2010-01"'}
else:
    query = {'query': 'SELECT distinct s.eventurl, s.lowPrice, s.highPrice FROM server s WHERE s.scriptname = "eventil_event_search.py" AND s.startDate > "' + dtg + '"'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 100

prices = []
# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
urllist = []
t1 = 0
for item in iter(result_iterable):
    t1 += 1
    if t1 % 1000 == 0:
        print(t1)
    try:
        urllist += [str(item['eventurl'])]
        price = {}
        price['eventurl'] = str(item['eventurl'])
        price['lowPrice'] = str(item['lowPrice'])
        price['highPrice'] = str(item['highPrice'])
        prices += [price]
    except Exception:
        pass

# Deduplicate URL list
urllist = list(set(urllist))
print(str(len(urllist)) + ' URLs found.')

# Query these items in SQL
if HISTORIC is True:
    query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "eventil_event_detailed.py"'}
else:
    query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "eventil_event_detailed.py" AND s.startDate > "' + dtg + '"'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 100

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
urllist2 = []
t1 = 0
for item in iter(result_iterable):
    t1 += 1
    if t1 % 1000 == 0:
        print(t1)
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
    time.sleep(3 + int(random.random() * 3))
    try:
        event_page = requests.get(url).content
    except Exception:
        time.sleep(20)
        event_page = requests.get(url).content
    soup = BeautifulSoup(event_page, 'html.parser')
    result = {}
    result['srcurl'] = url
    result['eventurl'] = url
    # Add prices from search
    for price in prices:
        if price['eventurl'] == result['eventurl']:
            result['lowPrice'] = price['lowPrice']
            result['highPrice'] = price['highPrice']
    result['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        result['name'] = json.loads(soup.find_all("script", attrs={
                                    "type": "application/ld+json"})[0].text, strict=False)['name'].strip()
    except Exception:
        pass
    try:
        result['description'] = BeautifulSoup(BeautifulSoup(json.loads(soup.find_all("script", attrs={"type": "application/ld+json"})[0].text, strict=False)[
                                              'description'], features="html.parser").get_text(separator="\n")[:2000].strip(), features="html.parser").get_text(separator="\n")[:2000].strip()
    except Exception:
        pass
    try:
        result['startDate'] = json.loads(soup.find_all("script", attrs={
                                         "type": "application/ld+json"})[0].text, strict=False)['startDate'].split(' ')[0] + ' 00:00:00'
    except Exception:
        pass
    try:
        result['endDate'] = json.loads(soup.find_all("script", attrs={
                                       "type": "application/ld+json"})[0].text, strict=False)['endDate'].split(' ')[0] + ' 00:00:00'
    except Exception:
        pass
    try:
        result['venuename'] = json.loads(soup.find_all("script", attrs={
                                         "type": "application/ld+json"})[0].text, strict=False)['location']['name'].strip()
    except Exception:
        pass
    try:
        result['streetAddress'] = json.loads(soup.find_all("script", attrs={
                                             "type": "application/ld+json"})[0].text, strict=False)['location']['address'].strip().split('\n')[0]
    except Exception:
        pass
    try:
        result['twitter'] = '@' + soup.find(
            "i", attrs={"class": "fa-twitter"}).parent['href'].split('/')[-1].strip()
    except Exception:
        pass
    try:
        result['website'] = soup.find(
            "i", attrs={"class": "fa-link"}).parent['href'].split('?')[0].strip()
    except Exception:
        pass
    result['category'] = ''
    categories2 = []
    try:
        for categories1 in soup.find("ul", attrs={"class": "topics list-unstyled"}).findall("a"):
            categories2 += [categories1['data-value'].strip()]
        result['category'] = '|'.join(categories2)
    except Exception:
        pass
    try:
        result['latitude'] = soup.find("a", string="Google Calendar")[
            'href'].split('location=')[-1].split('%')[0]
    except Exception:
        pass
    try:
        result['longitude'] = soup.find("a", string="Google Calendar")['href'].split(
            'location=')[-1].split('%2C')[-1].split('&')[0]
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
    if result['name'] != '' and result['eventurl'] != '':
        print(result['name'])
        print('categories: ' + result['category'])
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
