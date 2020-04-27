# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import os
import time
import random
import json
import sys

# Set parameters
TIME_LIMIT = 3600
WAIT_TIME = 4

# Get local folder and add project folder to PATH
workingdir = os.getcwd()
sys.path.insert(0, workingdir)
parentdir = os.path.dirname(workingdir)
sys.path.insert(0, parentdir)

# Import custom modules
from utils.scraping import update_time, scraper_info

# Get scraper info
scraperip, hostname, scriptname, dtg, start_time = scraper_info(__file__)

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Query these items in SQL
query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "eventbright_event_search.py" AND s.startDate > "' + dtg + '"'}

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
query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "eventbright_event_detailed.py" AND s.startDate > "' + dtg + '"'}

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
    result['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Findings field values using BeautifulSoup
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
    try:
        result['eid'] = result['eventurl'].split('-')[-1]
    except Exception:
        pass
    result['name'] = ''
    try:
        result['name'] = json.loads(soup.find("script", attrs={
                                    "type": "application/ld+json"}).text, strict=False)['name'].strip()
    except Exception:
        pass
    try:
        result['description'] = json.loads(soup.find("script", attrs={
                                           "type": "application/ld+json"}).text, strict=False)['description'][:2000].strip()
    except Exception:
        pass
    try:
        result['startDate'] = json.loads(soup.find("script", attrs={
                                         "type": "application/ld+json"}).text, strict=False)['startDate'].split('T')[0] + ' 00:00:00'
    except Exception:
        pass
    try:
        result['endDate'] = json.loads(soup.find("script", attrs={
                                       "type": "application/ld+json"}).text, strict=False)['endDate'].split('T')[0] + ' 00:00:00'
    except Exception:
        pass
    try:
        result['venuename'] = json.loads(soup.find("script", attrs={
                                         "type": "application/ld+json"}).text, strict=False)['location']['name']
    except Exception:
        pass
    try:
        result['streetAddress'] = json.loads(soup.find("script", attrs={
                                             "type": "application/ld+json"}).text, strict=False)['location']['address']['streetAddress']
    except Exception:
        pass
    try:
        result['locality'] = json.loads(soup.find("script", attrs={
                                        "type": "application/ld+json"}).text, strict=False)['location']['address']['addressLocality']
    except Exception:
        pass
    try:
        result['region'] = json.loads(soup.find("script", attrs={
                                      "type": "application/ld+json"}).text, strict=False)['location']['address']['addressRegion']
    except Exception:
        pass
    try:
        result['country'] = json.loads(soup.find("script", attrs={
                                       "type": "application/ld+json"}).text, strict=False)['location']['address']['addressCountry']
    except Exception:
        pass
    try:
        result['latitude'] = soup.find(
            "meta", attrs={"property": "event:location:latitude"})['content'].strip()
    except Exception:
        pass
    try:
        result['longitude'] = soup.find(
            "meta", attrs={"property": "event:location:longitude"})['content'].strip()
    except Exception:
        pass
    try:
        result['zipcode'] = json.loads(soup.find("script", attrs={
                                       "type": "application/ld+json"}).text, strict=False)['location']['address']['postalCode'].strip()
    except Exception:
        pass
    try:
        result['category'] = soup.find("a", attrs={
                                       "data-event-category": "listing"}).parent.parent.get_text('|').replace('\n|', '').strip()[:-1]
    except Exception:
        pass
    try:
        result['lowPrice'] = str(json.loads(soup.find("script", attrs={"type": "application/ld+json"}).text, strict=False)['offers'][0]['priceCurrency']) + ' ' + str(
            json.loads(soup.find("script", attrs={"type": "application/ld+json"}).text, strict=False)['offers'][0]['lowPrice'])
    except Exception:
        pass
    try:
        result['highPrice'] = str(json.loads(soup.find("script", attrs={"type": "application/ld+json"}).text, strict=False)['offers'][0]['priceCurrency']) + ' ' + str(
            json.loads(soup.find("script", attrs={"type": "application/ld+json"}).text, strict=False)['offers'][0]['highPrice'])
    except Exception:
        pass
    try:
        result['organizer'] = json.loads(soup.find("script", attrs={
                                         "type": "application/ld+json"}).text, strict=False)['organizer']['name'].strip()
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
