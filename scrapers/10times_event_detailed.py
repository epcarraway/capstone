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

# Create Cosmos DB client
client = cosmos_client.CosmosClient(
    url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
        'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Query these items in SQL
query = {'query': 'SELECT distinct s.eventurl FROM server s \
    WHERE s.scriptname = "10times_event_search.py" \
    AND s.startDate > "' + dtg + '"'}

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
query = {'query': 'SELECT distinct s.eventurl FROM server s \
    WHERE s.scriptname = "10times_event_detailed.py" \
    AND s.startDate > "' + dtg + '"'}

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
        result['name'] = soup.find("meta", attrs={"property": "og:title"})[
            'content'].strip()
    except Exception:
        pass
    try:
        result['description'] = soup.find("meta", attrs={
            "property": "og:description"})[
            'content'][:1000].strip()
    except Exception:
        pass
    try:
        result['startDate'] = str(soup.find("input", attrs={"id": "dateList"})[
                                  'value']).split(',')[0]
        result['startDate'] = datetime.strptime(
            result['startDate'], '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    try:
        result['endDate'] = str(soup.find("input", attrs={"id": "dateList"})[
                                'value']).split(',')[-1]
        result['endDate'] = datetime.strptime(
            result['endDate'], '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    try:
        result['venuename'] = str(
            soup.find("input", attrs={"id": "venueName"})['value'])
    except Exception:
        pass
    try:
        result['streetAddress'] = json.loads(soup.find("script", attrs={
            "type": "application/ld+json"}).text)['location']['address']['streetAddress']
    except Exception:
        pass
    try:
        result['locality'] = str(
            soup.find("input", attrs={"id": "cityName"})['value'])
    except Exception:
        pass
    try:
        result['region'] = json.loads(soup.find("script", attrs={
            "type": "application/ld+json"}).text)['location']['address']['addressRegion']
    except Exception:
        pass
    try:
        result['country'] = str(
            soup.find("input", attrs={"id": "country_id"})['value'])
    except Exception:
        pass
    try:
        result['latitude'] = str(
            soup.find("span", attrs={"id": "event_latitude"})).split('>')[1].split('<')[0]
    except Exception:
        pass
    try:
        result['longitude'] = str(
            soup.find("span", attrs={"id": "event_longude"})).split('>')[1].split('<')[0]
    except Exception:
        pass
    try:
        result['zipcode'] = str(
            int(result['streetAddress'].split(',')[-2].split(' ')[-1]))
    except Exception:
        pass
    try:
        result['category'] = soup.find("td", attrs={"id": "hvrout2"}).get_text(
            separator='|').replace('| |', '|').replace('Category & Type|', '').strip()
    except Exception:
        pass
    try:
        result['interest'] = int(
            soup.find("a", attrs={"id": "go-btn"}).text.strip().split('\n')[0])
    except Exception:
        pass
    try:
        result['going'] = int(soup.find(
            "a", attrs={"id": "go-btn"}).text.strip().split(' Going')[0].split(' ')[-1])
    except Exception:
        pass
    try:
        result['hotelPrice'] = soup.find("p", attrs={"id": "htleve_0"})\
            .nextSibling.nextSibling.text.split('from ')[-1]
    except Exception:
        pass
    try:
        result['rating'] = json.loads(soup.find("script", attrs={
            "type": "application/ld+json"}).text)['aggregateRating']['ratingValue']
    except Exception:
        pass
    try:
        result['ratingCount'] = json.loads(soup.find("script", attrs={
            "type": "application/ld+json"}).text)['aggregateRating']['ratingCount']
    except Exception:
        pass
    try:
        result['eventPrice'] = soup.find("h2", attrs={"id": "visitorTicket"}).nextSibling.find(
            "span", attrs={"class": "text-muted"}).text
    except Exception:
        pass
    try:
        result['organizer'] = str(
            soup.find("h3", attrs={"id": "org-name"}).text)
    except Exception:
        try:
            result['organizer'] = str(
                soup.find("a", attrs={"id": "org-name"}).text)
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
