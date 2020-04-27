# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from bs4 import BeautifulSoup
from random import shuffle
from datetime import datetime
import os
import json
import sys
import requests
import time
import socket

# Set parameters
TIME_LIMIT = 3600
WAIT_TIME = 4

# Get local folder and add project folder to PATH
workingdir = os.getcwd()
sys.path.insert(0, workingdir)
parentdir = os.path.dirname(workingdir)
sys.path.insert(0, parentdir)

# Import custom modules
from utils.scraping import headless_browser, date_parse, update_time, scraper_info

# Get scraper info
scraperip, hostname, scriptname, dtg, start_time = scraper_info(__file__)

# Define search paramaters
search_list = ['technology conference',
               'information security conference',
               'analytics conference',
               'big data conference',
               'cyber security conference',
               'artificial intelligence conference',
               'quantum conference',
               'developer conference',
               'python conference',
               'hacker conference',
               'telecommunication conference',
               'open source conference',
               'gis conference',
               'cryptography conference',
               'government conference',
               'data science conference',
               'machine learning conference',
               'deep learning conference',
               'NLP conference',
               'data engineering conference',
               'IoT conference',
               '5G conference']
shuffle(search_list)

# Establish connection to CosmosDB
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

print('scraping google.com for events')
eventurls = []

# Create query to find existing Google event items in Cosmos DB
query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "google_event_detailed.py"'}
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
print(str(len(eventurls)) + ' URLs found in database...')

# Start headless browser and fetch page info
browser = headless_browser(__file__)
useragent = browser.execute_script("return navigator.userAgent;")

results = []

# Iterate through search topics
for search_item in search_list:
    print(search_item)
    browser.get('https://www.google.com/search?q=' + search_item.replace(' ', '+') + '&oq=techno&ibp=htl;events')
    time.sleep(2)
    # Scroll results window to load more items
    try:
        window1 = browser.find_element_by_xpath('//div[@jsname="CaV2mb"]')
        for i in range(1, 10):
            browser.execute_script(
                'arguments[0].scrollTop = arguments[0].scrollHeight', window1)
            time.sleep(2)
    except Exception:
        print('no results...')
    # Convert page source into BeautifulSoup object tree for searching
    src = browser.page_source
    soup = BeautifulSoup(src, 'html.parser')
    # Find all event boxes
    rboxes = soup.find_all("li", attrs={
                           "class": "PaEvOc hide-focus-ring tv5olb gws-horizon-textlists__li-ed"})
    print(len(rboxes), 'boxes found...')
    for rbox in rboxes:
        # Parse event box into result dictionary
        result = {}
        result['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result['useragent'] = useragent
        result['scraperip'] = scraperip
        result['hostname'] = hostname
        result['scriptname'] = scriptname
        result['ludocid'] = ''
        try:
            result['name'] = rbox.find(
                "div", attrs={"jsname": "r4nke"}).text.strip()
        except Exception:
            pass
        try:
            result['description'] = rbox.find(
                "span", attrs={"class": "PVlUWc"}).text.strip()
        except Exception:
            pass
        try:
            result['date'] = rbox.find(
                "div", attrs={"class": "Gkoz3"}).text.strip()
        except Exception:
            pass
        try:
            result['venuename'] = rbox.find(
                "div", attrs={"class": "RVclrc"}).text.strip()
        except Exception:
            pass
        try:
            result['streetAddress'] = rbox.find(
                "span", attrs={"class": "U6txu"}).text.strip()
        except Exception:
            pass
        try:
            result['ludocid'] = rbox.find("div", attrs={"class": "pzNwRe"}).find('a')[
                'href'].split('&ludocid=')[1].split('&')[0]
        except Exception:
            pass
        try:
            result['eventurl'] = rbox.find(
                "div", attrs={"class": "MwDRlf"}).find('a')['href']
        except Exception:
            pass
        # Drop training/network events
        if 'workshop' in result['name'].lower() or 'boot camp' in result['name'].lower() or 'bootcamp' in result['name'].lower() or 'training' in result['name'].lower() or 'meetup' in result['name'].lower() or 'networking event' in result['name'].lower():
            result['name'] == ''
        else:
            # Drop items with no locations
            if result['ludocid'] != '' and not result['eventurl'] in eventurls and not ('https://10times.com/' in str(rbox) or 'https://www.eventbrite.com/' in str(rbox)):
                print(result['ludocid'])
                # Extract location information from Google maps API
                srcurl = 'https://maps.googleapis.com/maps/api/place/details/json?cid=' + \
                    result['ludocid'] + '&fields=name,address_components,geometry,place_id&key=' + \
                    os.environ['GOOGLE']
                result['street_number'] = ''
                result['street'] = ''
                dict1 = json.loads(requests.get(srcurl).content)
                try:
                    result['latitude'] = dict1['result']['geometry']['location']['lat']
                    result['longitude'] = dict1['result']['geometry']['location']['lng']
                except Exception:
                    pass
                # Parse address JSON
                for i in dict1['result']['address_components']:
                    if 'locality' in i['types']:
                        result['locality'] = i['long_name']
                    elif 'country' in i['types']:
                        result['country'] = i['short_name']
                    elif 'administrative_area_level_1' in i['types']:
                        result['region'] = i['short_name']
                    elif 'postal_code' in i['types']:
                        result['zip'] = i['short_name']
                    elif 'street_number' in i['types']:
                        result['street_number'] = i['short_name']
                    elif 'route' in i['types']:
                        result['street'] = i['short_name']
                    try:
                        result['streetAddress'] = (
                            result['street_number'] + ' ' + result['street']).strip()
                    except Exception:
                        pass
                    try:
                        result['venuename'] = dict1['result']['name']
                    except Exception:
                        pass
                del result['street']
                del result['street_number']
                # Parse date range
                try:
                    result['startDate'], result['endDate'] = date_parse(
                        result['date'])
                    del result['date']
                except Exception:
                    pass
                if result['name'] != '' and result['eventurl'] != '':
                    if not result['eventurl'] in eventurls:
                        eventurls = eventurls + [result['eventurl']]
                        print(result['name'])
                        # Store result in Cosmos DB
                        try:
                            item1 = client.CreateItem(
                                os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), result)
                        except Exception:
                            time.sleep(20)
                            item1 = client.CreateItem(
                                os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), result)
                # Append results to list
                results += [result]
    # Increment and show elapsed time until limit reached
    update_time(start_time, TIME_LIMIT, WAIT_TIME)

# Clean up and end script
print('finishing...', len(results), 'results found...')
browser.quit()
