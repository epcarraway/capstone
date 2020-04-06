# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from selenium import webdriver
from bs4 import BeautifulSoup
import os
import glob
from random import shuffle
import json
import sys
import requests
from datetime import datetime
import time
import socket
from urllib import request
from selenium.webdriver.firefox.options import Options
import logging
from selenium.webdriver.remote.remote_connection import LOGGER


def dateParse(dateraw):
    # Define function to parse raw text date range strings from Google
    d1 = dateraw.split(' – ')[0]
    d2 = dateraw.split(' – ')[-1]
    d1 = d1.replace('Mon, ', '').replace('Tue, ', '').replace('Wed, ', '').replace(
        'Thu, ', '').replace('Fri, ', '').replace('Sat, ', '').replace('Sun, ', '')
    d2 = d2.replace('Mon, ', '').replace('Tue, ', '').replace('Wed, ', '').replace(
        'Thu, ', '').replace('Fri, ', '').replace('Sat, ', '').replace('Sun, ', '')
    if ',' in d2 and ('PM' in d2 or 'AM' in d2):
        try:
            year2 = int(d2.split(', ')[-2].strip())
        except Exception:
            year2 = int(datetime.now().strftime("%Y"))
    else:
        try:
            year2 = int(d2.split(', ')[-1].strip())
        except Exception:
            year2 = int(datetime.now().strftime("%Y"))
    if ' ' in d1:
        mon1 = d1.split(' ')[0].split(',')[0]
        day1 = d1.split(' ')[1].split(',')[0]
        if ', ' in d1:
            if '20' in d1.split(', ')[1]:
                try:
                    year1 = int(d1.split(', ')[1])
                except Exception:
                    year1 = year2
            else:
                year1 = year2
        else:
            year1 = year2
    if ' ' in d2:
        try:
            try:
                day2 = int(d2.split(',')[0].split(' ')[1])
            except Exception:
                day2 = int(d2.split(',')[0].split(' ')[0])
            try:
                mon2 = d2.split(',')[0].split(' ')[0]
            except Exception:
                mon2 = mon1
        except Exception:
            day2 = day1
            mon2 = mon1
    test1 = False
    try:
        test1 = int(mon2) < 60
    except Exception:
        test1 = False
    if test1 is True:
        mon2 = mon1
    if 'PM' in d2.split(',')[0] or 'AM' in d2.split(',')[0]:
        day2 = day1
        mon2 = mon1
    if datetime.strptime(str(day1) + ' ' + str(mon1) + ' ' + str(year1), '%d %b %Y') - datetime.now() < datetime.timedelta(days=-10):
        year2 += 1
        year1 += 1
    startDate = datetime.strptime(str(
        day1) + ' ' + str(mon1) + ' ' + str(year1), '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
    endDate = datetime.strptime(str(
        day2) + ' ' + str(mon2) + ' ' + str(year2), '%d %b %Y').strftime("%Y-%m-%d %H:%M:%S")
    return(startDate, endDate)


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

# Create base metadata for scraping output
start_time = time.time()
dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
scraperip = requests.get('https://api.ipify.org/').content.decode('utf8')
hostname = socket.gethostname()
scriptname = os.path.basename(__file__)

# Establish connection to CosmosDB
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Get local working directory
try:
    workingdir = os.path.dirname(os.path.realpath(__file__))
    if '/' in workingdir:
        workingdir = workingdir + '/'
    else:
        workingdir = workingdir + '\\'
except Exception:
    workingdir = '\\'
sys.path.append(workingdir)

# Establish temporary log directory for web driver
current_directory = os.getcwd()
final_directory = os.path.join(current_directory, r'log')
if not os.path.exists(final_directory):
    os.makedirs(final_directory)

for logfile in glob.glob(workingdir + 'log\\geckodriver_*.log'):
    try:
        os.remove(logfile)
    except Exception:
        pass

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

# Set selenium browser and profile defaults
LOGGER.setLevel(logging.WARNING)
f_options = Options()
f_options.add_argument("--headless")
firefox_profile = webdriver.FirefoxProfile()
firefox_profile.set_preference("permissions.default.stylesheet", 2)
firefox_profile.set_preference("permissions.default.image", 2)
firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
browser = webdriver.Firefox(firefox_profile=firefox_profile, options=f_options)
useragent = browser.execute_script("return navigator.userAgent;")
browser.implicitly_wait(10)

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
            # Drop 10times / eventbrite items and items with no locations
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
                    result['startDate'], result['endDate'] = dateParse(
                        result['date'])
                    del result['date']
                except Exception:
                    pass
                if result['name'] != '' and result['eventurl'] != '':
                    if not result['eventurl'] in eventurls:
                        eventurls = eventurls + [result['eventurl']]
                        elapsed_time = int(time.time() - start_time)
                        print(str(elapsed_time) + ' seconds elapsed.')
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

# Clean up browser
print('finishing...', len(results), 'results found...')
browser.quit()
time.sleep(2)
for logfile in glob.glob(workingdir + 'log//geckodriver_*.txt'):
    try:
        os.remove(logfile)
    except Exception:
        pass
