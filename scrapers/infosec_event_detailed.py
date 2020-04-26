# Import modules
import socket
import requests
import os
import re
from datetime import datetime
import time
import random
from bs4 import BeautifulSoup
import json
import azure.cosmos.cosmos_client as cosmos_client
import country_converter as coco

historic = False

start_time = time.time()
dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Fetch working directory path
workingdir = os.path.dirname(os.path.realpath(__file__))
if '/' in workingdir:
    workingdir = workingdir + '/'
else:
    workingdir = workingdir + '\\'

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Query these items in SQL
query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "infosec_event_detailed.py"'}
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

# Fetch search page URLs
useragent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0'
srcurls = ['https://infosec-conferences.com/category/conference-' + datetime.now().strftime("%Y") + '/?fwp_load_more=300',
           'https://infosec-conferences.com/filter/?fwp_load_more=300']

if historic is True:
    srcurls += ['https://infosec-conferences.com/filter/?fwp_date=%2C' + datetime.now().strftime("%Y-%m-%d") + '&fwp_load_more=500']

urllist = []
for srcurl in srcurls:
    print(srcurl)
    event_page = requests.get(
        srcurl, headers={'User-Agent': useragent}).content
    soup = BeautifulSoup(event_page, 'html.parser')
    boxes = soup.find_all('div', attrs={'class', 'fwpl-result'})
    if len(boxes) == 0:
        boxes = soup.find_all('div', attrs={'class', 'inside-article'})
    print(len(boxes), 'results')
    for box in boxes:
        try:
            urllist += [box.find('a')['href']]
        except Exception:
            pass
    time.sleep(3)

# Remove already scraped URLs
urllist = list(set(urllist) - set(urllist2))
print(str(len(urllist)) + ' URLs to be scraped.')

random.shuffle(urllist)
tc = 0

# Loop through URLs
for url in urllist:
    tc += 1
    print(str(tc) + ': ' + url)
    time.sleep(4 + int(random.random() * 4))
    # Fetch page content
    try:
        event_page = requests.get(
            url, headers={'User-Agent': useragent}).content
    except Exception:
        time.sleep(20)
        event_page = requests.get(
            url, headers={'User-Agent': useragent}).content
    soup = BeautifulSoup(event_page, 'html.parser')
    result = {}
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
    try:
        result['useragent'] = useragent
    except Exception:
        pass
    result['name'] = ''
    result['latitude'] = ''
    result['longitude'] = ''
    result['locality'] = ''
    result['country'] = ''
    result['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result['eventurl'] = url
    try:
        result['name'] = soup.find('meta', attrs={'property': 'og:title'})[
            'content'].strip()
    except Exception:
        pass
    try:
        result['description'] = soup.find('meta', attrs={'property': 'og:description'})[
            'content'].strip()
    except Exception:
        pass
    try:
        result['website'] = soup.find("a", text='Event Website')['href']
    except Exception:
        pass
    try:
        result['twitter'] = soup.find("a", href=re.compile(
            "https://twitter.com/"))['href'].split('twitter.com/')[1].split('?')[0]
        if result['twitter'] == 'share':
            result['twitter'] = ''
    except Exception:
        pass
    # Parse categories
    result['category'] = ''
    for i in soup.find_all('a', attrs={'rel': 'category tag'}):
        if 'Conference' not in i.text and 'Training' not in i.text and 'Academic' not in i.text:
            result['category'] += i.text + '|'
    result['category'] = result['category'][:-1]
    try:
        result['organizer'] = soup.find("a", href=re.compile(
            "https://infosec-conferences.com/event-series")).text.strip()
    except Exception:
        pass
    try:
        result['country'] = soup.find("a", href=re.compile(
            "https://infosec-conferences.com/country")).text
    except Exception:
        pass
    try:
        result['region'] = soup.find("a", href=re.compile(
            "https://infosec-conferences.com/us-state")).text
    except Exception:
        pass
    try:
        result['locality'] = soup.find("a", href=re.compile(
            "https://infosec-conferences.com/city")).text
    except Exception:
        pass
    # Parse date range string
    startDate = ''
    try:
        startDate = box.find(
            'i', attrs={'class': 'icon-calendar'}).parent.text.split('\xa0')[1]
        startDate = datetime.strptime(startDate.replace('th,', '').replace(
            'st,', '').replace('nd,', '').replace('rd,', ''), '%B %d %Y').strftime("%Y-%m-%d %H:%M:%S")
        endDate = box.find(
            'i', attrs={'class': 'icon-calendar'}).parent.text.split('\xa0')[3]
        endDate = datetime.strptime(endDate.replace('th,', '').replace('st,', '').replace(
            'nd,', '').replace('rd,', ''), '%B %d %Y').strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        endDate = startDate
    if startDate == '':
        try:
            startDate = soup.find('header', attrs={'class': 'entry-header'}).find(
                'i', attrs={'class': 'icon-calendar'}).parent.get_text('\n').split('\n')[0].strip().split('-')[0].strip()
            startDate = datetime.strptime(startDate.replace('th,', '').replace(
                'st,', '').replace('nd,', '').replace('rd,', ''), '%B %d %Y').strftime("%Y-%m-%d %H:%M:%S")
            endDate = soup.find('header', attrs={'class': 'entry-header'}).find(
                'i', attrs={'class': 'icon-calendar'}).parent.get_text('\n').split('\n')[0].strip().split('-')[-1].strip()
            endDate = datetime.strptime(endDate.replace('th,', '').replace('st,', '').replace(
                'nd,', '').replace('rd,', ''), '%B %d %Y').strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            endDate = startDate
    result['startDate'] = startDate
    result['endDate'] = endDate
    print(startDate, endDate)
    # Parse event description
    j = [i for i in soup.find(
        "span", attrs={'class': 'efav-span'}).next_siblings]
    description = ''
    for i in j:
        try:
            description += i.text.strip() + '\n\n'
        except Exception:
            pass
    result['description'] = description
    # Get coordinates for locality / country
    srcurl = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=' + \
        result['locality'] + ',%20' + result['country'] + \
        '&inputtype=textquery&fields=geometry&key=' + os.environ['GOOGLE']
    dict1 = json.loads(requests.get(srcurl).content)
    try:
        result['latitude'] = dict1['candidates'][0]['geometry']['location']['lat']
        result['longitude'] = dict1['candidates'][0]['geometry']['location']['lng']
    except Exception:
        pass
    print(result['latitude'], result['longitude'])
    # Convert country name to ISO abbreviation
    try:
        result['country'] = coco.convert(result['country'], to='ISO2')
    except Exception:
        pass
    if result['name'] != '' and result['eventurl'] != '' and result['latitude'] != '':
        print(result['name'])
        elapsed_time = int(time.time() - start_time)
        urllist2 += [result['eventurl']]
        print(str(elapsed_time) + ' seconds elapsed. ' + str(len(urllist2)) + ' total events scraped.')
        # Save result to Cosmos DB
        try:
            item1 = client.CreateItem(
                os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), result)
        except Exception:
            time.sleep(20)
            item1 = client.CreateItem(
                os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), result)
