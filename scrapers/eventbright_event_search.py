# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from selenium import webdriver
from datetime import datetime
import os
import glob
import random
import re
import json
import sys
from bs4 import BeautifulSoup
import time
import socket
import requests
from selenium.webdriver.firefox.options import Options

# Get local folder
start_time = time.time()
workingdir = os.path.dirname(os.path.realpath(__file__))
if '/' in workingdir:
    workingdir = workingdir + '/'
else:
    workingdir = workingdir + '\\'
sys.path.append(workingdir)

# Create temp log folder
current_directory = os.getcwd()
final_directory = os.path.join(current_directory, r'log')
if not os.path.exists(final_directory):
    os.makedirs(final_directory)
for logfile in glob.glob(workingdir + 'log\\geckodriver_*.log'):
    try:
        os.remove(logfile)
    except Exception:
        pass

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Query these items in SQL
query = {'query': 'SELECT distinct s.eventurl FROM server s WHERE s.scriptname = "eventbright_event_search.py" \
                   OR s.scriptname = "eventbright_event_detailed.py"'}

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
foundurls = urllist.copy()

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

# Get list of cities and generate URLs
print('scraping eventbrite.com for events')
dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
myregex = re.compile(r'/d/.+?/events/')
options = Options()
options.add_argument("--headless")
firefox_profile = webdriver.FirefoxProfile()
firefox_profile.set_preference("permissions.default.stylesheet", 2)
firefox_profile.set_preference("permissions.default.image", 2)
firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
browser = webdriver.Firefox(firefox_profile=firefox_profile, options=options,
                            service_log_path=workingdir + 'log\\geckodriver_' + str(int(random.random() * 1000000)) + '.log')
srcurl = 'https://www.eventbrite.com/directory/sitemap/'
print(str(dtg) + ': ' + srcurl)
time.sleep(4)
try:
    browser.get(srcurl)
except Exception:
    pass
time.sleep(4)
src = browser.page_source
soup = BeautifulSoup(src, 'html.parser')
urls = []
for a1 in soup.find_all('a'):
    try:
        if myregex.match(a1['href']) and '/russian-federation' not in a1['href'] and '/china' not in a1['href'] and '/pakistan' not in a1['href']:
            urls += ['https://www.eventbrite.com' + a1['href'].replace('/events/', '/science-and-tech--conferences/')]
    except Exception:
        pass
urls = list(set(urls))
print(str(len(urls)) + ' URLs found.')
browser.quit()

# Loop through country URLs to get event URLs
# foundurls = []
for url1 in urls:
    firefox_profile = webdriver.FirefoxProfile()
    firefox_profile.set_preference("permissions.default.stylesheet", 2)
    firefox_profile.set_preference("permissions.default.image", 2)
    firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
    browser = webdriver.Firefox(firefox_profile=firefox_profile, options=options,
                                service_log_path=workingdir + 'log\\geckodriver_' + str(int(random.random() * 1000000)) + '.log')
    useragent = browser.execute_script("return navigator.userAgent;")
    srcurl = url1
    print(str(dtg) + ': ' + srcurl)
    time.sleep(4)
    try:
        browser.get(srcurl)
    except Exception:
        pass
    time.sleep(2)
    src = browser.page_source
    soup = BeautifulSoup(src, 'html.parser')
    try:
        pages = int(soup.find(
            "div", attrs={"data-spec": "paginator__last-page-link"}).text.replace('.', ''))
    except Exception:
        try:
            pages = int(soup.find("li", attrs={
                        "data-spec": "eds-pagination__navigation-minimal"}).text.split('of')[-1].strip())
        except Exception:
            pages = 1
    print(str(pages) + ' pages found.')
    elapsed_time = int(time.time() - start_time)
    print(str(elapsed_time) + ' seconds elapsed. starting page number 1')
    if elapsed_time > 180000:
        browser.quit()
        quit()
    for page1 in range(1, pages + 1):
        dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        jsons = []
        try:
            in1 = list(
                set(soup.find_all("script", attrs={"type": "application/ld+json"})))
            jsons = json.loads(str(in1[0].text))
            print(str(len(jsons)) + ' events found on current page. ' + str(len(foundurls)) + ' total events founds.')
        except Exception:
            try:
                browser.get(srcurl)
            except Exception:
                pass
            time.sleep(10)
            src = browser.page_source
            soup = BeautifulSoup(src, 'html.parser')
            jsons = []
            try:
                in1 = list(
                    set(soup.find_all("script", attrs={"type": "application/ld+json"})))
                jsons = json.loads(str(in1[0].text))
                print(str(len(jsons)) + ' events found on current page ' + str(len(foundurls)) + ' total events founds.')
            except Exception:
                jsons = []
                print('no events found...')
        for json1 in jsons:
            dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            result1 = {}
            result1['dtg'] = dtg
            result1['hostname'] = hostname
            result1['scriptname'] = scriptname
            result1['srcurl'] = srcurl
            result1['useragent'] = useragent
            result1['pagetitle'] = soup.title.text.strip()
            try:
                result1['name'] = json1['name'].strip()
            except Exception:
                pass
            try:
                result1['eventurl'] = json1['url']
                result1['eid'] = result1['eventurl'].split('-')[-1]
            except Exception:
                pass
            try:
                result1['description'] = json1['description'].strip()
            except Exception:
                pass
            try:
                result1['startDate'] = str(json1['startDate']) + ' 00:00:00'
            except Exception:
                pass
            try:
                result1['endDate'] = json1['endDate'] + ' 00:00:00'
            except Exception:
                pass
            try:
                result1['lowPrice'] = str(
                    json1['offers']['priceCurrency']) + ' ' + str(json1['offers']['lowPrice'])
            except Exception:
                pass
            try:
                result1['highPrice'] = str(
                    json1['offers']['priceCurrency']) + ' ' + str(json1['offers']['highPrice'])
            except Exception:
                pass
            try:
                result1['venuename'] = json1['location']['name'].strip()
            except Exception:
                pass
            try:
                result1['streetAddress'] = json1['location']['address']['streetAddress'].strip(
                )
            except Exception:
                pass
            try:
                result1['locality'] = json1['location']['address']['addressLocality'].strip()
            except Exception:
                pass
            try:
                result1['region'] = json1['location']['address']['addressRegion'].strip()
            except Exception:
                pass
            try:
                result1['country'] = json1['location']['address']['addressCountry']['name'].strip()
            except Exception:
                pass
            try:
                result1['latitude'] = round(
                    float(json1['location']['geo']['latitude']), 7)
            except Exception:
                pass
            try:
                result1['longitude'] = round(
                    float(json1['location']['geo']['longitude']), 7)
            except Exception:
                pass
            if result1['name'] != '' and not result1['eventurl'] in foundurls:
                foundurls += [result1['eventurl']]
                # Store result in Cosmos
                item1 = client.CreateItem(
                    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), result1)
                time.sleep(1)
        if pages != 1:
            elapsed_time = int(time.time() - start_time)
            print(str(elapsed_time) + ' seconds elapsed. starting page number ' + str(page1) + ' of ' + str(pages))
            try:
                browser.get(srcurl + '?page=' + str(page1))
            except Exception:
                pass
            time.sleep(3)
            src = browser.page_source
            soup = BeautifulSoup(src, 'html.parser')
    browser.quit()
    print(str(len(foundurls)) + ' total records found.')
    time.sleep(4)

# Remove temporary logs
for logfile in glob.glob(workingdir + 'log//geckodriver_*.txt'):
    try:
        os.remove(logfile)
    except Exception:
        pass
