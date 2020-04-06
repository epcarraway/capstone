# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
import os
import re
import json
import sys
import requests
import socket
import time
import Levenshtein as lev
import pandas as pd
import country_converter as coco
import pickle
from nltk.corpus import stopwords

# Specify column names for input text and category, as well as number of key terms to return.
DESC_COLUMN = 'description'
CAT_COLUMN = 'category'
KEY_WORD_COUNT = 20
REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;:-]')
BAD_SYMBOLS_RE = re.compile('[^0-9a-z #+_]')
STOPWORDS = set(stopwords.words('english') +
                ['2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013', '2012'])
NUMBER_OF_CATEGORIES = 50

# Get local folder
start_time = time.time()
workingdir = os.getcwd()
if '/' in workingdir:
    workingdir = workingdir + '/'
else:
    workingdir = workingdir + '\\'
sys.path.append(workingdir)

# Create base metadata for scraping output
dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
try:
    scraperip = requests.get('https://api.ipify.org/').content.decode('utf8')
except Exception:
    scraperip = '0.0.0.0'
hostname = socket.gethostname()
try:
    scriptname = os.path.basename(__file__)
except Exception:
    scriptname = 'contour_best_merged.py'

# Update currency exchange rate data
ckey = 'https://openexchangerates.org/api/latest.json?app_id=' + \
    os.environ['CURRENCY']
crates = dict(json.loads(requests.get(ckey).content.decode('utf8')))['rates']
print('currency exchange rates updated...')


def toUSD(raw_currency, crates=crates):
    # Define function to convert raw currency values to normalized USD numeric values
    if type(raw_currency) == float:
        if not raw_currency > 0.0:
            return 0.0
        return round(raw_currency, 2)
    if type(raw_currency) == int:
        return float(raw_currency)
    clean_currency = str(raw_currency).strip()
    clean_currency = clean_currency.replace('CAUSD ', 'CAD ')
    clean_currency = clean_currency.replace('NZUSD ', 'NZD ')
    clean_currency = clean_currency.replace('MXUSD ', 'MXN ')
    clean_currency = clean_currency.replace('HKUSD ', 'HKD ')
    clean_currency = clean_currency.replace('USUSD ', 'USD ')
    clean_currency = clean_currency.replace('RUSD ', 'BRL ')
    clean_currency = clean_currency.replace('AUSD ', 'AUD ')
    if ' ' not in clean_currency:
        try:
            converted_currency = round(
                float(clean_currency.replace(',', '')), 2)
        except Exception:
            return 0.0
    if clean_currency[:3] == 'USD ':
        try:
            converted_currency = round(
                float(clean_currency[3:].replace(',', '')), 2)
        except Exception:
            return 0.0
    try:
        raw_num = float(clean_currency.split(' ')[1].replace(',', ''))
    except Exception:
        return 0.0
    raw_type = clean_currency.split(' ')[0].upper()
    try:
        converted_currency = round(raw_num / crates[raw_type], 2)
        return converted_currency
    except Exception:
        print(clean_currency + ' not parsed...')
        return 0.0


def hotelPriceFix(row):
    # Define function to fix raw hotel prices from 10times based on number of nights and distance cutoff
    try:
        d1 = int(row['distance'])
    except Exception:
        d1 = 0.0
    try:
        h1 = int(row['hotelPrice'])
    except Exception:
        h1 = 0.0
    if d1 < 50:
        h1 = 0.0
    else:
        d2 = (datetime.strptime(row['endDate'], '%Y-%m-%d %H:%M:%S') -
              datetime.strptime(row['startDate'], '%Y-%m-%d %H:%M:%S')).days + 2
        h1 = h1 * d2
    return h1


def calcHours(row):
    # Define function to calculate event hours
    h1 = ((datetime.strptime(row['endDate'], '%Y-%m-%d %H:%M:%S') -
           datetime.strptime(row['startDate'], '%Y-%m-%d %H:%M:%S')).days + 1) * 8
    return h1


def fixCategory(row):
    # Define function to fix categories
    cats = str(row[CAT_COLUMN]).replace('/ ', '/').split('|')
    newcats = []
    for cat in cats:
        if cat == 'agile tools':
            cat = 'Agile'
        if cat == 'agile methodologies':
            cat = 'Agile'
        if cat == 'agile':
            cat = 'Agile'
        if cat == 'ux':
            cat = 'UI/UX'
        if cat == 'user interaction':
            cat = 'UI/UX'
        if cat == 'Security & Defense':
            cat = 'Security'
        if cat == 'cloud security':
            cat = 'Cloud Security'
        if cat == 'cloud native security':
            cat = 'Cloud Security'
        if cat == 'crypto':
            cat = 'Cryptography'
        if cat == 'developers':
            cat = 'Software Development'
        if cat == 'developer':
            cat = 'Software Development'
        if cat == 'development':
            cat = 'Software Development'
        if cat == 'software development':
            cat = 'Software Development'
        if cat == 'functionalprogramming':
            cat = 'Functional Programming'
        if cat == 'IOT':
            cat = 'IoT'
        if cat == 'iot':
            cat = 'IoT'
        if cat == 'microsoft azure':
            cat = 'Azure'
        if cat == 'mobile':
            cat = 'Mobile'
        if cat == 'Artificial Intelligence':
            cat = 'AI/ML'
        if cat == 'Machine Learning':
            cat = 'AI/ML'
        if cat == 'architectures':
            cat = 'Architecture'
        if cat == 'micropython':
            cat = 'Python'
        if cat == 'pycon':
            cat = 'Python'
        if cat == 'pydata':
            cat = 'Python'
        if cat == 'start-up':
            cat = 'Startups'
        if cat == 'women':
            cat = 'Women in Tech'
        if cat == 'women empowerment':
            cat = 'Women in Tech'
        if cat == 'Women In Cybersecurity':
            cat = 'Women in Tech'
        if cat == 'women in tech':
            cat = 'Women in Tech'
        if cat == 'web design':
            cat = 'Web'
        if cat == 'data science':
            cat = 'Data Science'
        if cat == 'frontend':
            cat = 'Front End'
        if cat == 'product':
            cat = 'Product'
        if cat == 'privacy':
            cat = 'Privacy'
        if cat == 'privacy regulations':
            cat = 'Privacy'
        if cat == 'women leaders':
            cat = 'Women in Tech'
        if cat == 'forensic':
            cat = 'Forensics'
        if cat == 'forensics':
            cat = 'Forensics'
        if cat == 'data engineering':
            cat = 'Data Engineering'
        if cat == 'data visualization':
            cat = 'Data Visualization'
        if cat == 'analytics':
            cat = 'Analytics'
        if cat == 'api':
            cat = 'API'
        if cat == 'api design':
            cat = 'API'
        if cat == 'cpp':
            cat = 'c++'
        if cat == 'software testing':
            cat = 'Software Testing'
        if cat == 'azure':
            cat = 'Azure'
        if cat == 'aws':
            cat = 'AWS'
        if cat == 'gcp':
            cat = 'GCP'
        if cat == 'virtualization':
            cat = 'Virtualization'
        if cat == 'vr':
            cat = 'Virtual Reality'
        if cat == 'Government USA':
            cat = 'Government'
        if cat == 'BSides':
            cat = 'Security'
        if 'technology' in cat:
            cat = ''
        if 'general' in cat:
            cat = ''
        if 'networking' in cat:
            cat = ''
        if 'Business Services' in cat:
            cat = ''
        if 'Education & Training' in cat:
            cat = ''
        if 'Science & Research' in cat:
            cat = ''
        if 'Event' in cat:
            cat = ''
        if 'Conference' in cat:
            cat = ''
        if 'IT & Technology' in cat:
            cat = ''
        if 'Things to do' in cat:
            cat = ''
        if 'nements' in cat:
            cat = ''
        if 'choses' in cat:
            cat = ''
        if 'fazer' in cat:
            cat = ''
        if 'faire' in cat:
            cat = ''
        if 'Actividades' in cat:
            cat = ''
        if 'Evenementen' in cat:
            cat = ''
        if 'Conferências' in cat:
            cat = ''
        if 'Conferenties' in cat:
            cat = ''
        if 'Événement' in cat:
            cat = ''
        if 'Ã' in cat:
            cat = ''
        if cat != '':
            newcats += [cat]
    newcats = '|'.join(newcats)
    return newcats


def cleanDescription(row):
    # Define function to process freeform text in description
    rawdescription = str(row['description'])
    for i in range(1, 4):
        rawdescription = rawdescription.replace('\n\n\n', '\n\n')
        rawdescription = rawdescription.replace('   ', '  ')
    if 'href=' in rawdescription:
        soup = BeautifulSoup(rawdescription, 'html.parser')
        rawdescription = soup.text
    return rawdescription


def createSearch(row):
    # Define function to join all fields as searchable string
    return ' '.join(str(x) for x in row.tolist()).lower()


def createContourURL(row):
    # Define function to create contour event url
    rawurl = str(row['eventurl']).replace(
        '%20', '-').replace('https://', '').replace('http://', '')
    if rawurl[-1] == '/':
        rawurl = rawurl[:-1]
    if len(rawurl.split('/')) < 2:
        rawurl = ''
    else:
        rawurl = rawurl.split('?')[0].split(
            '-tickets-')[0].split('-registration-')[0].split('/')[-1][:100]
    if '.asp' in rawurl or '.php' in rawurl or '.cfm' in rawurl or '.jsp' in rawurl:
        rawurl = ''
    if len(rawurl) < 5:
        rawurl = str(row['name'])[:100]
    rawurl2 = ''
    for i in rawurl:
        if re.match('[A-Za-z0-9\- ]', i):
            rawurl2 += i
        else:
            rawurl2 += ''
    rawurl = rawurl2.lower().strip().replace(
        ' ', '-').replace('--', '-').replace('--', '-')
    rawurl = rawurl + '-' + row['startDate'].split(' ')[0]
    return rawurl


def combine_cat(row):
    # Define function to combine categories from classifier
    a1 = '|'.join(filter(None, list(
        set(str(row['category']).split('|') + str(row['new_category']).split('|')))))
    return a1


def clean_text(text):
    # Define function to clean text
    text = str(text).lower()  # Lowercase text
    # Replace REPLACE_BY_SPACE_RE symbols by space in text. Substitute the matched string in REPLACE_BY_SPACE_RE with space.
    text = REPLACE_BY_SPACE_RE.sub(' ', text)
    # Remove symbols which are in BAD_SYMBOLS_RE from text. Substitute the matched string in BAD_SYMBOLS_RE with nothing.
    text = BAD_SYMBOLS_RE.sub('', text)
    # Remove stopwords from text
    text = ' '.join(word for word in text.split() if word not in STOPWORDS)
    text = text.strip()
    return text


def diff_ratio(row):
    name = BAD_SYMBOLS_RE_NUM.sub(
        '', str(row['name']).lower()).split('(')[0][:100].strip()
    name = name.replace(' Tech ', ' Technology ')
    if name[:4] == 'The ':
        name = name[4:]
    name = name.split('|')[0].split(' - ')[0].split(' – ')[0].strip()
    name2 = BAD_SYMBOLS_RE_NUM.sub(
        '', str(row['name2']).lower()).split('(')[0][:100].strip()
    name2 = name2.replace(' Tech ', ' Technology ')
    name2 = name2.split('|')[0].split(' - ')[0].split(' – ')[0].strip()
    if name[:4] == 'The ':
        name = name[4:]
    Ratio = lev.ratio(name, name2)
    return Ratio


# Set cutoff date for query
olddtg = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
futuredtg = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")

# Query these items in SQL
query = {'query': 'SELECT * \
        FROM server s WHERE s.endDate >= "' + olddtg + '" AND s.endDate <= "' + futuredtg + '" AND s.latitude <> "" AND \
        (s.scriptname = "eventbright_event_detailed.py" OR s.scriptname = "infosec_event_detailed.py" OR \
        s.scriptname = "google_event_detailed.py" OR s.scriptname = "10times_event_detailed.py" OR \
        s.scriptname = "tulula_event_detailed.py" OR s.scriptname = "eventil_event_detailed.py" OR \
        s.scriptname = "expedia_hotel.py" OR s.scriptname = "expedia_flight.py") \
        ORDER BY s.dtg DESC'}

# load configuration json and establish connection to CosmosDB
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = -1
options['MaxDegreeOfParallelism'] = -1

# Execute query and iterate over results
print('Fetching data from Cosmos DB')
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
t1 = 0
for item in iter(result_iterable):
    try:
        resultlist += [item]
        t1 += 1
        if t1 % 1000 == 0:
            elapsed_time = int(time.time() - start_time)
            print(str(t1) + ' records retrieved... ' +
                  str(elapsed_time) + ' seconds elapsed.')
    except Exception:
        pass
print(str(len(resultlist)) +
      ' results retrieved from Cosmos DB detailed and flight/hotels')
event_search = resultlist.copy()

# Convert results dictionary to dataframe
df = pd.DataFrame(event_search)
print(str(len(df)) + ' rows in converted dataframe. Processing results into best merged answer...')

# Drop non-conference events
df['name'] = df['name'].fillna('')
name_black_list = [' 101',
            ' camp ',
            ' overview',
            '(online)',
            'beginner',
            'bootcamp',
            'breakfast',
            'certification',
            'certified',
            'class',
            'collaboration session',
            'course',
            'deep dive',
            'dinner',
            'friday',
            'fundamentals',
            'hack night',
            'hackathon',
            'hands-on',
            'happy hour',
            'how to ',
            'Intro to ',
            'introduction',
            'lecture',
            'lightning talk',
            'lunch',
            'meetup',
            'meet-up',
            'monday',
            'monthly',
            'networking event',
            'pitch night',
            'saturday',
            'small group',
            'social night',
            'study group',
            'study night',
            'sunday',
            'tbd',
            'thursday',
            'training',
            'tuesday',
            'wednesday',
            'workshop']

for i in name_black_list:
    df = df[-df['name'].str.lower().str.contains(i)].copy()

# Filter to 10 raw data
df_10 = df[df['scriptname'] == '10times_event_detailed.py'][['name', 'startDate', 'endDate', 'description', 'category', 'eventurl', 'organizer', 'venuename',
                                                             'streetAddress', 'locality', 'region', 'country', 'zipcode', 'latitude', 'longitude', 'eventPrice', 
                                                             'hotelPrice', 'lodging', 'meals', 'perdiem']]

# Fix 10 price and normalize lat/longs
df_10['eventPrice'] = df_10['eventPrice'].apply(lambda x: toUSD(x))
df_10['hotelPrice'] = df_10['hotelPrice'].apply(lambda x: toUSD(x))
df_10['latitude'] = df_10['latitude'].apply(lambda x: round(float(x), 6))
df_10['longitude'] = df_10['longitude'].apply(lambda x: round(float(x), 6))

# Filter to flight price data
df_flights = df[df['scriptname'] == 'expedia_flight.py'][[
    'eventurl', 'flightPrice', 'distance', 'flightRoute', 'flightDuration']]

# Join 10 data to flight data and finish normalization
df_10_left = pd.merge(df_10, df_flights, on='eventurl', how='left')
df_10_left['flightPrice'] = df_10_left['flightPrice'].apply(lambda x: toUSD(x))
df_10_left['distance'] = df_10_left['distance'].fillna('')
df_10_left['flightRoute'] = df_10_left['flightRoute'].fillna('')
df_10_left['flightDuration'] = df_10_left['flightDuration'].fillna('')
df_10_left['hotelPrice'] = df_10_left.apply(hotelPriceFix, axis=1)
df_10_left = df_10_left.fillna('')

# Filter to EB raw data
df_eb = df[df['scriptname'] == 'eventbright_event_detailed.py'][['name', 'startDate', 'endDate', 'description', 'category', 'eventurl', 'organizer',
                                                                 'venuename', 'streetAddress', 'locality', 'region', 'country', 'zipcode', 'latitude', 
                                                                 'longitude', 'lowPrice', 'lodging', 'meals', 'perdiem']]

# Fix EB price and normalize lat/longs
df_eb.rename(columns={"lowPrice": "eventPrice"}, inplace=True)
df_eb['eventPrice'] = df_eb['eventPrice'].apply(lambda x: toUSD(x))
df_eb['latitude'] = df_eb['latitude'].apply(lambda x: round(float(x), 6))
df_eb['longitude'] = df_eb['longitude'].apply(lambda x: round(float(x), 6))

# Filter to hotel price data
df_hotels = df[df['scriptname'] == 'expedia_hotel.py'][['eventurl', 'hotelPrice']]

# Join EB data to hotel and flight data and finish normalization
df_eb_left = pd.merge(df_eb, df_hotels, on='eventurl', how='left')
df_eb_left = pd.merge(df_eb_left, df_flights, on='eventurl', how='left')
df_eb_left['hotelPrice'] = df_eb_left['hotelPrice'].apply(lambda x: toUSD(x))
df_eb_left['flightPrice'] = df_eb_left['flightPrice'].apply(lambda x: toUSD(x))
df_eb_left['distance'] = df_eb_left['distance'].fillna('')
df_eb_left['flightRoute'] = df_eb_left['flightRoute'].fillna('')
df_eb_left['flightDuration'] = df_eb_left['flightDuration'].fillna('')
df_eb_left = df_eb_left.fillna('')

# Filter to TU raw data
df_tu = df[df['scriptname'] == 'tulula_event_detailed.py'][['name', 'startDate', 'endDate', 'description', 'category', 'eventurl', 'organizer',
                                                            'venuename', 'streetAddress', 'locality', 'region', 'country', 'zipcode', 'latitude',
                                                            'longitude', 'lowPrice', 'lodging', 'meals', 'perdiem']]

# Fix TU price and normalize lat/longs
df_tu.rename(columns={"lowPrice": "eventPrice"}, inplace=True)
df_tu['eventPrice'] = df_tu['eventPrice'].apply(lambda x: toUSD(x))
df_tu['latitude'] = df_tu['latitude'].apply(lambda x: round(float(x), 6))
df_tu['longitude'] = df_tu['longitude'].apply(lambda x: round(float(x), 6))

# Join TU data to hotel and flight data and finish normalization
df_tu_left = pd.merge(df_tu, df_hotels, on='eventurl', how='left')
df_tu_left = pd.merge(df_tu_left, df_flights, on='eventurl', how='left')
df_tu_left['hotelPrice'] = df_tu_left['hotelPrice'].apply(lambda x: toUSD(x))
df_tu_left['flightPrice'] = df_tu_left['flightPrice'].apply(lambda x: toUSD(x))
df_tu_left['distance'] = df_tu_left['distance'].fillna('')
df_tu_left['flightRoute'] = df_tu_left['flightRoute'].fillna('')
df_tu_left['flightDuration'] = df_tu_left['flightDuration'].fillna('')
df_tu_left = df_tu_left.fillna('')

# Filter to EL raw data
df_el = df[df['scriptname'] == 'eventil_event_detailed.py'][['name', 'startDate', 'endDate', 'description', 'category', 'eventurl', 'organizer',
                                                             'venuename', 'streetAddress', 'locality', 'region', 'country', 'zipcode', 'latitude', 
                                                             'longitude', 'lowPrice', 'lodging', 'meals', 'perdiem']]

# Fix EL price and normalize lat/longs
df_el.rename(columns={"lowPrice": "eventPrice"}, inplace=True)
df_el['eventPrice'] = df_el['eventPrice'].apply(lambda x: toUSD(x))
df_el['latitude'] = df_el['latitude'].apply(lambda x: round(float(x), 6))
df_el['longitude'] = df_el['longitude'].apply(lambda x: round(float(x), 6))

# Join EL data to hotel and flight data and finish normalization
df_el_left = pd.merge(df_el, df_hotels, on='eventurl', how='left')
df_el_left = pd.merge(df_el_left, df_flights, on='eventurl', how='left')
df_el_left['hotelPrice'] = df_el_left['hotelPrice'].apply(lambda x: toUSD(x))
df_el_left['flightPrice'] = df_el_left['flightPrice'].apply(lambda x: toUSD(x))
df_el_left['distance'] = df_el_left['distance'].fillna('')
df_el_left['flightRoute'] = df_el_left['flightRoute'].fillna('')
df_el_left['flightDuration'] = df_el_left['flightDuration'].fillna('')
df_el_left = df_el_left.fillna('')

# Filter to GO raw data
df_go = df[df['scriptname'] == 'google_event_detailed.py'][['name', 'startDate', 'endDate', 'description', 'category', 'eventurl', 'organizer',
                                                            'venuename', 'streetAddress', 'locality', 'region', 'country', 'zipcode', 'latitude', 
                                                            'longitude', 'lowPrice', 'lodging', 'meals', 'perdiem']]

# Fix GO price and normalize lat/longs
df_go.rename(columns={"lowPrice": "eventPrice"}, inplace=True)
df_go['eventPrice'] = df_go['eventPrice'].apply(lambda x: toUSD(x))
df_go['latitude'] = df_go['latitude'].apply(lambda x: round(float(x), 6))
df_go['longitude'] = df_go['longitude'].apply(lambda x: round(float(x), 6))

# Join GO data to hotel and flight data and finish normalization
df_go_left = pd.merge(df_go, df_hotels, on='eventurl', how='left')
df_go_left = pd.merge(df_go_left, df_flights, on='eventurl', how='left')
df_go_left['hotelPrice'] = df_go_left['hotelPrice'].apply(lambda x: toUSD(x))
df_go_left['flightPrice'] = df_go_left['flightPrice'].apply(lambda x: toUSD(x))
df_go_left['distance'] = df_go_left['distance'].fillna('')
df_go_left['flightRoute'] = df_go_left['flightRoute'].fillna('')
df_go_left['flightDuration'] = df_go_left['flightDuration'].fillna('')
df_go_left = df_go_left.fillna('')

# Filter to IN raw data
df_in = df[df['scriptname'] == 'infosec_event_detailed.py'][['name', 'startDate', 'endDate', 'description', 'category', 'eventurl', 'organizer',
                                                             'venuename', 'streetAddress', 'locality', 'region', 'country', 'zipcode', 'latitude', 
                                                             'longitude', 'lowPrice', 'lodging', 'meals', 'perdiem']]

# Fix IN price and normalize lat/longs
df_in.rename(columns={"lowPrice": "eventPrice"}, inplace=True)
df_in['eventPrice'] = df_in['eventPrice'].apply(lambda x: toUSD(x))
df_in['latitude'] = df_in['latitude'].apply(lambda x: round(float(x), 6))
df_in['longitude'] = df_in['longitude'].apply(lambda x: round(float(x), 6))

# Join IN data to hotel and flight data and finish normalization
df_in_left = pd.merge(df_in, df_hotels, on='eventurl', how='left')
df_in_left = pd.merge(df_in_left, df_flights, on='eventurl', how='left')
df_in_left['hotelPrice'] = df_in_left['hotelPrice'].apply(lambda x: toUSD(x))
df_in_left['flightPrice'] = df_in_left['flightPrice'].apply(lambda x: toUSD(x))
df_in_left['distance'] = df_in_left['distance'].fillna('')
df_in_left['flightRoute'] = df_in_left['flightRoute'].fillna('')
df_in_left['flightDuration'] = df_in_left['flightDuration'].fillna('')
df_in_left = df_in_left.fillna('')

# Concat updated EB, 10, TU, EL, GO and IN dataframes to combined dataframe and create search/URL fields
df_both = pd.concat([df_10_left, df_eb_left, df_tu_left,
                     df_el_left, df_go_left, df_in_left], sort=False)
df_both['contourURL'] = df_both.apply(createContourURL, axis=1)
df_both['category'] = df_both.apply(fixCategory, axis=1)

# Finish preparing combined dataframe
df_both['country_name'] = coco.convert(
    df_both['country'].tolist(), to='name_short', not_found=None)
df_both['contourURL'] = df_both.apply(createContourURL, axis=1)
df_both['category'] = df_both.apply(fixCategory, axis=1)
df_both['scraperip'] = scraperip
df_both['dtg'] = dtg
df_both['hostname'] = hostname
df_both['scriptname'] = scriptname
df_both['traininghours'] = df_both.apply(calcHours, axis=1)
df_both['country'] = df_both['country'].apply(lambda x: str(x))
df_both = df_both[(df_both['country'] != 'RU') & (df_both['country'] != 'CN') & (df_both['country'].str.len() == 2)].copy()

# Deduplicate results
df_both.sort_values(by=['dtg'], ascending=False, inplace=True)
df_both.drop_duplicates(subset=['name', 'scriptname', 'latitude', 'longitude', 'startDate'], inplace=True)
df_both.drop_duplicates(subset=['contourURL'], inplace=True)
print(str(len(df_both)) + ' records in merged DataFrame')
for i1 in range(3):
    # Extract and index match columns
    df1 = df_both[['contourURL', 'name', 'startDate', 'country', 'latitude', 'longitude']].set_index(['startDate', 'country']).copy()
    df2 = df_both[['contourURL', 'name', 'startDate', 'country', 'latitude', 'longitude']].set_index(['startDate', 'country']).copy()
    df2.columns = ['contourURL2', 'name2', 'latitude2', 'longitude2']
    # Join and find distance / date / country matches
    dfj1 = df1.join(df2)
    dfj1 = dfj1[dfj1['contourURL'] != dfj1['contourURL2']]
    dfj1 = dfj1[((dfj1['latitude'] - dfj1['latitude2'])**2 +
                (dfj1['longitude'] - dfj1['longitude2'])**2)**0.5 < 0.01].copy()
    # Define string matching function
    BAD_SYMBOLS_RE_NUM = re.compile('[0-9]{2,}')
    # Filter possible matches by string similarity
    dfj1['diff'] = dfj1.apply(diff_ratio, axis=1)
    dfj1a = dfj1[dfj1['diff'] >= .9].copy()
    # Join and find distance / date / country matches
    dfj1 = df1.join(df2)
    dfj1 = dfj1[dfj1['contourURL'] != dfj1['contourURL2']]
    dfj1 = dfj1[((dfj1['latitude'] - dfj1['latitude2'])**2 +
                (dfj1['longitude'] - dfj1['longitude2'])**2)**0.5 < 0.1].copy()
    # Filter possible matches by string similarity
    dfj1['diff'] = dfj1.apply(diff_ratio, axis=1)
    dfj1b = dfj1[dfj1['diff'] >= .95].copy()
    # Join and find distance / date / country matches
    dfj1 = df1.join(df2)
    dfj1 = dfj1[dfj1['contourURL'] != dfj1['contourURL2']]
    dfj1 = dfj1[((dfj1['latitude'] - dfj1['latitude2'])**2 +
                (dfj1['longitude'] - dfj1['longitude2'])**2)**0.5 < 0.2].copy()
    # Filter possible matches by string similarity
    dfj1['diff'] = dfj1.apply(diff_ratio, axis=1)
    dfj1c = dfj1[dfj1['diff'] >= .999].copy()
    dfj1 = pd.concat([dfj1a, dfj1b, dfj1c], ignore_index=True)
    dfj1.drop_duplicates(['contourURL', 'contourURL2'], inplace=True)
    # Build list of unique pairs
    r1 = dfj1['contourURL'].tolist()
    r2 = dfj1['contourURL2'].tolist()
    s1 = []
    s2 = []
    for i, j in zip(r1, r2):
        if j not in s1 and i not in s2:
            s1 += [i]
            s2 += [j]
    print(str(len(s1)) + ' duplicates found...')
    alldupes = list(set(s1 + s2))
    print(str(len(alldupes)) + ' unique event URLs found in dupes...')
    df_both_sub = df_both[df_both['contourURL'].isin(alldupes)].copy()
    df_both_none = df_both[~df_both['contourURL'].isin(alldupes)].copy()
    print(str(len(df_both_none)) + ' non-duped rows.')
    print(str(len(df_both_sub)) + ' duped rows to merge.')
    merged = []
    tc = 0
    # Iterate through match pairs and combine column values
    for i, j in zip(s1, s2):
        tc += 1
        dfi = df_both_sub[df_both_sub['contourURL'] == i].head(1).copy()
        dfj = df_both_sub[df_both_sub['contourURL'] == j].head(1).copy()
        if len(dfi) > 0 and len(dfj) > 0:
            print(tc, ' | ', dfi['name'].values[0], ' | ', dfj['name'].values[0])
            if len(str(dfi['streetAddress'].values[0])) > len(str(dfj['streetAddress'].values[0])):
                dfj['latitude'] = dfi['latitude'].values[0]
                dfj['longitude'] = dfi['longitude'].values[0]
            else:
                dfi['latitude'] = dfj['latitude'].values[0]
                dfi['longitude'] = dfj['longitude'].values[0]
            dfi['category'] = '|'.join(filter(None, list(set(
                dfi['category'].values[0].split('|') + dfj['category'].values[0].split('|')))))
            dfj['category'] = dfi['category'].values[0]
            for col in dfi.columns:
                if len(str(dfi[col].values[0])) > len(str(dfj[col].values[0])):
                    dfj[col] = dfi[col].values[0]
                else:
                    dfi[col] = dfj[col].values[0]
            dfi['contourURL'] = i
            dfj['contourURL'] = j
            merged += [dfi.copy(), dfj.copy()]
    # merged = pd.concat(merged, ignore_index=True)
    df_both = pd.concat([df_both_none] + merged, ignore_index=True)
    # Drop duplicate values
    print(str(len(df_both)) + ' records before fuzzy aggregation')
    df_both.drop_duplicates(subset="eventurl", inplace=True)
    print(str(len(df_both)) + ' records after fuzzy aggregation')

# Clean description
print('Applying processing to descriptions...')
df_both['description'] = df_both.apply(cleanDescription, axis=1)

# Select model for categorization
local_file_name = 'tfidf-model-2020-03-05.p'
local_path = ''
download_file_path = os.path.join(local_path, local_file_name)

if not os.path.exists(download_file_path):
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(
        os.environ['AZURE_STORAGE_CONNECTION_STRING'])
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(
        container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name)
    print("\nDowloading from Azure Storage blob:\n\t" + local_file_name)
    # Download the blob to a local file
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

# Load pickled data
file = open(local_file_name, 'rb')
model = pickle.load(file)
file.close()

# Select vectorizer
local_file_name = 'vectorizer-2020-03-05.p'
local_path = ''
download_file_path = os.path.join(local_path, local_file_name)

if not os.path.exists(download_file_path):
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(
        os.environ['AZURE_STORAGE_CONNECTION_STRING'])
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(
        container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name)
    print("\nDowloading from Azure Storage blob:\n\t" + local_file_name)
    # Download the blob to a local file
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

# Load pickled data
file = open(local_file_name, 'rb')
vectorizer = pickle.load(file)
file.close()

# Predict category
df_both['new_category'] = model.predict(vectorizer.transform(
    (df_both['name'] + ' ' + df_both['description']).apply(clean_text)))
df_both['category'] = df_both.apply(combine_cat, axis=1)

# Add search string
df_both['search'] = df_both.apply(createSearch, axis=1)

# Drop duplicate values
df_both.drop_duplicates(subset="eventurl", inplace=True)
df_both.drop_duplicates(subset="contourURL", inplace=True)
print(str(len(df_both)) + ' combined rows in dataframe.')

# Define query to retrieve event page views
query = {'query': 'SELECT s.username, s.userurl FROM server s WHERE s.scriptname = "app.py" and \
                   CONTAINS(s.userurl, "contour4.azurewebsites.net/e/")'}

# Execute query and iterate over results
print('Fetching page view data.')
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
results = []
for item in iter(result_iterable):
    try:
        results += [item]
    except Exception:
        pass
print(len(results), 'total event views')

# Create summary of page views
dfa = pd.DataFrame(results)
dfa = dfa.groupby(['userurl', 'username'])['username'].agg(['count'])
dfa = dfa.groupby(['userurl'])['count'].agg(['sum', 'count'])
dfa.reset_index(inplace=True)
dfa.columns = ['contourURL', 'total', 'unique']
dfa['contourURL'] = dfa['contourURL'].apply(lambda x: str(x).split('/e/')[-1])
dfa = dfa[dfa['contourURL'] != ''].copy()

# Join page view counts to event rows
df_both = pd.merge(df_both, dfa, on='contourURL', how='left')
df_both['total'] = df_both['total'].fillna(0)
df_both['unique'] = df_both['unique'].fillna(0)
df_both = df_both.astype({'total': 'int32', 'unique': 'int32'})

newurls = df_both['contourURL'].tolist()

# Define query to retrieve manually generated/edited pages
query = {'query': 'SELECT s.contourURL FROM server s WHERE s.scriptname = "contour_manual.py"'}

# Execute query and iterate over results
print('Fetching manual page data.')
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
results = []
for item in iter(result_iterable):
    try:
        results += [item['contourURL']]
    except Exception:
        pass
print(len(results), 'total manual pages. Removing from new merged list.')
manual_list = results.copy()
df_both = df_both[-df_both['contourURL'].isin(manual_list)].copy()

# Define query to get existing best merged results
query = {'query': 'SELECT * FROM server s WHERE s.scriptname = "contour_best_merged.py"'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = -1
options['MaxDegreeOfParallelism'] = -1

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
tc = 0
print('Querying Cosmos DB for existing merged data')
for item in iter(result_iterable):
    tc += 1
    if tc % 1000 == 0:
        print(tc)
    try:
        resultlist += [item]
    except Exception:
        pass
print(str(len(resultlist)) + ' old results in merged dataset')
old_results = resultlist.copy()
old_merge_df = pd.DataFrame(resultlist).set_index(['contourURL'])
old_merge_df = old_merge_df[['dtg', '_self', 'id', '_rid', '_etag', '_attachments', '_ts']]
old_merge_df.columns = ['dtg2', '_self', 'id', '_rid', '_etag', '_attachments', '_ts']

# Split merge DataFrame into upsert and insert DataFrames
df_both_upsert = df_both.set_index(['contourURL']).join(old_merge_df)
df_both_upsert.reset_index(inplace=True)
df_both_upsert.fillna('', inplace=True)
df_both_insert = df_both_upsert[df_both_upsert['dtg2'] == ''].copy()
df_both_insert.sort_values(by=['dtg'], ascending=False, inplace=True)
df_both_insert['dupe'] = df_both_insert.duplicated(subset=['eventurl'])
insert_dupe_delete_list = df_both_insert[df_both_insert['dupe'] == 1][['contourURL', 'dtg', '_self']].to_dict(orient='records')
df_both_insert.drop(columns=['dupe', 'dtg2', '_self', 'id', '_rid', '_etag', '_attachments', '_ts'], inplace=True)
df_both_insert.drop_duplicates(subset=['contourURL'], inplace=True)
df_both_upsert = df_both_upsert[df_both_upsert['dtg2'] != ''].copy()
df_both_upsert['dtg'] = df_both_upsert['dtg2']
df_both_upsert.sort_values(by=['dtg'], ascending=False, inplace=True)
df_both_upsert['dupe'] = df_both_upsert.duplicated(subset=['eventurl'])
upsert_dupe_delete_list = df_both_upsert[df_both_upsert['dupe'] == 1][['contourURL', 'dtg', '_self']].to_dict(orient='records')
df_both_upsert.drop_duplicates(subset=['contourURL'], inplace=True)
df_both_upsert.drop(columns=['dtg2', 'dupe'], inplace=True)
df_both_delete = old_merge_df.join(df_both.set_index(['contourURL']))
df_both_delete.reset_index(inplace=True)
df_both_delete.fillna('', inplace=True)
df_both_delete = df_both_delete[df_both_delete['dtg'] == ''].copy()
df_both_delete.sort_values(by=['dtg2'], ascending=False, inplace=True)
df_both_delete['dtg'] = df_both_delete['dtg2']
df_both_delete = df_both_delete[['contourURL', 'dtg', '_self']]
delete_list = df_both_delete.to_dict(orient='records')
delete_list += insert_dupe_delete_list + upsert_dupe_delete_list
upsert_list = df_both_upsert.to_dict('records')

# Skip upserts on records with no updates
print('Checking for upsert changes')
upsert_list_2 = []
for j in upsert_list:
    for i in old_results:
        if j['contourURL'] == i['contourURL']:
            j1 = j.copy()
            i1 = i.copy()
            for k in ['_self', 'id', '_rid', '_etag', '_attachments', '_ts', 'search']:
                try:
                    del j1[k]
                except Exception:
                    pass
                try:
                    del i1[k]
                except Exception:
                    pass
            for k in ['latitude', 'longitude', 'eventPrice', 'hotelPrice', 'lodging',
                      'meals', 'perdiem', 'flightPrice', 'distance']:
                try:
                    i1[k] = float(i1[k])
                except Exception:
                    pass
                try:
                    j1[k] = float(j1[k])
                except Exception:
                    pass
            if i1 != j1:
                upsert_list_2 += [j]
                if len(upsert_list_2) % 500 == 0:
                    print(len(upsert_list_2))
                break
print(len(upsert_list_2))
upsert_list = upsert_list_2.copy()

print(str(len(upsert_list)) + ' records to upsert.')
print(str(len(df_both_insert)) + ' records to insert.')
print(str(len(delete_list)) + ' records to delete.')

# Upsert new data in Cosmos DB
t1 = 0
for item in upsert_list:
    if item['eventurl'] != '' and item['name'] != '':
        try:
            client.UpsertItem(os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace(
                '-', '='), item)
        except Exception:
            print('document upsert failed... retrying...')
            time.sleep(10)
            try:
                client.UpsertItem(os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace(
                    '-', '='), item)
            except Exception:
                print('upsert failed...')
        t1 += 1
        if t1 % 100 == 0:
            elapsed_time = int(time.time() - start_time)
            print(str(t1) + ' records upserted... ' +
                  str(elapsed_time) + ' seconds elapsed.')

elapsed_time = int(time.time() - start_time)
print(str(t1) + ' records upserted in Cosmos DB... ' +
      str(elapsed_time) + ' seconds elapsed.')

# Insert new data in Cosmos DB
insert_list = df_both_insert.to_dict('records')
t1 = 0
for item in insert_list:
    if item['eventurl'] != '' and item['name'] != '':
        try:
            item1 = client.CreateItem(
                os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), item)
        except Exception:
            print('document insert failed... retrying...')
            time.sleep(10)
            try:
                item1 = client.CreateItem(
                    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), item)
            except Exception:
                print('insert failed...')
        t1 += 1
        if t1 % 100 == 0:
            elapsed_time = int(time.time() - start_time)
            print(str(t1) + ' records created... ' +
                  str(elapsed_time) + ' seconds elapsed.')

elapsed_time = int(time.time() - start_time)
print(str(t1) + ' records created in Cosmos DB... ' +
      str(elapsed_time) + ' seconds elapsed.')

# Delete old data from Cosmos DB
t1 = 0
for item in delete_list:
    t1 += 1
    try:
        client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})
    except Exception:
        print('document deletion failed... retrying...')
        time.sleep(10)
        try:
            client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})
        except Exception:
            print('deletion failed...')
    if t1 % 100 == 0:
        elapsed_time = int(time.time() - start_time)
        print(str(t1) + ' old records deleted... ' +
              str(elapsed_time) + ' seconds elapsed.')
        
elapsed_time = int(time.time() - start_time)
print(str(t1) + ' records deleted from Cosmos DB... ' +
      str(elapsed_time) + ' seconds elapsed.')

# Prepare dataframe for snapshot storage
df_both.drop(columns=['scraperip', 'hostname', 'search',
                      'dtg', 'scriptname'], inplace=True)
df_both['totalPrice'] = df_both['eventPrice'] + \
    df_both['hotelPrice'] + df_both['flightPrice']
output_dict = df_both.to_dict('records')

# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(
    os.environ['AZURE_STORAGE_CONNECTION_STRING'])

# Get date string
dtg = datetime.now().strftime("%Y-%m-%d")

# Create a file path in local Documents directory to upload
local_path = ""
local_file_name_json = "event-dump-" + str(dtg) + ".json"
upload_file_path_json = os.path.join(local_path, local_file_name_json)

# Write to local JSON file
file = open(upload_file_path_json, 'w')
file.write(json.dumps(output_dict))
file.close()

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_json)

print("\nUploading to Azure Storage as blob:\n\t" + local_file_name_json)

# Upload the created file
with open(upload_file_path_json, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

local_file_name_json = "event-dump.json"
print("\t" + local_file_name_json)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_json)

# Upload the created file
with open(upload_file_path_json, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

# Convert Pandas DataFrame to CSV
local_file_name_csv = "event-dump-" + str(dtg) + ".csv"
upload_file_path_csv = os.path.join(local_path, local_file_name_csv)
df_both.to_csv(upload_file_path_csv, index=False)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_csv)

print("\t" + local_file_name_csv)

# Upload the created file
with open(upload_file_path_csv, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

local_file_name_csv = "event-dump.csv"
print("\t" + local_file_name_csv)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_csv)

# Upload the created file
with open(upload_file_path_csv, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

# Convert Pandas DataFrame to xlsx
local_file_name_xlsx = "event-dump-" + str(dtg) + ".xlsx"
upload_file_path_xlsx = os.path.join(local_path, local_file_name_xlsx)
writer = pd.ExcelWriter(upload_file_path_xlsx, engine='xlsxwriter', options={'strings_to_urls': False})
df_both.to_excel(writer, sheet_name='event-dump', index=False)
workbook = writer.book
left_format = workbook.add_format({'align': 'left', 'text_wrap': True})
left_format.set_align('top')
header_format = workbook.add_format({'align': 'left', 'text_wrap': True, 'bold': True})
header_format.set_align('top')

# Format spreadsheet
worksheet = writer.sheets['event-dump']
worksheet.set_default_row(45)
worksheet.set_row(0, 15)
worksheet.autofilter('A1:AE' + str(len(df_both)))
worksheet.set_column('A:A', 30, left_format)
worksheet.set_column('B:C', 20, left_format)
worksheet.set_column('D:D', 40, left_format)
worksheet.set_column('E:E', 30, left_format)
worksheet.set_column('F:AE', 20, left_format)
worksheet.set_column('L:X', 10, left_format)
for col, val in enumerate(df_both.columns.values):
    worksheet.write(0, col, val, header_format)
writer.save()

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_xlsx)

print("\t" + local_file_name_xlsx)

# Upload the created file
with open(upload_file_path_xlsx, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

local_file_name_xlsx = "event-dump.xlsx"
print("\t" + local_file_name_xlsx)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_xlsx)

# Upload the created file
with open(upload_file_path_xlsx, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

# Finish script
elapsed_time = int(time.time() - start_time)
print(str(t1) + ' old records deleted from Cosmos DB... ' +
      str(elapsed_time) + ' seconds elapsed.')
print('Script complete...')
