# Import modules
import os
import re
import time
import geopy
import azure.cosmos.cosmos_client as cosmos_client

# Define state fixing list
replist = ['AL|Alabama',
           'AK|Alaska',
           'AZ|Arizona',
           'AR|Arkansas',
           'CA|California',
           'CO|Colorado',
           'CT|Connecticut',
           'DE|Delaware',
           'FL|Florida',
           'GA|Georgia',
           'HI|Hawaii',
           'ID|Idaho',
           'IL|Illinois',
           'IN|Indiana',
           'IA|Iowa',
           'KS|Kansas',
           'KY|Kentucky',
           'LA|Louisiana',
           'ME|Maine',
           'MD|Maryland',
           'MA|Massachusetts',
           'MI|Michigan',
           'MN|Minnesota',
           'MS|Mississippi',
           'MO|Missouri',
           'MT|Montana',
           'NE|Nebraska',
           'NV|Nevada',
           'NH|New Hampshire',
           'NJ|New Jersey',
           'NM|New Mexico',
           'NY|New York',
           'NC|North Carolina',
           'ND|North Dakota',
           'OH|Ohio',
           'OK|Oklahoma',
           'OR|Oregon',
           'PA|Pennsylvania',
           'RI|Rhode Island',
           'SC|South Carolina',
           'SD|South Dakota',
           'TN|Tennessee',
           'TX|Texas',
           'UT|Utah',
           'VT|Vermont',
           'VA|Virginia',
           'WA|Washington',
           'WV|West Virginia',
           'WI|Wisconsin',
           'WY|Wyoming',
           'DC|District of Columbia',
           'MH|Marshall Islands']


def fix_state(state):
    # Define function to fix state abbreviations
    for i in replist:
        if state == i.split('|')[0]:
            state = i.split('|')[1]
            break
    return state


def fix_geocode(item, geo1):
    # Define function to fix geocoding
    tempadd = {}
    print(item.get('locality', ''), item.get('region', ''),
          item.get('zipcode', ''), item.get('country', ''))
    if geo1.domain == 'nominatim.openstreetmap.org':
        try:
            tempadd = geo1.reverse('{}, {}'.format(
                item['latitude'], item['longitude']), language='en').raw['address']
        except Exception:
            pass
        if tempadd != {}:
            try:
                item['country'] = tempadd['country_code'].upper()
            except Exception:
                pass
            item['zipcode'] = ''
            if not re.match('.?[A-Za-z]{3}.?', item.get('region', '')):
                item['region'] = ''
            if not re.match('.?[A-Za-z]{3}.?', item.get('locality', '')):
                item['locality'] = ''
            if item.get('region', '') == '' and tempadd.get('state', '') != '':
                try:
                    item['region'] = tempadd['state']
                except Exception:
                    pass
            if item.get('zipcode', '') == '' and tempadd.get('postcode', '') != '':
                try:
                    item['zipcode'] = tempadd['postcode']
                except Exception:
                    pass
            if item.get('locality', '') == '' and tempadd.get('city', '') != '':
                try:
                    item['locality'] = tempadd['city']
                except Exception:
                    pass
            item['geocode'] = True
            print(item.get('locality', ''), item.get('region', ''),
                  item.get('zipcode', ''), item.get('country', ''))
            item['geocode_source'] = geo1.domain
            success = True
        else:
            print('no results found...')
            print(item['latitude'], item['longitude'])
            success = False
    elif geo1.domain == 'maps.googleapis.com':
        if not re.match('.?[A-Za-z]{3}.?', item.get('region', '')):
            item['region'] = ''
        if not re.match('.?[A-Za-z]{3}.?', item.get('locality', '')):
            item['locality'] = ''
        try:
            tempadd = geo1.reverse('{}, {}'.format(
                item['latitude'], item['longitude']), language='en', exactly_one=True).raw
        except Exception:
            pass
        if tempadd != {}:
            # Parse address JSON
            for i in tempadd['address_components']:
                if 'locality' in i['types']:
                    item['locality'] = i['long_name']
                elif 'country' in i['types']:
                    item['country'] = i['short_name']
                elif 'administrative_area_level_1' in i['types']:
                    item['region'] = i['long_name']
                elif 'postal_code' in i['types']:
                    if i['short_name'] != 'of Freedom#8573311~!#':
                        item['zipcode'] = i['short_name']
                elif 'street_number' in i['types']:
                    item['street_number'] = i['short_name']
                elif 'route' in i['types']:
                    item['street'] = i['short_name']
                try:
                    item['address'] = (
                        item['street_number'] + ' ' + item['street']).strip()
                except Exception:
                    pass
                try:
                    del item['street']
                except Exception:
                    pass
                try:
                    del item['street_number']
                except Exception:
                    pass
            item['geocode'] = True
            print(item.get('locality', ''), item.get('region', ''),
                  item.get('zipcode', ''), item.get('country', ''))
            item['geocode_source'] = geo1.domain
            success = True
        else:
            success = False
    return item, success


# Establish Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})
options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 1
query = {
    'query': 'SELECT * FROM server s WHERE s.country = "US" AND LENGTH(s.region) = 2'}
# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
c = 0

print('Fixing state abbreviations')
for item in iter(result_iterable):
    c += 1
    print(c, item['region'])
    item['region'] = fix_state(item['region'].upper())
    print(item['region'])
    client.UpsertItem(os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace(
        '-', '='), item, options=None)

# Define geocoder
geo1 = geopy.geocoders.Nominatim(user_agent='contour')
geo1 = geopy.geocoders.GoogleV3(os.environ['GOOGLE'])

query = {'query': 'SELECT * FROM server s WHERE (((s.country = "UK" OR s.country = "not found" OR IS_DEFINED(s.country) = false \
                   OR LENGTH(s.region) < 3 OR IS_DEFINED(s.region) = false \
                   OR LENGTH(s.zipcode) < 3 OR IS_DEFINED(s.zipcode) = false \
                   OR LENGTH(s.locality) < 3 OR IS_DEFINED(s.locality) = false) \
                   AND (IS_DEFINED(s.geocode) = false OR IS_DEFINED(s.geocode_source) = false) AND CONTAINS(s.scriptname,"_detailed.py")) OR \
                   (s.locality <> "" AND CONTAINS(s.scriptname, "_detailed.py") AND udf.regexMatchUdf(s.locality, ".?[A-Za-z]{3}.?") = false) OR \
                   (s.country = "US" AND IS_DEFINED(s.zipcode) = false AND CONTAINS(s.scriptname,"_detailed.py")) OR \
                   (s.perdiemerror = true AND CONTAINS(s.scriptname,"_detailed.py")) OR (CONTAINS(s.zipcode,"8573311") AND CONTAINS(s.scriptname,"_detailed.py")) OR \
                   (s.country = "US" AND udf.regexMatchUdf(s.zipcode, ".?[0-9]{6}.?") = true AND CONTAINS(s.scriptname,"_detailed.py"))) \
                   AND s.latitude <> "" AND s.longitude <> ""'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
c = 0

print('Fixing missing geo information')
for item in iter(result_iterable):
    c += 1
    print(c)
    time.sleep(1)
    item, success = fix_geocode(item, geo1)
    if success:
        try:
            client.UpsertItem(os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace(
                '-', '='), item, options=None)
        except Exception:
            time.sleep(30)
            try:
                client.UpsertItem(os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace(
                    '-', '='), item, options=None)
            except Exception:
                print('upsert failed...')
    else:
        print('no change... skipping upsert...')
print('Script finished.')
