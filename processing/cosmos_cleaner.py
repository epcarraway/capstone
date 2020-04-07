# Import modules
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime
from datetime import timedelta
import os
import sys
import time
import pandas as pd

# Get local folder
start_time = time.time()
workingdir = os.getcwd()
if '/' in workingdir:
    workingdir = workingdir + '/'
else:
    workingdir = workingdir + '\\'
sys.path.append(workingdir)

# Define cutoff date to remove old event dates
olddtg = (datetime.now() + timedelta(days=-10 * 3650)
          ).strftime("%Y-%m-%d %H:%M:%S")
print(olddtg)

# load configuration json and establish connection to CosmosDB
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Define query to get 10times event searches
query = {'query': 'SELECT s.eventurl, s.dtg, s._self \
            FROM server s WHERE s.scriptname = "10times_event_search.py" \
                ORDER BY s.dtg DESC'}

options = {}
options['enableCrossPartitionQuery'] = True
options['maxItemCount'] = 100

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item]
    except Exception:
        pass
print(str(len(resultlist)) + ' results in 10times_event_search')
event_search = resultlist.copy()

# Define query to get detailed results url list
query = {'query': 'SELECT s.eventurl \
            FROM server s WHERE s.scriptname = "10times_event_detailed.py" \
                ORDER BY s.dtg DESC'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item['eventurl']]
    except Exception:
        pass
print(str(len(resultlist)) + ' results in 10times_event_detailed')
event_detailed = resultlist.copy()

# Filter to search results that already have detailed results
flist = []
for e in event_search:
    if e['eventurl'] in event_detailed:
        flist += [e]

print(str(len(flist)) + ' results in 10times_event_search to delete')

# Delete unneeded search results from Cosmos DB
tc = 0
for item in flist:
    tc += 1
    print(str(tc) + ': ' + str(item['eventurl']))
    client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

# Define query to get eventbright event searches
query = {'query': 'SELECT s.eventurl, s.dtg, s._self \
            FROM server s WHERE s.scriptname = "eventbright_event_search.py" \
                ORDER BY s.dtg DESC'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item]
    except Exception:
        pass
print(str(len(resultlist)) + ' results in eventbright_event_search')
event_search = resultlist.copy()

# Define query to get eventbright event details
query = {'query': 'SELECT s.eventurl \
            FROM server s WHERE s.scriptname = "eventbright_event_detailed.py" \
                ORDER BY s.dtg DESC'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item['eventurl']]
    except Exception:
        pass
print(str(len(resultlist)) + ' results in eventbright_event_detailed')
event_detailed = resultlist.copy()

# Filter to search results that already have detailed results
flist = []
for e in event_search:
    if e['eventurl'] in event_detailed:
        flist += [e]

print(str(len(flist)) + ' results in eventbright_event_search to delete')

# Delete unneeded search results from Cosmos DB
tc = 0
for item in flist:
    tc += 1
    print(str(tc) + ': ' + str(item['eventurl']))
    client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

# Define query to get eventil event searches
query = {'query': 'SELECT s.eventurl, s.dtg, s._self \
            FROM server s WHERE s.scriptname = "eventil_event_search.py" \
                ORDER BY s.dtg DESC'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item]
    except Exception:
        pass
print(str(len(resultlist)) + ' results in eventil_event_search')
event_search = resultlist.copy()

# Define query to get eventil event details
query = {'query': 'SELECT s.eventurl \
            FROM server s WHERE s.scriptname = "eventil_event_detailed.py" \
                ORDER BY s.dtg DESC'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item['eventurl']]
    except Exception:
        pass
print(str(len(resultlist)) + ' results in eventil_event_detailed')
event_detailed = resultlist.copy()

# Filter to search results that already have detailed results
flist = []
for e in event_search:
    if e['eventurl'] in event_detailed:
        flist += [e]

print(str(len(flist)) + ' results in eventil_event_search to delete')

# Delete unneeded search results from Cosmos DB
tc = 0
for item in flist:
    tc += 1
    print(str(tc) + ': ' + str(item['eventurl']))
    client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

# Define query to get tulula event searches
query = {'query': 'SELECT s.eventurl, s.dtg, s._self \
            FROM server s WHERE s.scriptname = "tulula_event_search.py" \
                ORDER BY s.dtg DESC'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item]
    except Exception:
        pass
print(str(len(resultlist)) + ' results in tulula_event_search')
event_search = resultlist.copy()

# Define query to get tulula event details
query = {'query': 'SELECT s.eventurl \
            FROM server s WHERE s.scriptname = "tulula_event_detailed.py" \
                ORDER BY s.dtg DESC'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item['eventurl']]
    except Exception:
        pass
print(str(len(resultlist)) + ' results in tulula_event_detailed')
event_detailed = resultlist.copy()

# Filter to search results that already have detailed results
flist = []
for e in event_search:
    if e['eventurl'] in event_detailed:
        flist += [e]

print(str(len(flist)) + ' results in tulula_event_search to delete')

# Delete unneeded search results from Cosmos DB
tc = 0
for item in flist:
    tc += 1
    print(str(tc) + ': ' + str(item['eventurl']))
    client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

# Define syntax to query for old events
query = {'query': 'SELECT s.eventurl, s.dtg, s._self \
            FROM server s WHERE (s.scriptname = "eventbright_event_search.py" OR s.scriptname = "10times_event_search.py" \
                 OR s.scriptname = "tulula_event_search.py"  OR s.scriptname = "eventil_event_search.py") \
               AND s.startDate < "' + olddtg + '" ORDER BY s.dtg DESC'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item]
    except Exception:
        pass

print(str(len(resultlist)) + ' results in event_search to delete')

# Remove old events from Cosmos DB
tc = 0
for item in resultlist:
    tc += 1
    print(str(tc) + ': ' + str(item['eventurl']))
    client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

print('Script complete')

# Define syntax to query for dev events
query = {'query': 'SELECT s.username, s.dtg, s._self \
            FROM server s WHERE (s.username = "DEVMODE")'}

# Execute query and iterate over results
result_iterable = client.QueryItems(
    os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
resultlist = []
for item in iter(result_iterable):
    try:
        resultlist += [item]
    except Exception:
        pass

print(str(len(resultlist)) + ' results in event_search to delete')

# Remove old events from Cosmos DB
tc = 0
for item in resultlist:
    tc += 1
    print(str(tc) + ': ' + str(item['username']))
    client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

print(str(tc) + ' rows deleted...')
print('Running search to remove duplicate detailed events in Cosmos')

# Query these items in SQL
query = {'query': 'SELECT s.dtg, s._self, s.eventurl, s.scriptname \
        FROM server s WHERE CONTAINS(s.scriptname, "event_detailed.py")\
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
            print(str(t1) + ' records retrieved... ' + str(elapsed_time) + ' seconds elapsed.')
    except Exception:
        pass
print(str(len(resultlist)) + ' results retrieved from Cosmos DB detailed')
event_search = resultlist.copy()

# Convert results dictionary to dataframe
df = pd.DataFrame(event_search)
print(str(len(df)) + ' rows in converted dataframe. deduping...')

df['dupe'] = df.duplicated(subset=['eventurl', 'scriptname'])
delete_list = df[df['dupe'] == 1].to_dict(orient='records')

# Delete duplicate detailed results from Cosmos DB
tc = 0
for item in delete_list:
    tc += 1
    print(str(tc) + ': ' + str(item['eventurl']))
    client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

print(str(tc) + ' rows deleted...')

print('Running search to remove duplicate search events in Cosmos')

# Query these items in SQL
query = {'query': 'SELECT s.dtg, s._self, s.eventurl, s.scriptname \
        FROM server s WHERE CONTAINS(s.scriptname, "event_search.py")\
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
            print(str(t1) + ' records retrieved... ' + str(elapsed_time) + ' seconds elapsed.')
    except Exception:
        pass
print(str(len(resultlist)) + ' results retrieved from Cosmos DB detailed')
event_search = resultlist.copy()

if len(event_search) > 1:
    # Convert results dictionary to dataframe
    df = pd.DataFrame(event_search)
    print(str(len(df)) + ' rows in converted dataframe. deduping...')
    df.sort_values(by=['dtg'], ascending=False, inplace=True)
    df['dupe'] = df.duplicated(subset=['eventurl', 'scriptname'])
    delete_list = df[df['dupe'] == 1].to_dict(orient='records')

    # Delete duplicate detailed results from Cosmos DB
    tc = 0
    for item in delete_list:
        tc += 1
        print(str(tc) + ': ' + str(item['eventurl']))
        client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

    print(str(tc) + ' rows deleted...')

print('Running search to remove duplicate merged events in Cosmos')

# Query these items in SQL
query = {'query': 'SELECT s.dtg, s._self, s.eventurl, s.scriptname \
        FROM server s WHERE CONTAINS(s.scriptname, "best_merged.py")\
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
            print(str(t1) + ' records retrieved... ' + str(elapsed_time) + ' seconds elapsed.')
    except Exception:
        pass
print(str(len(resultlist)) + ' results retrieved from Cosmos DB detailed')
event_search = resultlist.copy()

if len(event_search) > 1:
    # Convert results dictionary to dataframe
    df = pd.DataFrame(event_search)
    print(str(len(df)) + ' rows in converted dataframe. deduping...')
    df.sort_values(by=['dtg'], ascending=False, inplace=True)
    df['dupe'] = df.duplicated(subset=['eventurl', 'scriptname'])
    delete_list = df[df['dupe'] == 1].to_dict(orient='records')

    # Delete duplicate detailed results from Cosmos DB
    tc = 0
    for item in delete_list:
        tc += 1
        print(str(tc) + ': ' + str(item['eventurl']))
        client.DeleteItem(item['_self'], {'partitionKey': item['dtg']})

    print(str(tc) + ' rows deleted...')

print('Script complete')