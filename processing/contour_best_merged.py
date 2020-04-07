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
import pickle
import calendar
import Levenshtein as lev
import country_converter as coco
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ColorConverter, ListedColormap
from matplotlib.patches import Polygon
from dateutil.relativedelta import relativedelta
import plotly.express as px
import plotly
import plotly.graph_objs as go
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Get local folder and add project folder to PATH
start_time = time.time()
workingdir = os.getcwd()
sys.path.insert(0, workingdir)
parentdir = os.path.dirname(workingdir)
sys.path.insert(0, parentdir)

# Import custom modules
from utils.visualizations import piecols, stackplots, lineplots, logplots, calmap, \
    range_cal_plot, custom_sunburst, polar_day_month, lines_and_stacks
from utils.processing import calcHours, fixCategory, cleanDescription, createSearch, createURL, \
    websiteSource, combine_cat, clean_text, diff_ratio, create_classifier_examples, summarize_cities, get_all_month_total_counts
from utils.classifiers import load_model

# Set cutoff date for query
olddtg = (datetime.now() + timedelta(days=-3650)).strftime("%Y-%m-%d %H:%M:%S")
futuredtg = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")

# Query these items in SQL
query = {'query': 'SELECT * \
        FROM server s WHERE s.endDate >= "' + olddtg + '" AND s.endDate <= "' + futuredtg + '" AND s.latitude <> "" AND \
        CONTAINS(s.scriptname, "_detailed.py") \
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
        if t1 % 10000 == 0:
            elapsed_time = int(time.time() - start_time)
            print(str(t1) + ' records retrieved... ' +
                  str(elapsed_time) + ' seconds elapsed.')
    except Exception:
        pass
print(str(len(resultlist)) +
      ' results retrieved from Cosmos DB detailed')
event_search = resultlist.copy()

# Convert results dictionary to dataframe
df = pd.DataFrame(event_search)
print(str(len(df)) + ' rows in converted dataframe. Processing results into best merged answer...')

# Drop non-conference events
df['name'] = df['name'].fillna('')
blacklist = [' 101',
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

for i in blacklist:
    df = df[-df['name'].str.lower().str.contains(i)].copy()

# Filter to raw data
df_both = df[['name', 'startDate', 'endDate', 'description', 'category', 'eventurl', 'organizer',
             'venuename', 'streetAddress', 'locality', 'region', 'country', 'zipcode', 'latitude', 
             'longitude', 'dtg', 'scriptname']].copy()

# Normalize lat/longs
df_both['latitude'] = df_both['latitude'].apply(lambda x: round(float(x), 6))
df_both['longitude'] = df_both['longitude'].apply(lambda x: round(float(x), 6))

# Create search/URL fields
df_both['contourURL'] = df_both.apply(createURL, axis=1)
df_both['category'] = df_both.apply(fixCategory, axis=1)

# Finish preparing combined dataframe
df_both['country_name'] = coco.convert(
    df_both['country'].tolist(), to='name_short', not_found=None)
df_both['contourURL'] = df_both.apply(createURL, axis=1)
df_both['category'] = df_both.apply(fixCategory, axis=1)
df_both['hours'] = df_both.apply(calcHours, axis=1)
df_both['country'] = df_both['country'].apply(lambda x: str(x))
df_both = df_both[(df_both['country'].str.len() == 2)].copy()

# Deduplicate results
df_both.sort_values(by=['dtg'], ascending=False, inplace=True)
df_both.drop_duplicates(subset=['name', 'scriptname', 'latitude', 'longitude', 'startDate'], inplace=True)
df_both.drop_duplicates(subset=['contourURL'], inplace=True)
print(str(len(df_both)) + ' records in merged DataFrame')
for i1 in range(2):
    # Extract and index match columns
    df1 = df_both[['contourURL', 'name', 'startDate', 'country', 'latitude', 'longitude']].set_index(['startDate', 'country']).copy()
    df2 = df_both[['contourURL', 'name', 'startDate', 'country', 'latitude', 'longitude']].set_index(['startDate', 'country']).copy()
    df2.columns = ['contourURL2', 'name2', 'latitude2', 'longitude2']
    # Join and find distance / date / country matches
    dfj1 = df1.join(df2)
    dfj1 = dfj1[dfj1['contourURL'] != dfj1['contourURL2']]
    dfj1 = dfj1[((dfj1['latitude'] - dfj1['latitude2'])**2 +
                (dfj1['longitude'] - dfj1['longitude2'])**2)**0.5 < 0.01].copy()
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
            if tc % 1000:
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
    df_both = pd.concat([df_both_none] + merged, ignore_index=True)
    # Drop duplicate values
    print(str(len(df_both)) + ' records before fuzzy aggregation')
    df_both.drop_duplicates(subset="eventurl", inplace=True)
    print(str(len(df_both)) + ' records after fuzzy aggregation')

# Clean description
print('Applying processing to descriptions...')
df_both['description'] = df_both.apply(cleanDescription, axis=1)

# Select model for categorization
model, vectorizer = load_model()

# Predict category
df_both['old_category'] = df_both['category']
df_both['new_category'] = model.predict(vectorizer.transform(
    (df_both['name'] + ' ' + df_both['description']).apply(clean_text)))
df_both['new_category_proba'] = [max(p) for p in model.predict_proba(vectorizer.transform(
    (df_both['name'] + ' ' + df_both['description']).apply(clean_text)))]

# Drop low probability predictions
df_both.loc[df_both.new_category_proba < .25, "new_category"] = ""
df_both.loc[df_both.description.str.len() < 20, "new_category"] = ""

# Append new predictions
df_both['category'] = df_both.apply(combine_cat, axis=1)

# Add search string
df_both['search'] = df_both.apply(createSearch, axis=1)

# Drop duplicate values
df_both.drop_duplicates(subset="eventurl", inplace=True)
df_both.drop_duplicates(subset="contourURL", inplace=True)

# Create additional feature columns
df_both['year'] = df_both['startDate'].apply(lambda x: int(str(x)[0:4]))
df_both['days'] = (df_both.hours / 8).astype(int)
df_both['site'] = df_both.eventurl.apply(lambda x: websiteSource(x))
df_both['month'] = df_both.startDate.apply(lambda x: calendar.month_name[int(x.split('-')[1])])
df_both['day'] = df_both.startDate.apply(lambda x: calendar.day_name[datetime.strptime(x,"%Y-%m-%d %H:%M:%S").weekday()])
print(str(len(df_both)) + ' combined rows in dataframe.')


# Plot distribution of events by weekday and month
div = polar_day_month(df_both, div=True)

# Create results dictionary
savedplots = {}
savedplots['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
savedplots['chartname'] = 'polar_day_month'

# Create stack plot of daily total entries by scriptname in dataset over time
print('Creating chart...')
try:
    savedplots['plotly_html'] = div
except Exception:
    savedplots['plotly_html'] = ''

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Push plot data dictionary to Cosmos DB
if savedplots['plotly_html'] != '':
    print('Storing results in Cosmos DB...')
    try:
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    except Exception:
        time.sleep(20)
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    print('Data stored...')
else:
    print('Something went wrong... No data stored...')


# Plot calendar heatmaps
img = range_cal_plot(df_both, 2014, 2021)

# Save heatmap plot as png file
img.savefig("calheatmap.png", bbox_inches="tight", dpi=150)

# Create a file path in local Documents directory to upload
local_path = ""
local_file_name_m = "calheatmap.png"
upload_file_path_m = os.path.join(local_path, local_file_name_m)

# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(
    os.environ['AZURE_STORAGE_CONNECTION_STRING'])

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_m)

print("\nUploading to Azure Storage as blob:\n\t" + local_file_name_m)

# Upload the created file
with open(upload_file_path_m, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)


# Set parameters for stackplot
col = 'category'
yearlist = list(range(2010,2020))
n=15

# Remove empty category values and expand events with multiple categories
df2 = df_both[(df_both[col] != '') & (df_both[col] != 'nan')].copy()
df2[col] = df2[col].str.split('|')
df2 = df2.explode(col).copy()
df2 = df2.dropna(subset=[col])
df2 = df2[(df2[col] != '') & (df2[col] != 'nan')].copy()
df2[col] = df2[col].apply(lambda x: str(x).split(' / ')[0])

# Group by category to get top category list
dfg = df2[[col, 'name']].groupby(col).count()
dfg.columns = ['count']
dfg = dfg.sort_values(by=['count'],ascending=False)
catlist = dfg.head(n).index.to_list()


# Remove empty category values and expand events with multiple categories
df2 = df_both[(df_both[col] != '') & (df_both[col] != 'nan')].copy()
df2[col] = df2[col].str.split('|')
df2 = df2.explode(col).copy()
df2 = df2.dropna(subset=[col])
df2 = df2[(df2[col] != '') & (df2[col] != 'nan')].copy()
df2[col] = df2[col].apply(lambda x: str(x).split(' / ')[0])

# Create year column from creation date
df2['year'] = df2['startDate'].apply(lambda x: int(str(x)[0:4]))

# Filter to previously identified top categories and years
df2 = df2[df2[col].isin(catlist)].copy()
df2 = df2[df2.astype({"year": int})['year'].isin(yearlist)].copy()

# Group and count by category and year
dfg = df2[['year', col, 'name']].groupby(['year', col]).count()
dfg.columns = ['count']
dfg = dfg.sort_values(by=['year'],ascending=True).reset_index().copy()
dfg = dfg.astype({"year": int, "count": int})

# Add zero value rows for blank year/category pairs
for year in yearlist:
    for category in catlist:
        dfp2 = pd.DataFrame([[year, category, 0]], columns=['year', 'category', 'count'])
        dfg = pd.concat([dfg, dfp2], ignore_index=True)
dfg = dfg.drop_duplicates(subset=['year', 'category'], keep='first')
dfg = dfg.sort_values(by=['year'], ascending=True).reset_index(drop=True).copy()

# Create list of categories in table ordered by last year event count
categories_ordered = dfg[dfg['year']==yearlist[-1]].sort_values(by=['count'],ascending=False)[col].tolist()

# Create tuple of event count arrays for each category to pass to stack plot function
plotlist = (dfg[dfg[col]==categories_ordered[0]]['year'].values,)
for category in categories_ordered:
    plotlist = plotlist + (dfg[dfg[col] == category]['count'].values,)

# Create stack plot of annual new events by category in dataset over time
div = lines_and_stacks(plotlist, categories_ordered, title='New Conferences Over Time (Annually)', div=True)

# Create results dictionary
savedplots = {}
savedplots['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
savedplots['chartname'] = 'timeline'

# Create stack plot of daily total entries by scriptname in dataset over time
print('Creating chart...')
try:
    savedplots['plotly_html'] = div
except Exception:
    savedplots['plotly_html'] = ''
try:
    savedplots['table_json'] = json.dumps(dfg.to_dict('records'))
except Exception:
    savedplots['table_json'] = ''

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Push plot data dictionary to Cosmos DB
if savedplots['plotly_html'] != '' and savedplots['table_json'] != '':
    print('Storing results in Cosmos DB...')
    try:
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    except Exception:
        time.sleep(20)
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    print('Data stored...')
else:
    print('Something went wrong... No data stored...')


# Create classification examples dictionary
savedplots = create_classifier_examples(df_both, catlist)
savedplots['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
savedplots['chartname'] = 'classifier_examples'

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Push plot data dictionary to Cosmos DB
if savedplots['dtg'] != '':
    print('Storing results in Cosmos DB...')
    try:
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    except Exception:
        time.sleep(20)
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    print('Data stored...')
else:
    print('Something went wrong... No data stored...')

# Generate localities rollups
dfg = summarize_cities(df_both)
df3 = get_all_month_total_counts(dfg)
df4 = dfg[['locality', 'country', 'latitude', 'longitude']].drop_duplicates()
dfg2 = pd.merge(df3, df4, on=['locality'], how='left').fillna('').copy()
dfg2 = dfg2[dfg2['country'] != ''].copy()
dfg2 = dfg2.sort_values(['records'], ascending=False).copy()
dfg2.to_json('localities.json',orient='records')


# Plot starburst diagram of event locations
df_both['locality'] = df_both.locality.apply(lambda x: str(x).split('City of ')[-1])
cols = ['country_name', 'region', 'locality']
n = 50
div = custom_sunburst(df_both, cols, n, div=True)

# Create results dictionary
savedplots = {}
savedplots['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
savedplots['chartname'] = 'starburst'

# Create stack plot of daily total entries by scriptname in dataset over time
print('Creating chart...')
try:
    savedplots['plotly_html'] = div
except Exception:
    savedplots['plotly_html'] = ''

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Push plot data dictionary to Cosmos DB
if savedplots['plotly_html'] != '':
    print('Storing results in Cosmos DB...')
    try:
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    except Exception:
        time.sleep(20)
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    print('Data stored...')
else:
    print('Something went wrong... No data stored...')


# Generate pie chart visualizations for different categories
div = piecols(df_both, ['category', 'organizer', 'venuename', 'site', 'days'], 10, div=True)

# Create results dictionary
savedplots = {}
savedplots['dtg'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
savedplots['chartname'] = 'donuts'

# Create stack plot of daily total entries by scriptname in dataset over time
print('Creating chart...')
try:
    savedplots['plotly_html'] = div
except Exception:
    savedplots['plotly_html'] = ''

# Create Cosmos DB client
client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                    'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})

# Push plot data dictionary to Cosmos DB
if savedplots['plotly_html'] != '':
    print('Storing results in Cosmos DB...')
    try:
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    except Exception:
        time.sleep(20)
        item1 = client.CreateItem(
            os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), savedplots)
    print('Data stored...')
else:
    print('Something went wrong... No data stored...')


# Insert new data in Cosmos DB
df_both['scriptname'] = 'contour_best_merged.py'
insert_list = df_both.to_dict('records')
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


# Finish script
elapsed_time = int(time.time() - start_time)
print(str(t1) + ' old records deleted from Cosmos DB... ' +
      str(elapsed_time) + ' seconds elapsed.')
print('Script complete...')
