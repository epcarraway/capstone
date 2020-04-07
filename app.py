# Import modules
from flask import Flask, request, render_template, Response, redirect, url_for
from flask_caching import Cache
import azure.cosmos.cosmos_client as cosmos_client
from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
import json
import pickle
from datetime import datetime
from urllib.parse import unquote
import plotly
import plotly.graph_objs as go
import requests
import urllib.parse
import uuid
from utils.processing import clean_text, summarize_table
from utils.visualizations import barcols, load_static_cal_map
from utils.classifiers import load_model

# Configure caching
cache = Cache(config={'CACHE_TYPE': 'simple'})

# Initialize app and cache
app = Flask(__name__, template_folder='templates')
cache.init_app(app)

SESSION = requests.Session()


@app.route('/')
@app.route('/home')
@app.route('/index.html')
@cache.cached(timeout=600)
def index():
    return render_template('index.html')


@app.route('/trends')
@cache.cached(timeout=600)
def trends():
    return render_template('trends.html')


@app.route('/about')
@cache.cached(timeout=600)
def about():
    return render_template('about.html')


@app.route('/training')
@cache.cached(timeout=600)
def training():
    return render_template('training.html')


@app.route('/e/<event>')
@cache.cached(timeout=600, query_string=True)
def showevent(event):
    # Establish Cosmos DB client
    client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                        'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 1
    # Query these items in SQL
    query = {'query': 'SELECT TOP 1 s.name, s.startDate, s.endDate, s.description, \
            s.category, s.eventurl, s.organizer, s.venuename, s.streetAddress, s.locality, \
            s.region, s.country, s.country_name, s.latitude, s.longitude \
            FROM server s WHERE (s.scriptname = "contour_best_merged.py" or s.scriptname = "contour_manual.py") \
            AND s.contourURL=@event',
             'parameters': [
                 {'name': '@event', 'value': event.lower()}
             ]}
    # Execute query and iterate over results
    result_iterable = client.QueryItems(
        os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
    mydatadict = []
    catbutton = ''
    for item in iter(result_iterable):
        try:
            item['startDate'] = item['startDate'][:10]
            item['endDate'] = item['endDate'][:10]
            try:
                item['category'] = item['category'].replace('|', ' - ')
            except Exception:
                item['category'] = ''
            for i in item['category'].split(' - '):
                if len(i) > 1:
                    catbutton += '<a href="../search/?q={}" class="btn btn-outline-primary btn-sm" style="margin:5px;">More on {}</a>\n'.format(
                        i, i)
            mydatadict += [item]
        except Exception:
            pass
    mydata = json.dumps(mydatadict)
    return render_template('event.html', mydata=mydata, mydatadict=mydatadict[0], catbutton=catbutton)


@app.route('/search/')
@cache.cached(timeout=600, query_string=True)
def showsearch():
    dtg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    q = request.args.get("q", "")
    limit = request.args.get("limit", 100)
    try:
        limit = int(limit)
    except Exception:
        limit = 100
    if limit > 5000:
        limit = 5000
    extents = request.args.get("extents", "off")
    bounds = request.args.get("bounds", "")
    qbounds = ''
    lbounds = '[-50, -190], [70, 190]'
    if bounds != '' and extents != 'off':
        extents = 'checked'
        try:
            lat1, lon1, lat2, lon2 = bounds.split(' ')
            lat1 = str(float(lat1))
            lon1 = str(float(lon1))
            lat2 = str(float(lat2))
            lon2 = str(float(lon2))
            qbounds = ' AND (s.latitude BETWEEN {} AND {}) AND (s.longitude BETWEEN {} AND {})'.format(
                lat1, lat2, lon1, lon2)
            lbounds = '[{}, {}], [{}, {}]'.format(lat1, lon1, lat2, lon2)
        except Exception:
            bounds = ''
    else:
        extents = ''
    # Establish Cosmos DB client
    client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                        'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})
    # Query these items in SQL
    query = {'query': 'SELECT TOP @limit s.name, s.category, s.locality, s.country, s.contourURL, s.startDate, s.latitude, s.longitude, s.eventurl \
        FROM server s WHERE (s.scriptname = "contour_best_merged.py" or s.scriptname = "contour_manual.py") \
        AND CONTAINS(s.search,@q) AND s.endDate >= "' + dtg + '"' + qbounds + ' \
        AND (s.suppress <> true OR IS_DEFINED(s.suppress) = false) ORDER BY s.startDate ASC',
             'parameters': [
                 {'name': '@q', 'value': q.lower()},
                 {'name': '@limit', 'value': limit}
             ]}
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = limit
    # Execute query and iterate over results
    result_iterable = client.QueryItems(
        os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
    mydata = []
    mydata2 = {}
    for item in iter(result_iterable):
        try:
            item['display'] = '<a href="../e/' + \
                item['contourURL'] + '">' + item['name'] + '</a>'
            mydata += [item]
            try:
                mydata2[str(int(datetime.strptime(item['startDate'] + ' -0400', "%Y-%m-%d %H:%M:%S %z").timestamp()))] += 1
            except Exception:
                mydata2[str(int(datetime.strptime(item['startDate'] + ' -0400', "%Y-%m-%d %H:%M:%S %z").timestamp()))] = 1
        except Exception:
            pass
    try:
        a1 = int(round(round(mydata2[max(mydata2, key=mydata2.get)] / 5) / 5.0) * 5.0)
    except Exception:
        a1 = 2
    if a1 == 0:
        a1 = 1
    legend_split = []
    for i in range(1, 5):
        legend_split += [i * a1]
    df = pd.DataFrame(mydata)
    mydata = json.dumps(mydata)
    mydata2 = json.dumps(mydata2)
    # Summarize categories
    col = 'category'
    dfg = summarize_table(df, col)
    # Create stacked bar charts sorted by count
    catbardata = barcols(dfg, [col], 'count', 20)
    # Summarize localities
    col = 'location'
    df['location'] = df.locality + ', ' + df.country
    dfg = summarize_table(df, col)
    # Create stacked bar charts sorted by count
    locationbardata = barcols(dfg, [col], 'count', 20)
    return render_template('search.html', mydata=mydata, mydata2=mydata2, legend_split=legend_split, 
                           q=q, extents=extents, lbounds=lbounds, limit=limit, bardata=catbardata+locationbardata)


@app.route('/classifier')
@cache.cached(timeout=600, query_string=True)
def showclassifier():
    # Establish Cosmos DB client
    client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                        'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})
    # Query these items in SQL
    query = {'query': 'SELECT TOP 1 * \
        FROM server s WHERE s.chartname = "classifier_examples" \
            ORDER BY s.dtg DESC'}
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 100
    # Execute query and iterate over results
    result_iterable = client.QueryItems(
        os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
    catbutton = ''
    for item in iter(result_iterable):
        try:
            tc = 0
            for key in item:
                tc += 1
                if tc < 10:
                    catbutton += '<a href="../classifier?q={}" class="btn btn-outline-primary btn-sm" style="margin:5px;">{}</a>\n'.format(
                            item[key].replace('#','').replace('&','').strip(), key)
        except Exception:
            pass
    q = request.args.get("q", "")
    bardata = ''
    if q != '':
        try:
            # Load model
            model, vectorizer = load_model()
            # Transform and vectorize text
            transformed = vectorizer.transform([clean_text(q)])
            # Generate prediction
            prediction = model.predict(transformed)[0]
            proba_list = model.predict_proba(transformed)[0]
            proba = proba_list.max()
            if proba > .75:
                comment = 'High Confidence'
            elif proba < .25:
                comment = 'Low Confidence'
            else:
                comment = 'Medium Confidence'
            # Format results and table of probabilities
            mydata = 'Prediction: {} ({:+.2f}) / {}'.format(prediction, proba, comment)
            df = pd.DataFrame(model.classes_, columns=['category'])
            df['probability'] = proba_list
            # Create stacked bar charts sorted by probabilty
            col = 'category'
            bardata = barcols(df, [col], 'probability', 10)
        except Exception:
            mydata = 'classification failed...'
    else:
        mydata = ''
        bardata = ''
    return render_template('classifier.html', mydata=mydata, q=q, bardata=bardata, catbutton=catbutton)


@app.route('/yeartrends')
@cache.cached(timeout=600, query_string=True)
def yeartrends():
    # Establish Cosmos DB client
    client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                        'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})
    # Query these items in SQL
    query = {'query': 'SELECT TOP 1 s.plotly_html, s.table_json \
        FROM server s WHERE s.chartname = "timeline" \
            ORDER BY s.dtg DESC'}
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 100
    # Execute query and iterate over results
    result_iterable = client.QueryItems(
        os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
    mydata = []
    for item in iter(result_iterable):
        try:
            mydata += [item]
        except Exception:
            pass
    return render_template('yeartrends.html', table_json=mydata[0]['table_json'],
                            bardata=mydata[0]['plotly_html'])


@app.route('/daymonthtrends')
@cache.cached(timeout=600, query_string=True)
def daymonthtrends():
    load_static_cal_map()
    # Establish Cosmos DB client
    client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                        'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})
    # Query these items in SQL
    query = {'query': 'SELECT TOP 1 s.plotly_html \
        FROM server s WHERE s.chartname = "polar_day_month" \
            ORDER BY s.dtg DESC'}
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 100
    # Execute query and iterate over results
    result_iterable = client.QueryItems(
        os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
    mydata = []
    for item in iter(result_iterable):
        try:
            mydata += [item]
        except Exception:
            pass
    return render_template('daymonthtrends.html', bardata=mydata[0]['plotly_html'])


@app.route('/geotrends')
@cache.cached(timeout=600)
def geotrends():
    # Establish Cosmos DB client
    client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                        'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})
    # Query these items in SQL
    query = {'query': 'SELECT TOP 1 s.plotly_html, s.table_json \
        FROM server s WHERE s.chartname = "starburst" \
            ORDER BY s.dtg DESC'}
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 100
    # Execute query and iterate over results
    result_iterable = client.QueryItems(
        os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
    mydata = []
    for item in iter(result_iterable):
        try:
            mydata += [item]
        except Exception:
            pass
    return render_template('geotrends.html', bardata=mydata[0]['plotly_html'])


@app.route('/categories')
@cache.cached(timeout=600)
def categories():
    # Establish Cosmos DB client
    client = cosmos_client.CosmosClient(url_connection=os.environ['AZURE_COSMOS_ENDPOINT'].replace('-', '='), auth={
                                        'masterKey': os.environ['AZURE_COSMOS_MASTER_KEY'].replace('-', '=')})
    # Query these items in SQL
    query = {'query': 'SELECT TOP 1 s.plotly_html, s.table_json \
        FROM server s WHERE s.chartname = "donuts" \
            ORDER BY s.dtg DESC'}
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 100
    # Execute query and iterate over results
    result_iterable = client.QueryItems(
        os.environ['AZURE_COSMOS_CONTAINER_PATH'].replace('-', '='), query, options)
    mydata = []
    for item in iter(result_iterable):
        try:
            mydata += [item]
        except Exception:
            pass
    return render_template('categories.html', bardata=mydata[0]['plotly_html'])


@app.errorhandler(404)
@cache.cached(timeout=600)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == '__main__':
    app.run()
