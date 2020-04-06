# Import modules
from datetime import datetime
from datetime import timedelta
import os
import re
import json
import requests
import socket
import time
import pickle
import calendar
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ColorConverter, ListedColormap
from matplotlib.patches import Polygon
from dateutil.relativedelta import relativedelta
import plotly.express as px
import plotly
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from IPython.display import display, HTML

pal = ['#48c9b0', '#5499c7', '#ec7063', '#cd6155', '#82FFAB', '#FFC300', 
       '#f39c12', '#e74c3c', '#8e44ad', '#16a085', '#27ae60', '#d35400',
       '#48c9b0', '#5499c7', '#ec7063', '#cd6155', '#82FFAB', '#FFC300', 
       '#f39c12', '#e74c3c', '#8e44ad', '#16a085', '#27ae60', '#d35400']


def piecols(df, cols, n=10, div=True):
    '''Function to generate offline plotly interactive pie charts of top n values for each category'''
    # Hide legend if there are too many labels
    if n*len(cols) <= 20:
        showlegend = True
    else:
        showlegend = False
    domcol = 0
    domrow = 0
    datas = []
    # Loop through specified columns
    for col in cols:
        # Drop blanks and NAs, and do basic data cleaning
        df2 = df[(df[col] != '') & (df[col] != 'nan')].copy()
        df2[col] = df2[col].astype(str) 
        df2[col] = df2[col].str.split('|')
        df2 = df2.explode(col).copy()
        df2 = df2.dropna(subset=[col]).copy()
        rep = r'TBA|.*refund.*|.*reembols.*'
        df2[col] = df2[col].apply(lambda x: re.sub(rep, '', str(x), flags=re.IGNORECASE))
        df2[col] = df2[col].apply(lambda x: str(x).split(' / ')[0])
        df2 = df2[(df2[col] != '') & (df2[col] != 'nan')].copy()
        # Group by specified column and create sorted top n counts
        dfg = df2[[col,'name']].groupby(col).count()
        dfg.columns = ['count']
        dfg = dfg.sort_values(by=['count'], ascending=False).copy()
        dfg = dfg.head(n)
        # Create pie chart figure dictionary for each column and append to pie figure list
        data = {"hoverinfo": "label+percent+value",
                "labels": list(dfg.index),
                "marker": {"colors": pal,
                            "line": {"color": "#000000", "width": 1.0}},
                "opacity": .8,
                            "pull": 0.0,
                "domain": {"column": domcol, "row": domrow},
                "title": col.title(),
                "text": ["test"],
                "textfont": {"size": 12},
                "textinfo": "percent+label",
                "textposition": "inside",
                "insidetextorientation": "radial", 
                "values": list(dfg['count']),
                "type": "pie",
                "hole": .5}
        domcol += 1
        if domcol >= 2:
            domcol = 0
            domrow += 1
        datas = datas + [data]
    # Generate figure layout and plot
    fig = {
              "data": datas,
              "layout": {
                    "autosize": True,
                    "height": 500 * round(.1 + len(cols)/2),
                    "title": "Events by Top {} Categories".format(n),
                    "legend": {"orientation": "h"}, 
                    "showlegend": showlegend,
                    "uniformtext_minsize": 10,
                    "uniformtext_mode": "hide",
                    "grid": {"rows": round(.1 + len(cols)/2), "columns": 2}
                }
            }
    if div == True:
        output = plotly.offline.plot(fig, validate=False, include_plotlyjs=False, output_type='div')
    else:
        output = plotly.offline.iplot(fig, validate=False)
    return output


def stackplots(plotlist, catlist, title, div=True):
    '''Function to create stack plot of annual new categories in dataset over time'''
    data = []
    for y1 in range(1,len(plotlist)):
        data = data + [dict(x=plotlist[0],
                            y=plotlist[y1],
                            name=catlist[y1-1],
                            hoverinfo='x+y+name',
                            mode='lines',line=dict(width=1.5,color=pal[y1]),stackgroup='one')]
    layout = go.Layout(
        title=title,
        width=900, height=600,
        legend=dict(x=1,y=.4, bgcolor='rgba(0,0,0,0)'),
        yaxis=dict(title='Conference Events')
    )
    fig = go.Figure(data=data, layout=layout)
    if div == True:
        output = plotly.offline.plot(fig, validate=False, include_plotlyjs=False, output_type='div')
    else:
        output = plotly.offline.iplot(fig, validate=False)
    return output


def lineplots(plotlist, catlist, title, div=True):
    '''Function to create line plot of annual new categories in dataset over time'''
    data = []
    for y1 in range(1,len(plotlist)):
        data = data + [dict(x=plotlist[0],
                            y=plotlist[y1],
                            name=catlist[y1-1],
                            hoverinfo='x+y+name',
                            mode='lines',line=dict(width=1.5,color=pal[y1]))]
    layout = go.Layout(
        title=title,
        width=900, height=600,
        legend=dict(x=1,y=.4, bgcolor='rgba(0,0,0,0)'),
        yaxis=dict(title='Conference Events')
    )
    fig = go.Figure(data=data, layout=layout)
    if div == True:
        output = plotly.offline.plot(fig, validate=False, include_plotlyjs=False, output_type='div')
    else:
        output = plotly.offline.iplot(fig, validate=False)
    return output


def logplots(plotlist, catlist, title, div=True):
    '''Function to create log line plot of annual new categories in dataset over time'''
    data = []
    for y1 in range(1,len(plotlist)):
        data = data + [dict(x=plotlist[0],
                            y=plotlist[y1],
                            name=catlist[y1-1],
                            hoverinfo='x+y+name',
                            mode='lines',line=dict(width=1.5,color=pal[y1]))]
    layout = go.Layout(
        title=title,
        width=900, height=600,
        legend=dict(x=1,y=.4, bgcolor='rgba(0,0,0,0)'),
        yaxis=dict(title='Conference Events')
    )
    fig = go.Figure(data=data, layout=layout)
    fig.update_layout(xaxis_type="log", yaxis_type="log")
    if div == True:
        output = plotly.offline.plot(fig, validate=False, include_plotlyjs=False, output_type='div')
    else:
        output = plotly.offline.iplot(fig, validate=False)
    return output


def lines_and_stacks(plotlist, catlist, title, div=True):
    '''Function to create linear, log and stack line plots of annual new categories in dataset over time'''
    scale = plotlist[1].max() * 1.05
    data0 = []
    for y1 in range(1,len(plotlist)):
        data0 = data0 + [dict(x=plotlist[0],
                            y=plotlist[y1],
                            name=catlist[y1-1],
                            hoverinfo='x+y+name',
                            mode='lines',line=dict(width=1.5,color=pal[y1]),stackgroup='')]
    data1 = []
    scale1 = 0
    for y1 in range(1,len(plotlist)):
        scale1 += (plotlist[y1].max() * 1.05)
        data1 = data1 + [dict(x=plotlist[0],
                            y=plotlist[y1],
                            name=catlist[y1-1],
                            hoverinfo='x+y+name',
                            mode='lines',line=dict(width=1.5,color=pal[y1]),stackgroup='one')]
    layout0 = go.Layout(
        title=dict(text=title, x=.5, xanchor='center'),
        plot_bgcolor='rgba(255,255,255,1)',
        xaxis=dict(gridcolor='rgba(0,0,0,.1)'),
        height=600,
        legend=dict(x=1, y=.4, bgcolor='rgba(0,0,0,0)'),
        yaxis_type='linear',
        yaxis=dict(
            title='Conference Events',
            ticklen= 5,
            gridwidth= 2,
            gridcolor='rgba(0,0,0,.1)',
            range=[(-.05 * scale), scale],
            autorange=False
        )
    )
    layout1 = go.Layout(
        title=dict(text=title, x=.5, xanchor='center'),
        plot_bgcolor='rgba(255,255,255,1)',
        xaxis=dict(gridcolor='rgba(0,0,0,.1)'),
        height=600,
        legend=dict(x=1, y=.4, bgcolor='rgba(0,0,0,0)'),
        yaxis_type='log',
        yaxis=dict(
            title='Conference Events',
            ticklen= 5,
            gridwidth= 2,
            gridcolor='rgba(0,0,0,.1)',
            range=[-.1, np.log10(scale)*1.05],
            autorange=False
        )
    )
    layout2 = go.Layout(
        title=dict(text=title, x=.5, xanchor='center'),
        plot_bgcolor='rgba(255,255,255,1)',
        xaxis=dict(gridcolor='rgba(0,0,0,.1)'),
        height=600,
        legend=dict(x=1, y=.4, bgcolor='rgba(0,0,0,0)'),
        yaxis_type='linear',
        yaxis=dict(
            title='Conference Events',
            ticklen= 5,
            gridwidth= 2,
            gridcolor='rgba(0,0,0,.1)',
            range=[(-.05 * scale1), scale1],
            autorange=False
        )
    )
    fig = go.Figure(data=data0, layout=layout0)
    # Add dropdown
    fig.update_layout(
        updatemenus=[
            dict(
                type = "buttons",
                direction = "right",
                active=0,
                buttons=list([
                    dict(
                        args=[{'data' : data0, 'layout': layout0}],
                        label="linear",
                        method="animate"
                    ),
                    dict(
                        args=[{'data' : data0, 'layout': layout1}],
                        label="log",
                        method="animate"
                    ),
                    dict(
                        args=[{'data' : data1, 'layout': layout2}],
                        label="stack",
                        method="animate"
                    )
                ]),
                pad={"r": 10, "t": 10},
                showactive=True,
                x=0.25,
                y=1.115,
                yanchor="top"
            ),
        ]
    )
    if div == True:
        output = plotly.offline.plot(fig, validate=False, include_plotlyjs=False, output_type='div')
    else:
        output = plotly.offline.iplot(fig, validate=False)
    return output


def calmap(ax, year, data):
    """Function to plot calendar heatmap"""
    ax.tick_params('x', length=0, labelsize='medium', which='major')
    ax.tick_params('y', length=0, labelsize='x-small', which='major')
    # Month borders
    xticks, labels = [], []
    start = datetime(year, 1, 1).weekday()
    for month in range(1, 13):
        first = datetime(year, month, 1)
        last = first + relativedelta(months=1, days=-1)
        y0 = first.weekday()
        y1 = last.weekday()
        x0 = (int(first.strftime('%j')) + start - 1) // 7
        x1 = (int(last.strftime('%j')) + start - 1) // 7
        P = [(x0, y0), (x0, 7), (x1, 7), (x1, y1 + 1), (x1 + 1, y1 + 1), (x1 + 1, 0), (x0 + 1, 0), (x0 + 1, y0)]
        xticks.append(x0 + (x1 - x0 + 1) / 2)
        labels.append(first.strftime('%b'))
        poly = Polygon(P, edgecolor = 'black', facecolor='None', linewidth=1, zorder=20, clip_on=False)
        ax.add_artist(poly)
    ax.set_xticks(xticks)
    ax.set_xticklabels(labels)
    ax.set_yticks(0.5 + np.arange(7))
    ax.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    ax.set_title('{}'.format(year), weight='semibold')
    # Clear first and last day
    valid = datetime(year, 1, 1).weekday()
    data[valid + 1:, x1] = np.nan
    # Show data
    ax.imshow(data, extent=[0, 53, 0, 7], zorder=10, vmin=-1, cmap='RdYlBu', origin='lower', alpha=.6)


def range_cal_plot(df, start_year, end_year):
    """Function to plot multiple year heatmaps"""
    years = range(start_year, end_year)
    ax = [''] * len(years)
    ks = range(0, len(years))
    fig = plt.figure(figsize=(8, 1 + len(years) * 1.5), dpi=100)
    for year, k in zip(years, ks):
        df2 = df[['startDate','name']].copy()
        # Create year column from creation date
        df2 = df2[df2['startDate'].apply(lambda x: int(str(x)[0:4])) == year].copy()
        dfg = df2.groupby(['startDate']).count()
        dfg.columns = ['count']
        dfg = dfg.reset_index()
        dfg = dfg.sort_values(by=['count'], ascending=False).copy()
        dates = dfg['startDate'].to_list()
        blank_dates = []
        t1 = 0
        # Fill in blank dates
        for i in range(-6, 53 * 7):
            if t1 == 1:
                dtg = (datetime(year, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
                if not dtg in dates:
                    blank_dates += [{"startDate": dtg, "count": 0}]
            elif i >= 0:
                dtg = (datetime(year, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
                if not dtg in dates:
                    blank_dates += [{"startDate": dtg, "count": 0}]
            elif i < 0 and calendar.day_name[(datetime(year, 1, 1) + timedelta(days=i)).weekday()] == 'Monday':
                dtg = (datetime(year, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
                if not dtg in dates:
                    blank_dates += [{"startDate": dtg, "count": 0}]
                t1 = 1
        # Concat dataframes to combined dataframe
        dfg = pd.concat([dfg, pd.DataFrame(blank_dates)], sort=False)
        dfg.drop_duplicates(subset=['startDate'], inplace=True)
        dfg = dfg.sort_values(by=['startDate'], ascending=True).copy()
        dfg['count'] = (dfg['count'] - dfg['count'].mean())/dfg['count'].std(ddof=0)
        I = dfg['count'].to_numpy()
        ax[k] = plt.subplot(100 * len(years) + 111 + k, xlim=[0, 53], ylim=[0, 7], frameon=False, aspect=1)
        calmap(ax[k], year, 1-I[:53 * 7].reshape(53, 7).T)
    plt.tight_layout()
    # Show data
    return fig


def custom_sunburst(df, cols, n=50, div=True):
    '''Function to plot sunburst diagram based on specific columns'''
    df2 = df.copy()
    for col in cols:
        df2 = df2[df2[col] != ''].copy()
    # Group by specified column and create sorted top n counts
    dfg = df2[cols + ['name']].groupby(cols).count()
    dfg.columns = ['count']
    dfg = dfg.sort_values(by=['count'],ascending=False)
    dfg = dfg.head(n)
    dfg = dfg.reset_index()
    fig = px.sunburst(dfg, path=cols, values='count')
    fig.update_traces(
            go.Sunburst(hovertemplate='%{id}: %{value}'),
            insidetextorientation='radial',
            marker={"line": {"color": "#000000", "width": .5}}
        )
    fig.update_layout(
        height=650,
        margin = dict(t=10, l=10, r=10, b=10)
    )
    if div == True:
        output = plotly.offline.plot(fig, validate=False, include_plotlyjs=False, output_type='div')
    else:
        output = plotly.offline.iplot(fig, validate=False)
    return output


def barcols(df, cols, sortcol, n, div=True):
    '''Function to stack bar plot different count columns against name column'''
    df2 = df.copy()
    # Sort dataframe by specified column, drop duplicates and take top n rows
    df2 = df2.sort_values(by=[sortcol], ascending=False)
    df2 = df2.drop_duplicates(subset=cols, keep='first')
    df2 = df2.head(n)
    traces = []
    p1 = 0
    # Loop through count columns and create bar traces
    for col in cols:
        trace1 = go.Bar(
            x=list(df2[col]),
            y=list(df2[sortcol]),
            width=.4,
            name=col, marker=dict(
                color=pal[p1],
                line=dict(
                    color='black',
                    width=1.5)
            ),
            opacity=0.8
        )
        traces += [trace1]
        p1 += 1
    # Define layout
    layout = go.Layout(
        title='Top {} {} (Ranked by {})'.format(
            n, col.title(), sortcol.title()),
        autosize=True,
        height=550,
        margin=dict(b=130),
        yaxis=dict(title=sortcol.title()),
        barmode='stack'
    )
    # Execute and plot layout
    fig = go.Figure(data=traces, layout=layout)
    if div == True:
        output = plotly.offline.plot(fig, validate=False, include_plotlyjs=False, output_type='div')
    else:
        output = plotly.offline.iplot(fig, validate=False)
    return output


def polar_day_month(df, div=True):
    '''Function generates plotly polar area charts of event distribution by day of week and month of year'''
    fig = make_subplots(rows=1, cols=2, specs=[[{'type': 'polar'}]*2]*1)
    # Generate day of week data from data frame
    r = []
    theta = []
    for i in range(7):
        r += [len(df[df['day'] == calendar.day_name[i]])]
        theta += [calendar.day_name[i]]
    r += [r[0]]
    theta += [theta[0]]
    # Create day plot
    fig.add_trace(go.Scatterpolar(
          name = "Daily Distribution",
          r = r,
          theta = theta,
        ), 1, 1)
    # Generate month of year data from data frame
    r = []
    theta = []
    for i in range(1, 13):
        r += [len(df[df['month'] == calendar.month_name[i]])]
        theta += [calendar.month_name[i]]
    r += [r[0]]
    theta += [theta[0]]
    # Create month plot
    fig.add_trace(go.Scatterpolar(
          name = "Monthly Distribution",
          r = r,
          theta = theta,
        ), 1, 2)
    # Finish formatting for plot
    fig.update_traces(fill='toself')
    fig.update_layout(
        polar = dict(
          radialaxis_angle = -45,
          angularaxis = dict(
            direction = "clockwise",
            period = 7)
        ),
        polar2 = dict(
          radialaxis_angle = -45,
          angularaxis = dict(
            direction = "clockwise",
            period = 12)
        ),
        title=dict(text="Event Distribution by Day of Week and Month", x=.5, xanchor='center'),
        font_size = 12
        )
    if div == True:
        output = plotly.offline.plot(fig, validate=False, include_plotlyjs=False, output_type='div')
    else:
        output = plotly.offline.iplot(fig, validate=False)
    return output


# Import modules
from azure.storage.blob import BlobServiceClient
import os
import pickle
from nltk.corpus import stopwords


def load_static_cal_map():
    '''Function to loading static calendar heatmap for trends'''
    # Select model for categorization
    local_file_name = 'calheatmap.png'
    local_path = 'static'
    download_file_path = os.path.join(local_path, local_file_name)
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