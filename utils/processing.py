# Import modules
from datetime import datetime
from bs4 import BeautifulSoup
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import re
import Levenshtein as lev
import pandas as pd

# Specify column names for input text and category, as well as number of key terms to return.
DESC_COLUMN = 'description'
CAT_COLUMN = 'category'
KEY_WORD_COUNT = 20
REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;:-]')
BAD_SYMBOLS_RE = re.compile('[^0-9a-z #+_]')
BAD_SYMBOLS_RE_NUM = re.compile('[0-9]{2,}')
STOPWORDS = set(stopwords.words('english') +
                ['2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013', '2012', '2011', '2010'])
NUMBER_OF_CATEGORIES = 50


def calcHours(row):
    '''Function to calculate event hours'''
    h1 = ((datetime.strptime(row['endDate'], '%Y-%m-%d %H:%M:%S') -
           datetime.strptime(row['startDate'], '%Y-%m-%d %H:%M:%S')).days + 1) * 8
    return h1


def fixCategory(row):
    '''Function to fix categories'''
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
        if cat != '' and cat != 'nan':
            newcats += [cat]
    newcats = '|'.join(newcats)
    return newcats


def cleanDescription(row):
    '''Function to process freeform text in description'''
    rawdescription = str(row['description'])
    for i in range(1, 4):
        rawdescription = rawdescription.replace('\n\n\n', '\n\n')
        rawdescription = rawdescription.replace('   ', '  ')
    if 'href=' in rawdescription:
        soup = BeautifulSoup(rawdescription, 'html.parser')
        rawdescription = soup.text
    return rawdescription


def createSearch(row):
    '''Function to join all fields as searchable string'''
    return ' '.join(str(x) for x in row.tolist()).lower()


def createURL(row):
    '''Function to create event url'''
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


def websiteSource(raw):
    '''Function to extract base website from URL'''
    rep = r'http://www.|https://www.|https://|http://'
    mod = re.sub(rep, '', raw, flags=re.IGNORECASE)
    mod = mod.split('/')[0]
    if 'eventbrite' in mod:
        mod = 'eventbrite.com'
    return mod


def combine_cat(row):
    '''Function to combine categories from classifier'''
    a1 = '|'.join(filter(None, list(
        set(str(row['category']).split('|') + str(row['new_category']).split('|')))))
    return a1


def clean_text(text):
    '''Function to clean text'''
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
    '''Function to calculate text similarity ratio'''
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


def summarize_table(df, col, delimiter='|'):
    '''Function to explode and summarize dataframe based on specified column and split delimiter'''
    df2 = df.copy()
    df2[col] = df2[col].astype(str) 
    df2[col] = df2[col].str.split(delimiter)
    df2 = df2.explode(col).copy()
    df2 = df2.dropna()
    dfg = df2[[col, 'startDate']].groupby([col]).agg(
        {
            col: "count",  # get the count of categories
            'startDate': [min, max]  # get the first date per group
        }
    )
    dfg.columns = ['count', 'first_seen', 'last_seen']
    dfg = dfg.reset_index().sort_values('count', ascending=False)
    dfg = dfg[dfg[col] != ''].copy()
    return dfg


def create_classifier_examples(df, catlist):
    '''Function to create high proba examples for classifer'''
    classifier_examples = {}
    df2 = df[df.new_category != ''].copy()
    df2['new_category'] = df2.new_category.str.split('|')
    df2 = df2.explode('new_category').copy()
    df2['name'] = df2['name'].fillna('')
    df2['description'] = df2['description'].fillna('')
    df2['description'] = df2['description'].replace('\n \n','\n\n').replace('\n\n\n','\n\n').replace('\n\n\n','\n\n')
    df2 = df2[df2['description'] != ''].copy()
    df2['description_alpha_ratio'] = df2.description.apply(lambda x: len(re.findall('[A-Za-z]', str(x).replace(' ',''), flags=0))/len(str(x).replace(' ','')))
    df2['new_category'] = df2.new_category.apply(lambda x: str(x).split(' / ')[0])
    for i in catlist:
        j = df2[(df2.new_category == i) & (df2.description.str.len() > 300) & (df2.description_alpha_ratio > .9)].sort_values(by=['new_category_proba'], ascending=False).description.to_list()[0]
        classifier_examples[i] = j
    return classifier_examples


def summarize_cities(df, n=100, start='2010-04', end='2020-04'):
    '''Function to summarize tables based on cities and months'''
    df2 = df[(df['locality'] != '') & (df['country'] != '')][['name', 'locality', 'country', 'latitude', 'longitude', 'startDate']].copy()
    df2['month'] = df2.startDate.apply(lambda x: x[:7])
    df2['locality'] = df2.locality.apply(lambda x: str(x).split('City of ')[-1])
    df2 = df2.drop_duplicates(subset=['name', 'locality', 'country', 'startDate'], keep='first')[['name', 'locality', 'country', 'latitude', 'longitude', 'month']].copy()
    df2['locality'] = df2['locality'] + ', ' + df2['country']
    dfg = df2[['name', 'locality']].groupby(['locality']).count()
    dfg.columns = ['records']
    dfg = dfg.sort_values(by=['records'],ascending=False)
    dfg = dfg.head(n)
    citylist = dfg.index.tolist()
    df2 = df2[df2['locality'].isin(citylist)].copy()
    df2 = df2[(df2['month'] >= start) & (df2['month'] <= end)].copy()
    dfg = df2.groupby(['locality', 'country', 'month']).agg(
            {'name': "count"}
        )
    dfg = dfg.reset_index().copy()
    dfg.columns = ['locality', 'country', 'month', 'records']
    dfg2 = df2.groupby(['locality', 'country']).agg(
            {
                'latitude': ["mean"],
                'longitude': ["mean"]
            }
        )
    dfg2 = dfg2.reset_index().copy()
    dfg2.columns = ['locality', 'country', 'latitude', 'longitude']
    dfg = pd.merge(dfg, dfg2, on=['locality', 'country'], how='left').copy()
    return dfg


def get_all_month_total_counts(df):
    '''Function to fill in empty month counts'''
    # Find aggregated counts for each month from all resource categories
    df = df[['locality','month', 'records']].copy()
    dfg = df[['locality','month', 'records']].groupby(['month']).agg({'records': ['sum']}).reset_index()
    dfg.columns = ['month','records']
    dfg['locality'] = 'All'
    dfg = dfg[['month','locality','records']]
    df = pd.concat([df, dfg], ignore_index=True)
    # Aggregate running totals for each category
    month_list = list(set(df['month'].tolist()))
    types_list = df[['locality']].drop_duplicates().to_dict('records')
    df2 = pd.DataFrame(columns=['month', 'locality', 'records', 'total'])
    for types in types_list:
        for month in month_list:
            total = df[(df['month'] <= month) & 
                      (df['locality'] == types['locality'])]['records'].sum()
            dfp2 = df[(df['month'] == month) & 
                      (df['locality'] == types['locality'])].copy()
            if len(dfp2) == 0:
                dfp2 = pd.DataFrame([[month, types['locality'], 0]], 
                                    columns=['month', 'locality', 'records'])
            dfp2['total'] = total
            # Concatenated back to working Dataframe
            df2 = pd.concat([df2, dfp2], ignore_index=True)
    df2 = df2.astype({"records": int,"total": int})
    # Find aggregated counts for all months from each categories
    dfg = df2.groupby(['locality']).agg({'records': ['sum'],
                                            'total': ['max']}).reset_index()
    dfg.columns = ['locality','records','total']
    dfg['month'] = 'All'
    dfg = dfg[['month','locality','records','total']]
    # Concatenated back to working Dataframe
    df2 = pd.concat([df2, dfg], ignore_index=True)
    df2 = df2.sort_values(by='total', ascending=False)
    return df2