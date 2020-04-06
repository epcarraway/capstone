# Import modules
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LogisticRegression
from nltk.corpus import stopwords
import re
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import azure.cosmos.cosmos_client as cosmos_client
import os
import time
from sklearn.feature_extraction.text import TfidfVectorizer
import sklearn.metrics as metrics
import seaborn as sns
import pickle

start_time = time.time()

try:
    FNAME = os.path.basename(__file__).replace('.py', '')
except Exception:
    FNAME = 'tfidf'


def print_plot(index):
    # Define function to print example category/description
    print(df[CAT_COLUMN][index])
    print(df[DESC_COLUMN][index])


# Specify column names for input text and category, as well as number of key terms to return.
DESC_COLUMN = 'description'
CAT_COLUMN = 'category'
KEY_WORD_COUNT = 20
REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;:-]')
BAD_SYMBOLS_RE = re.compile('[^0-9a-z #+_]')
STOPWORDS = set(stopwords.words('english') + ['2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013', '2012'])
NUMBER_OF_CATEGORIES = 50


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


# Query these items in SQL
query = {'query': 'SELECT s.name, s.category, s.description \
        FROM server s WHERE CONTAINS(s.scriptname, "_event_detailed.py") AND s.category <> "" AND s.name <> "" AND s.description <> ""'}

# Load configuration json and establish connection to CosmosDB
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
start_time = time.time()
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
print(str(len(resultlist)) + ' results retrieved from Cosmos DB detailed')
event_search = resultlist.copy()

# Convert results dictionary to dataframe
df = pd.DataFrame(event_search)
print(str(len(df)) + ' rows in converted dataframe. Processing results...')

# Drop non-conference events
df['name'] = df['name'].fillna('')

# Consolidate categories
df[CAT_COLUMN] = df.apply(fixCategory, axis=1)
df = df[(df[CAT_COLUMN] != '') & (df[CAT_COLUMN] != 'nan')].copy()
df[CAT_COLUMN] = df[CAT_COLUMN].str.split('|')
df = df.explode(CAT_COLUMN)
df.loc[df[CAT_COLUMN] == 'agile tools', CAT_COLUMN] = 'Agile'
df.loc[df[CAT_COLUMN] == 'agile methodologies', CAT_COLUMN] = 'Agile'
df.loc[df[CAT_COLUMN] == 'agile', CAT_COLUMN] = 'Agile'
df.loc[df[CAT_COLUMN] == 'ux', CAT_COLUMN] = 'UI/UX'
df.loc[df[CAT_COLUMN] == 'user interaction', CAT_COLUMN] = 'UI/UX'
df.loc[df[CAT_COLUMN] == 'Security & Defense', CAT_COLUMN] = 'Security'
df.loc[df[CAT_COLUMN] == 'cloud security', CAT_COLUMN] = 'Cloud Security'
df.loc[df[CAT_COLUMN] == 'cloud native security', CAT_COLUMN] = 'Cloud Security'
df.loc[df[CAT_COLUMN] == 'crypto', CAT_COLUMN] = 'Cryptography'
df.loc[df[CAT_COLUMN] == 'developers', CAT_COLUMN] = 'Software Development'
df.loc[df[CAT_COLUMN] == 'developer', CAT_COLUMN] = 'Software Development'
df.loc[df[CAT_COLUMN] == 'development', CAT_COLUMN] = 'Software Development'
df.loc[df[CAT_COLUMN] == 'software development',
       CAT_COLUMN] = 'Software Development'
df.loc[df[CAT_COLUMN] == 'functionalprogramming',
       CAT_COLUMN] = 'Functional Programming'
df.loc[df[CAT_COLUMN] == 'IOT', CAT_COLUMN] = 'IoT'
df.loc[df[CAT_COLUMN] == 'microsoft azure', CAT_COLUMN] = 'Azure'
df.loc[df[CAT_COLUMN] == 'mobile', CAT_COLUMN] = 'Mobile'
df.loc[df[CAT_COLUMN] == 'Artificial Intelligence', CAT_COLUMN] = 'AI/ML'
df.loc[df[CAT_COLUMN] == 'Machine Learning', CAT_COLUMN] = 'AI/ML'
df.loc[df[CAT_COLUMN] == 'architectures', CAT_COLUMN] = 'Architecture'
df.loc[df[CAT_COLUMN] == 'micropython', CAT_COLUMN] = 'Python'
df.loc[df[CAT_COLUMN] == 'pycon', CAT_COLUMN] = 'Python'
df.loc[df[CAT_COLUMN] == 'pydata', CAT_COLUMN] = 'Python'
df.loc[df[CAT_COLUMN] == 'start-up', CAT_COLUMN] = 'Startups'
df.loc[df[CAT_COLUMN] == 'women', CAT_COLUMN] = 'Women in Tech'
df.loc[df[CAT_COLUMN] == 'women empowerment', CAT_COLUMN] = 'Women in Tech'
df.loc[df[CAT_COLUMN] == 'Women In Cybersecurity', CAT_COLUMN] = 'Women in Tech'
df.loc[df[CAT_COLUMN] == 'women in tech', CAT_COLUMN] = 'Women in Tech'
df.loc[df[CAT_COLUMN] == 'web design', CAT_COLUMN] = 'Web'
df.loc[df[CAT_COLUMN] == 'data science', CAT_COLUMN] = 'Data Science'
df.loc[df[CAT_COLUMN] == 'frontend', CAT_COLUMN] = 'Front End'
df.loc[df[CAT_COLUMN] == 'product', CAT_COLUMN] = 'Product'
df.loc[df[CAT_COLUMN] == 'architecture', CAT_COLUMN] = 'Architecture'
df[CAT_COLUMN].value_counts()[:500].to_csv('categories.csv', header=True)

# Plot category distribution
categories = list(df[CAT_COLUMN].value_counts()[:NUMBER_OF_CATEGORIES].index)
df = df[df[CAT_COLUMN].isin(categories)].copy()
df[CAT_COLUMN].value_counts()[:NUMBER_OF_CATEGORIES]
df[CAT_COLUMN].value_counts().sort_values(ascending=False)[:NUMBER_OF_CATEGORIES].plot(
    kind='bar', figsize=(16, 8), fontsize=13, title='Categories').plot()
plt.tight_layout()
plt.savefig(FNAME + '_histogram.png', dpi=100)

# Show sample description
df = df.reset_index(drop=True)
df2 = df.copy()
df[DESC_COLUMN] = df[CAT_COLUMN] + ' ' + df['name'] + ' ' + df[DESC_COLUMN]
print_plot(5)
df[DESC_COLUMN] = df[DESC_COLUMN].apply(clean_text)
print_plot(5)

# Explode category column
df[CAT_COLUMN] = df[CAT_COLUMN].str.split('|')
df = df.explode(CAT_COLUMN)

df2[DESC_COLUMN] = df2['name'] + ' ' + df2[DESC_COLUMN]
df2[DESC_COLUMN] = df2[DESC_COLUMN].apply(clean_text)
df2[CAT_COLUMN] = df2[CAT_COLUMN].str.split('|')
df2 = df2.explode(CAT_COLUMN)

elapsed_time = int(time.time() - start_time)
print(str(elapsed_time) + ' seconds elapsed. Loading data finished...')

# Train/fit model
print('Vectorizing words...')
vectorizer = TfidfVectorizer(
    stop_words='english',
    sublinear_tf=True,
    strip_accents='unicode',
    analyzer='word',
    token_pattern=r'\w{2,}',  # vectorize 2-character words or more
    ngram_range=(1, 3),  # word count token size
    max_df=.5,
    min_df=1,
    max_features=None)

X_train = vectorizer.fit_transform(df[DESC_COLUMN])
X_test = vectorizer.transform(df2[DESC_COLUMN])
y_train = df[CAT_COLUMN]
feature_names = vectorizer.get_feature_names()
feature_names = np.asarray(feature_names)

# Fit model
print('Fitting model...')
clf = LogisticRegression(solver='sag', C=10, multi_class='ovr')
clf.fit(X_train, y_train)

elapsed_time = int(time.time() - start_time)
print(str(elapsed_time) + ' seconds elapsed. Training finished...')

print('Model fitted. Running predict...')
pred = clf.predict(X_test)

# Print category key works
print('Running model prediction and displaying top token words/phrases per category...')
for x1 in range(0, len(clf.classes_)):
    print(clf.classes_[x1])
    topN = np.argsort(clf.coef_[x1])[-KEY_WORD_COUNT:]
    list1 = feature_names[topN].tolist()
    list1.reverse()
    print(list1)
    print('')

# Generate scores and confusion matrix
score = metrics.accuracy_score(y_train, pred)
print('accuracy against training: %0.3f' % score)
cnf_matrix = metrics.confusion_matrix(y_train, pred)
plt.figure(figsize=(20, 16))
sns.heatmap(cnf_matrix, annot=True, fmt='d', cmap='Blues',
            linewidths=.5, linecolor='black', annot_kws={'size': 10})
plt.xticks(plt.xticks()[0], clf.classes_, rotation='vertical')
plt.yticks(plt.xticks()[0], clf.classes_, rotation='horizontal')
plt.title('Predicted Versus Actual ' + CAT_COLUMN.title() +
          ' (Against Training Data Only)')
plt.tight_layout()
plt.savefig(FNAME + '_confusion.png', dpi=100)

# Test classifier on new description
new_description = 'deliver:Agile is a technical conference that will provide the attendees with the opportunity \
    to learn how to best support and evolve their Agile engineering behaviors and habits in light of these new \
    capabilities and emerging technologies. This conference will focus on Agile that is now a multidisciplinary \
    field that includes Developers and QA, UX Designers, Infrastructure Engineers, Data Scientists, Cloud Specialists, and more.'
print(clean_text(new_description))
new_description = vectorizer.transform([clean_text(new_description)])
pred = clf.predict(new_description)
print(pred[0])
new_description = 'Today’s leading CIOs are more than technology experts. They are cross functional \
    change-maker. In an uncertain environment, it’s about embracing the mindset, technology and capabilities \
    to drive change. At Gartner CIO Leadership Forum 2020, you will get insights on leadership skills development, digital \
    business transformation, innovation strategies, it cost optimization and much more. '
print(clean_text(new_description))
new_description = vectorizer.transform([clean_text(new_description)])
pred = clf.predict(new_description)
print(pred[0])

# Get date string
dtg = datetime.now().strftime("%Y-%m-%d")

# Serialize vectorizer and model using pickle
pickle.dump(vectorizer, open("vectorizer-" + str(dtg) + ".p", "wb"))
pickle.dump(clf, open("tfidf-model-" + str(dtg) + ".p", "wb"))

# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(
    os.environ['AZURE_STORAGE_CONNECTION_STRING'])

# Create a file path in local Documents directory to upload
local_path = ""
local_file_name_v = "vectorizer-" + str(dtg) + ".p"
upload_file_path_v = os.path.join(local_path, local_file_name_v)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_v)

print("\nUploading to Azure Storage as blob:\n\t" + local_file_name_v)

# Upload the created file
with open(upload_file_path_v, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

# Create a file path in local Documents directory to upload
local_path = ""
local_file_name_m = "tfidf-model-" + str(dtg) + ".p"
upload_file_path_m = os.path.join(local_path, local_file_name_m)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_m)

print("\nUploading to Azure Storage as blob:\n\t" + local_file_name_m)

# Upload the created file
with open(upload_file_path_m, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

# Finish script
elapsed_time = int(time.time() - start_time)
print(str(elapsed_time) + ' seconds elapsed. Script finished...')
