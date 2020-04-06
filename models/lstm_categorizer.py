import pandas as pd
import matplotlib.pyplot as plt
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from keras.models import Sequential
from keras.layers import Dense, Embedding, LSTM, SpatialDropout1D
from sklearn.model_selection import train_test_split
from keras.callbacks import EarlyStopping
from nltk.corpus import stopwords
import re
import azure.cosmos.cosmos_client as cosmos_client
import os
import pickle
import time
from datetime import datetime
from azure.storage.blob import BlobServiceClient

start_time = time.time()

try:
    FNAME = os.path.basename(__file__).replace('.py', '')
except Exception:
    FNAME = 'tfidf'

FNAME = os.path.basename(__file__).replace('.py', '')


def fixCategory(row):
    # Define function to fix categories
    cats = str(row[CAT_COLUMN]).split('|')
    newcats = []
    for cat in cats:
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
        if 'faire' in cat:
            cat = ''
        if 'Evenementen' in cat:
            cat = ''
        if 'Ã' in cat:
            cat = ''
        if cat != '':
            newcats += [cat]
    newcats = '|'.join(newcats)
    return newcats


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
STOPWORDS = set(stopwords.words('english') +
                ['2022', '2021', '2020', '2019', '2018', '2017'])
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


# Query these items in SQL
query = {'query': 'SELECT s.name, s.category, s.description \
        FROM server s WHERE s.category <> "" AND s.name <> "" AND s.description <> ""'}

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

elapsed_time = int(time.time() - start_time)
print(str(elapsed_time) + ' seconds elapsed. Loading data finished...')

# Set hyperparameters
MAX_NB_WORDS = 50000  # The maximum number of words to be used. (most frequent)
MAX_SEQUENCE_LENGTH = 500  # Max number of words in each description.
EMBEDDING_DIM = 100  # Fixed
EPOCHS = 20  # Number of training epochs
BATCH_SIZE = 64  # Training batch size

# Tokenize description
tokenizer = Tokenizer(num_words=MAX_NB_WORDS,
                      filters='!"#$%&()*+,-./:;<=>?@[\]^_`{|}~', lower=True)
tokenizer.fit_on_texts(df['description'].values)
word_index = tokenizer.word_index
print('Found %s unique tokens.' % len(word_index))
X = tokenizer.texts_to_sequences(df['description'].values)
X = pad_sequences(X, maxlen=MAX_SEQUENCE_LENGTH)
print('Shape of data tensor:', X.shape)

# One hot encode categories
Y = pd.get_dummies(df[CAT_COLUMN]).values
print('Shape of label tensor:', Y.shape)
categories = pd.get_dummies(df[CAT_COLUMN]).columns.tolist()

# Train test split data
X_train, X_test, Y_train, Y_test = train_test_split(
    X, Y, test_size=0.15, random_state=42)
print(X_train.shape, Y_train.shape)
print(X_test.shape, Y_test.shape)

# Initialize model
model = Sequential()
model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1]))
model.add(SpatialDropout1D(0.25))
model.add(LSTM(300, dropout=0.25, recurrent_dropout=0.25))
model.add(Dense(200, activation='relu'))
model.add(Dense(50, activation='sigmoid'))
model.compile(loss='categorical_crossentropy',
              optimizer='adam', metrics=['accuracy'])
print(model.summary())

# Train model
history = model.fit(X_train, Y_train, epochs=EPOCHS, batch_size=BATCH_SIZE,
                    validation_split=0.15, callbacks=[EarlyStopping(monitor='val_loss', patience=5, min_delta=0.0001)])

elapsed_time = int(time.time() - start_time)
print(str(elapsed_time) + ' seconds elapsed. Training finished...')

# Evaluate model
accr = model.evaluate(X_test, Y_test)
print('Test set\n  Loss: {:0.3f}\n  Accuracy: {:0.3f}'.format(
    accr[0], accr[1]))

# Plot loss over training epochs
plt.title('Loss')
plt.plot(history.history['loss'], label='train')
plt.plot(history.history['val_loss'], label='test')
plt.legend()
plt.show()
plt.savefig(FNAME + '_loss_history.png', dpi=100)

# Plot accuracy over training epochs
plt.title('Accuracy')
plt.plot(history.history['accuracy'], label='train')
plt.plot(history.history['val_accuracy'], label='test')
plt.legend()
plt.show()
plt.savefig(FNAME + '_accuracy_history.png', dpi=100)

# Test classifier on new description
new_description = 'deliver:Agile is a technical conference that will provide the attendees with the opportunity \
    to learn how to best support and evolve their Agile engineering behaviors and habits in light of these new \
    capabilities and emerging technologies. This conference will focus on Agile that is now a multidisciplinary \
    field that includes Developers and QA, UX Designers, Infrastructure Engineers, Data Scientists, Cloud Specialists, and more.'
new_description = [clean_text(new_description)]
print(new_description)
seq = tokenizer.texts_to_sequences(new_description)
padded = pad_sequences(seq, maxlen=MAX_SEQUENCE_LENGTH)
pred = model.predict(padded)
labels = categories
for i in pred[0].argsort()[-3:][::-1].tolist():
    print(categories[i])

new_description = 'Today’s leading CIOs are more than technology experts. They are cross functional \
    change-maker. In an uncertain environment, it’s about embracing the mindset, technology and capabilities \
    to drive change. At Gartner CIO Leadership Forum 2020, you will get insights on leadership skills development, digital \
    business transformation, innovation strategies, it cost optimization and much more. '
print(clean_text(new_description))
new_description = [clean_text(new_description)]
print(new_description)
seq = tokenizer.texts_to_sequences(new_description)
padded = pad_sequences(seq, maxlen=MAX_SEQUENCE_LENGTH)
pred = model.predict(padded)
labels = categories
for i in pred[0].argsort()[-3:][::-1].tolist():
    print(categories[i])

# Get date string
dtg = datetime.now().strftime("%Y-%m-%d")

# Serialize vectorizer and model using pickle
pickle.dump(tokenizer, open("tokenizer-" + str(dtg) + ".p", "wb"))
pickle.dump(model, open("lstm-model-" + str(dtg) + ".p", "wb"))

# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(
    os.environ['AZURE_STORAGE_CONNECTION_STRING'])

# Create a file path in local Documents directory to upload
local_path = ""
local_file_name_v = "tokenizer-" + str(dtg) + ".p"
upload_file_path_v = os.path.join(local_path, local_file_name_v)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_v)

print("\nUploading to Azure Storage as blob:\n\t" + local_file_name_v)

# Create a file path in local Documents directory to upload
local_path = ""
local_file_name_m = "lstm-model-" + str(dtg) + ".p"
upload_file_path_m = os.path.join(local_path, local_file_name_m)

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name_m)

print("\nUploading to Azure Storage as blob:\n\t" + local_file_name_m)

# Finish script
elapsed_time = int(time.time() - start_time)
print(str(elapsed_time) + ' seconds elapsed. Script finished...')
