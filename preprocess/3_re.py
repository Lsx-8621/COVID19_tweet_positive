# total number of tweets: 345275
# total number of users: 
# filis in /data_8t/LSX/preprocess/filter/positive/



import re
import glob
import tqdm 
import pandas as pd


pattern_1 = re.compile(r"(\b(if|or|nor|may)\b|tell|told|manage|say|said|want|wanna|try|tried|guess|have not|havent|haven't|didn|don|would|will|can|could|rather|might|dream|hate|likely|seem|scare|pretend|chance|prolly|probably|look to|possibly|think|thought|imagine|feel|felt|plan|wonder|doubt|wish|hope|maybe|unless|almost|wether|whether|in case|chance|paranoid|assume|worried|worry|pray|afraid|suspect|either)(?=.{0,25}(covid|corona|virus|ncov|koronavirus|sars-cov-2|covd|rona|positive))",flags=re.I)
pattern_2 = re.compile(r'(\b(do|am|were)\b\si)(?=.{0,20}(covid|corona|virus|ncov|koronavirus|sars-cov-2|covd|rona|positive))',flags=re.I)
pattern_3 = re.compile(r"(\bi\b)(?=.{0,30}(covid|corona|virus|ncov|koronavirus|sars-cov-2|covd|rona|positive))",flags=re.I)

def pattern_match(text):
    if not re.search(pattern_1,text) == None:
        return False
    elif not re.search(pattern_2,text) == None:
        return False
    elif not re.search(pattern_3,text) == None:
        return True

'-----------------------------------------------------------------------'

data_dir = "/data_8t/LSX/preprocess/filter/positive/" 
files = sorted(glob.glob(data_dir+"*.csv"))

all_data_frame = []
for file in tqdm.tqdm(files):
    data_frame = pd.read_csv(file, low_memory = False,lineterminator = '\n')
    all_data_frame.append(data_frame)
data_frame_concat = pd.concat(all_data_frame, axis=0, ignore_index=True)
data_frame_concat.to_csv("self-reported.csv", index=False)   

data = pd.read_csv('self-reported.csv',low_memory =False, lineterminator = '\n')
data['label'] = data['full_text'].apply(pattern_match)
data = data[data['label']==True]

# number of positive tweets: 341901
data.to_csv('/data_8t/LSX/preprocess/filter_strategy/positive_tweets.csv',index=False)



# number of pisitive users: 237423
data = pd.read_csv('positive_tweets.csv',low_memory =False, lineterminator = '\n')
data['created_at'] = pd.to_datetime(data['created_at'])
data = data.sort_values(by='created_at').reset_index(drop=True)
# keep first self-reported tweet of each user
data.drop_duplicates(subset=['user_id'],keep='first',inplace=True)
data = data.reset_index(drop=True)
data.to_csv("/data_8t/LSX/preprocess/filter_strategy/self-reported_users.csv",index=False)