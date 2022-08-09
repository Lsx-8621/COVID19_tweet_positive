# remove organization accounts
# for 237,423 self-reported users, 6454 users were is-org

import json
import pandas as pd 

df = pd.read_csv('/data_8t/LSX/preprocess/filter_strategy/self-reported_users.csv',lineterminator='\n',usecols=['created_at', 'id_str', 'full_text', 'user', 'coordinates', 'place',
       'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'geo'])

# write into information of each user
with open('frame.json','w',encoding='UTF-8') as f:
    for i in range(len(df)):
        dic = {}
        dic['created_at'] = df['created_at'][i]
        dic['id_str'] = str(df['id_str'][i])
        dic['full_text'] = df['full_text'][i]
        dic['user'] = eval(df['user'][i])
        dic['coordinates'] = df['coordinates'][i]
        dic['place'] = df['place'][i]
        dic['quote_count'] = str(df['quote_count'][i])
        dic['reply_count'] = str(df['reply_count'][i])
        dic['retweet_count'] = str(df['retweet_count'][i])
        dic['favorite_count'] = str(df['favorite_count'][i])
        dic['geo'] = df['geo'][i]
        line = json.dumps(dic,ensure_ascii=False)
        f.write(line+'\n')

# Download the user's image from the Tweet JSON Object
from m3inference import M3Twitter

m3twitter=M3Twitter(cache_dir="/home/lsx/m3/cache") #Change the cache_dir parameter to control where profile images are downloaded
m3twitter.transform_jsonl(input_file="/data_8t/LSX/preprocess/filter_strategy/frame.json",output_file="/data_8t/LSX/preprocess/filter_strategy/m3_input.jsonl")


from m3inference import M3Inference

# If image information is missing, set use_full_model=False
m3 = M3Inference() 
#the prediction for each user's gender, age and organization
pred = m3.infer('m3_input.jsonl') 


age_predict_list = []
gender_predict_list = []
org_predict_list = []

for i in range(len(df)):
    pred_i = pred[str(eval(df['user'][i])['id'])]

    age = pred_i['age']
    age_predict = sorted(age.items(), key=lambda age:age[1],reverse = True)[0][0]
    age_predict_list.append(age_predict)

    gender = pred_i['gender']
    gender_predict = sorted(gender.items(), key=lambda gender:gender[1],reverse = True)[0][0]
    gender_predict_list.append(gender_predict)

    org = pred_i['org']
    org_predict = sorted(org.items(), key=lambda org:org[1],reverse = True)[0][0]
    org_predict_list.append(org_predict)

geo_information_data = pd.DataFrame({'age':age_predict_list,'gender':gender_predict_list,'org':org_predict_list})
df = df.join(geo_information_data)
df.to_csv('/data_8t/LSX/preprocess/filter_strategy/self-reported_users.csv',index=False)