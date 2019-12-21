"""

Filter non-accident @OxfordPolice tweets

"""

import time
import glob
import pickle
import pandas as pd
import GetOldTweets3 as got3
from datetime import date, timedelta
import warnings
warnings.filterwarnings('ignore')


####################################################################################

## Get our full list of tweets
df = pd.read_csv("data-summary/all_tweets.csv")

## Drop extra cols that cropped up
df.drop(['Unnamed: 0', 'Unnamed: 0.1'], axis=1, inplace=True)

## Keep tweets that have 'accident' in them
filter1 = df['text'].str.contains('accident', na=False)
filter2 = df['text'].str.contains('Accident', na=False)
df = df.loc[(filter1 | filter2)]

## Filter for accident-reporting tweets, and remove Incomplete accident-reporting tweets
## (Gets rid of tweets that just have the word 'accident')
## Looking for:
##     * `Reported Auto Accident at/near`, or 
##     * `Reported Accident at/near`, and which
##     * Don't end in ','

filter1  = df['text'].str.contains('Reported Auto Accident at/near')
filter2  = df['text'].str.contains('Reported Accident at/near')
filter3n = df['text'].str.endswith('at/near ,')
df = df[(filter1 | filter2) & ~filter3n].copy()

## Set index as timeseries
df['ts'] = pd.to_datetime(df['date'])
df['ts'] = df['ts'].dt.tz_convert('US/Central')
df.set_index('ts', inplace=True)

df.to_csv("data-summary/all_accidents.csv")
