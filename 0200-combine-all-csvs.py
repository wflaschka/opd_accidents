"""

Combine all tweets for Twitter user `@OxfordPolice`

"""

import time
import glob
import pickle
import pandas as pd
import GetOldTweets3 as got3
from datetime import date, timedelta
import warnings
# warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings('ignore')

####################################################################################
## arguments
username = "OxfordPolice"


print()
print("####################################################################################")
print(f"## Combining @{username}'s posts")
print("####################################################################################")
print()

files = glob.glob("data-raw-tweets/*.csv")

df = pd.DataFrame()

for file in files:

    dftmp = pd.read_csv(file)
    df = df.append( dftmp )

print(f"Number of tweets: {len(df)}")

df.to_csv("data-summary/all_tweets.csv")

print("DONE")