"""

Grab tweets for Twitter user `@OxfordPolice`,
for a given set of days.

"""

import pickle
import pandas as pd
import GetOldTweets3 as got3
from datetime import date, timedelta
import time


####################################################################################
## Year/month iterator
####################################################################################
def month_year_iter( start_month, start_year, end_month, end_year ):
    ym_start= 12*start_year + start_month - 1
    ym_end= 12*end_year + end_month - 1
    for ym in range( ym_start, ym_end ):
        y, m = divmod( ym, 12 )
        yield y, m+1

####################################################################################
## arguments

username = "OxfordPolice"

## First round
date_from_month = 1
date_from_year = 2014
date_to_month = 11
date_to_year = 2019

## Get a bit more
date_from_month = 10
date_from_year = 2019
date_to_month = 12
date_to_year = 2019

## Update our dataset
date_from_month = 11
date_from_year = 2019
date_to_month = 2
date_to_year = 2020


print()
print("####################################################################################")
print(f"## Collecting @{username}'s posts")
print("####################################################################################")
print()


date_from = ''
for (year, month) in month_year_iter(date_from_month, date_from_year, date_to_month, date_to_year):
    df = pd.DataFrame()

    ## Dates
    date_to = date(year, month, 1)
    if (date_from == ''):
        date_from = date_to
        continue

    ## Get tweets
    print(f"> Getting tweets between {date_from} and {date_to}.")
    criteria = got3.manager \
        .TweetCriteria() \
        .setUsername(username) \
        .setSince(date_from.strftime('%Y-%m-%d')) \
        .setUntil(date_to.strftime('%Y-%m-%d')) \
        .setTopTweets(False)
    tweets = got3.manager.TweetManager.getTweets(criteria)

    ## Save tweets
    for tweet in tweets:

        row = {}
        row['id']        = tweet.id
        row['permalink'] = tweet.permalink
        row['username']  = tweet.username
        row['text']      = tweet.text
        row['date']      = tweet.date
        row['retweets']  = tweet.retweets
        row['favorites'] = tweet.favorites
        row['mentions']  = tweet.mentions
        row['hashtags']  = tweet.hashtags
        row['geo']       = tweet.geo

        df = df.append(pd.Series(row), ignore_index=True)

    # print(df)
    # quit()

    ## Save to CSV
    filename = f"data-raw-tweets/{date_from}-{date_to}.csv"
    df.to_csv(filename)

    ## Save to pickle
    filename = f"data-raw-tweets/{date_from}-{date_to}.p"
    fn = open(filename,'wb')
    pickle.dump(df, fn)
    fn.close()

    print(f"   > Saved {len(df)} tweets")
    time.sleep(2)

    date_from = date_to

print("DONE")