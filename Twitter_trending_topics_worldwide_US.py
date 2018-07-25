import json
import requests
import pandas as pd
import twitter
import datetime as dt
import time
import os

def search_topic(q,count):
    n_tweets = 0
    q = q.replace('#','%23')
    # first page
    url = 'https://api.twitter.com/1.1/search/tweets.json?q=%s&result_type=recent&count=%s' %(q,count)
    try:
        search_results = requests.get(url,headers=search_headers_1).json()
        statuses = search_results['statuses']
    except:
        search_results = requests.get(url,headers=search_headers_2).json()
        statuses = search_results['statuses']
    # most recent used time
    recent_time = dt.datetime.strptime(statuses[0]['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)
    last_time = dt.datetime.strptime(statuses[-1]['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)
    # next page
    while last_time >= start_time:
        try:
            next_results = search_results['search_metadata']['next_results']
        except KeyError as e:
            break
        kwargs = next_results[1:]
        url = url = 'https://api.twitter.com/1.1/search/tweets.json?%s' %(kwargs)
        while True:
            try:
                try:
                    search_results = requests.get(url,headers=search_headers_1).json()
                    statuses = search_results['statuses']
                except:
                    search_results = requests.get(url,headers=search_headers_2).json()
                    statuses = search_results['statuses']
                test = 'token 1'
                last_time = dt.datetime.strptime(statuses[-1]['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)
                break
            except:
                print('Waiting for 15 min...')
                time.sleep(905)
    for i in statuses:
        check_time = dt.datetime.strptime(i['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)
        if (check_time>= start_time) & (check_time<= end_time):
            n_tweets = n_tweets+1
    df = pd.DataFrame(data=[[q,n_tweets,recent_time]],columns=['name','n_tweets','recent_time'])
    return df


def main():    
    ## Authorization
    print('Authorizing...')
    # Credential 1
    consumer_key = os.environ['CONSUMER_KEY']
    consumer_secret = os.environ['CONSUMER_SECRET']
    access_token = os.environ['ACCESS_TOKEN']
    access_token_secret = os.environ['ACCESS_SECRET']
    bearer_access_token = os.environ['BEARER_ACCESS_TOKEN']
    auth = twitter.oauth.OAuth(access_token, access_token_secret,consumer_key,consumer_secret)
    twitter_api = twitter.Twitter(auth=auth)
    # Credential 2
    bearer_access_token_2 = os.environ['BEARER_ACCESS_TOKEN_2']

    # get trends of US and worldwide
    print('Pulling trends...')
    location_list = [1,23424977]
    trends = pd.DataFrame()
    for i in location_list:
        data = twitter_api.trends.place(_id=i)[0]
        trend = data['trends']
        trend = pd.DataFrame(trend)
        trend['as_of'] = data['as_of']
        trend['location'] = data['locations'][0]['name']
        trend['woeid'] = data['locations'][0]['woeid']
        trends = trends.append(trend)
    trends = trends[['as_of','name','tweet_volume','location','woeid']]
    trends['Type'] = trends['name'].apply(lambda x: 'Hashtag' if str(x).startswith('#') else 'Topic')

    # find comment trend with world
    world_trend = list(set(trends.loc[trends['woeid']==1,'name']))
    us_trend = list(set(trends.loc[trends['woeid']==23424977,'name']))
    #location_trend = list(set(trends.loc[(trends['woeid']!=23424977)&(trends['woeid']!=1),'name']))
    trends['World_trend'] = trends['name'].isin(world_trend)
    trends.loc[trends['woeid']==1,'World_trend'] = trends.loc[trends['woeid']==1,'name'].isin(us_trend)
    trends['as_of'] = trends['as_of'].str.replace('T',' ').str.replace('Z','')

    ## 1 hour tweets
    end_time = dt.datetime.strptime(trends['as_of'].min(), "%Y-%m-%d %H:%M:%S")
    start_time = end_time - dt.timedelta(hours=1)

    # get post
    print('Pulling posts...')
    
    search_headers_1 = {'Authorization': 'Bearer {}'.format(bearer_access_token)}
    search_headers_2 = {'Authorization': 'Bearer {}'.format(bearer_access_token_2)}

    # check only for recent trends
    count = 100
    df_all = pd.DataFrame()
    for q in trends.loc[pd.isnull(trends['tweet_volume']),'name'].unique():
        while True:
            try:
                df = search_topic(q,count)
                df_all = df_all.append(df)
                break
            except:
                print('Waiting for 15 min...')
                time.sleep(905)


    df_all['name'] = df_all['name'].str.replace('%23','#')
    trends_all = trends.merge(df_all, how='left', on='name')
    trends_all['tweets'] = trends_all['tweet_volume'].fillna(trends_all['n_tweets'])
    trends_all.loc[pd.isnull(trends_all['tweet_volume']), 'Categories'] = 'Recent'
    trends_all.loc[pd.isnull(trends_all['tweet_volume'])==False, 'Categories'] = 'Popular'
    trends_all['as_of'] = dt.datetime.now()
    trends_all['rank'] = trends_all.groupby(['location','Categories'])['tweets'].rank(method='dense',ascending=False)

    ## save as json file
    print('Output to json file...')
    trends_all.to_json('twitter_trending_topics_of_worldwide_US.json',orient='records')
    
if __name__ == '__main__':
    main()