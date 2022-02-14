# Start writing# Import packages
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
import pandas as pd 
import pandas_gbq
import os 
from datetime import datetime, timedelta
import time
info = 'facebook'
ext = '.csv'

# Credentials
app_id = os.environ["APP_ID"]
my_app_secret = os.environ["MY_APP_SECRET"]
my_access_token = os.environ["MY_ACCESS_TOKEN"]
ad =  AdAccount(os.environ["ADACCOUNT"])
FacebookAdsApi.init(app_id, my_app_secret, my_access_token)  
me = User(fbid='me')
my_accounts = list(me.get_ad_accounts())

# Get Date
yesterday = datetime.now() - timedelta(1)
yesterday = datetime.strftime(yesterday, '%Y-%m-%d')

# Open sftp
import paramiko
host= os.environ["HOST"]
port= os.environ["PORT"]
username= os.environ["USERNAME"]
password= os.environ["PASSWORD"]
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.load_system_host_keys()
client.connect(host,port=port, username=username, password=password, allow_agent=False, look_for_keys=False)
sftp_client = client.open_sftp()


def fb_importation (date_info): 
  params = {
    'time_range': {'since': date_info , 'until': date_info },        
    'level': 'ad',
    'time_increment': 1,
    'breakdowns': ['impression_device'],
    'action_type': 'offsite_conversion'}
  fields = ['date_start','date_stop','account_id','campaign_id','adset_id','ad_id','spend', 'impressions','inline_link_clicks'] 
  insights = ad.get_insights(params=params , fields = fields)
  results = []
  for item in insights:
    data = dict(item)
    results.append(data)
# Transform into DataFrame
  df = pd.DataFrame(results)
  nom_fichier = info + date_info + ext
  df = df.rename(columns={"date_start": "Reporting Starts", 
                   "date_stop": "Reporting Ends", 
                   "account_id": "Account ID",
                   "campaign_id": "Campaign ID",
                   "adset_id": "Ad Set ID",
                   "ad_id": "Ad ID",
                   "spend": "Amount Spent (USD)",
                   "impressions": "Impressions",
                   "inline_link_clicks": "Link Clicks",
                   "impression_device": "Impression Device"  })
  df.to_csv(nom_fichier,index=False)
  sftp_client.put('/work/'+nom_fichier, nom_fichier)
  return ('Importation is OK , the filename is ' + nom_fichier )

fb_importation(yesterday)
