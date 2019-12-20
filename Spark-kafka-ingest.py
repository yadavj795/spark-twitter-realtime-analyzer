import tweepy,time
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
from datetime import datetime, timedelta,date
from kafka import KafkaConsumer, KafkaProducer
import sys
import json
#reload(sys)
#sys.setdefaultencoding('utf8')

if len (sys.argv) != 5 :
    print ("Usage: spark-submit Spark-kafka-ingest.py consumer_key consumer_secret access_token access_token_secret")
    sys.exit (1)

conf = SparkConf().setAppName("spark-kafka-ingest")
conf = conf.setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)



today = str(date.today())

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta(hours=1)   # the tweets are timestamped in GMT timezone, while I am in +1 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

consumer_key = sys.argv[1]
consumer_secret = sys.argv[2]
access_token = sys.argv[3]
access_token_secret = sys.argv[4]

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True) #wait for rate limit replenish without exit

query = "#hadoop OR #Spark OR #IOT OR #BigData OR #Blockchain"
max_tweets = 100
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'tweets'

def get_twitter_data():
    res = api.search(query)

    for i in res:
        record = ''
        record += str(normalize_timestamp(str(i.created_at)))
        record += ','
        record += str(i.id)
        record += ','
        record += str(i.user.id_str)
        record += ','
        record += i.text.encode('utf-8').strip()
        record += ','
        record += str(i.source)
        record += ','
        record += str(i.in_reply_to_status_id)
        record += ','
        record += str(i.in_reply_to_status_id_str)
        record += ','
        record += str(i.in_reply_to_user_id)
        record += ','
        record += str(i.in_reply_to_user_id_str)
        record += ','
        record += str(i.in_reply_to_screen_name)
        record += ','
        record += str(i.geo)
        record += ','
        record += str(i.coordinates)
        record += ','
        record += str(i.place)
        record += ','
        record += str(i.user.followers_count)
        record += ','
        record += str(i.user.friends_count)
        record += ','
        record += str(i.user.favourites_count)
        record += ','
        record += str(i.user.statuses_count)
        record += ','
        record += str(i.user.location)
        record += ','
        record += str(i.favorite_count)
        record += ','
        record += str(i.retweet_count)
        record += ','
        record += str(i.user.screen_name)
        record += ','
        record += str(i.user.name)
        record += ','
        record += str(i.lang)
        print(record)
        #producer.send(topic_name, record)

def get_twitter_data_json():
    searched_tweets = [status._json for status in tweepy.Cursor(api.search,  q=query).items(max_tweets)]
    for json_obj in searched_tweets:
        rddjson = sc.parallelize([json.dumps(json_obj).encode('utf-8')])
        df = sqlContext.read.option("multiLine", "true").option("mode", "DROPMALFORMED").json(rddjson)

        producer.send(topic_name, json.dumps(json_obj).encode('utf-8'))

def periodic_work(interval):
    while True:
        get_twitter_data_json()
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)

periodic_work(60 * 0.2)
