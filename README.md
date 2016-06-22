# flume_kafka_spark_solr_hive
A template from my team to work with flume, kafka, spark(MLlib+streaming), hive &amp; solr

-- ----------
-- 1. Target:
-- ----------

- Collect tweeter public streams related to malaysia
- Store it with below ideas in mind:
	a. fast searchable
	b. can use Sql to query
- Perform real-time scoring (sentiment considering both malay & english)
Note: We are considering Malay language here as it uses English characters, makes it easy to use. 

-- -------------------------------
-- 2. Data flow for this solution:
-- -------------------------------

We can divide entire solution as below: 

```  
  a.twitter	b.flume(exec source tweepy)   > g.Kafka > c.HDFS            > d.Hive        > e.Solr
                                                                                        > f.Parquet
                                                    > h.Spark Streaming > i.SparkMLlib  > j.Solr
```

-- ----------------------
-- 3. Create Kafka topic:
-- ----------------------

create topic below properties:
	- with 10G
	- 24 hours retention
	- 20 partitions, 2 replication
	- snappy compression 
	- name "flume.MalaysiaTweets"

ssh sthdetl1-pvt
su -etl
/usr/bin/kafka-topics --zookeeper zkhost1:2181,zkhost2:2181,zkhost3:2181/kafka --create --topic flume.MalaysiaTweets --partitions 20 --replication-factor 2 --config retention.bytes=10737418240 --config retention.ms=86400000 --config compression.type='snappy'

4. Collecting Twitter Streams
- We will use tweepy as an API to collect tweeter streams
- We need to update tweepy code to avoid error "'NoneType' object has no attribute 'strip'"  as per  https://github.com/tweepy/tweepy/commit/f4bfce5d612304ed4697d2cc300ff06935a4585b
- Python code has been written keeping below points in mind:
	- Will use tweepy 
	- capable of logging
	- print tweets directly to stdout (we will use exec source in flume) and stderr will go to logger
 
Code is attached ./code/tweetStdout.py

-- ----------------
-- 5. flume config:
-- ----------------

Below is the content of flume.conf:
	- it will be using "exec" source and executing above python code
	- it will auto restart the exec script, if the script dies for any reason
	- kafka channel and sink to HDFS 
	- it will also use "search_replace" interceptors to replace literal "\n" with spaces in tweets to avoid line break in hive table
    - Create related HDFS sink directory "/user/etl/prod/internet/twitter/malaysia/flume/" before start this agent
```
## Configuration Start:TwitterMalaysia
AgentMalaysiaTweets.sources = TwitterExec
AgentMalaysiaTweets.channels = kafka-channel
AgentMalaysiaTweets.sinks = HDFS
AgentMalaysiaTweets.sources.TwitterExec.interceptors = cleanSpecialchars

AgentMalaysiaTweets.sources.TwitterExec.interceptors.cleanSpecialchars.type = search_replace
AgentMalaysiaTweets.sources.TwitterExec.interceptors.cleanSpecialchars.searchPattern =(\\\\r|\\\\n)
AgentMalaysiaTweets.sources.TwitterExec.interceptors.cleanSpecialchars.replaceString = [ ]

AgentMalaysiaTweets.sources.TwitterExec.type = exec
AgentMalaysiaTweets.sources.TwitterExec.restart = true
AgentMalaysiaTweets.sources.TwitterExec.channels = kafka-channel
AgentMalaysiaTweets.sources.TwitterExec.command = /stage/AIU/ETL/internet/twitter/scripts/tweetMalaysiaStdout.py /stage/AIU/ETL/internet/twitter/logs/tweetMalaysiaStdout.log

AgentMalaysiaTweets.sinks.HDFS.channel = kafka-channel
AgentMalaysiaTweets.sinks.HDFS.type = hdfs
AgentMalaysiaTweets.sinks.HDFS.hdfs.path = hdfs://nameservice1:8020/user/etl/prod/internet/twitter/malaysia/flume/
AgentMalaysiaTweets.sinks.HDFS.hdfs.fileType = DataStream
AgentMalaysiaTweets.sinks.HDFS.hdfs.writeFormat = Text
AgentMalaysiaTweets.sinks.HDFS.hdfs.batchSize = 1000
AgentMalaysiaTweets.sinks.HDFS.hdfs.rollSize = 0
AgentMalaysiaTweets.sinks.HDFS.hdfs.rollCount = 10000
AgentMalaysiaTweets.sinks.HDFS.hdfs.rollInterval = 300

AgentMalaysiaTweets.channels.kafka-channel.type = org.apache.flume.channel.kafka.KafkaChannel
AgentMalaysiaTweets.channels.kafka-channel.brokerList = brokerhost1:9092,brokerhost2:9092
AgentMalaysiaTweets.channels.kafka-channel.topic = flume.MalaysiaTweets
AgentMalaysiaTweets.channels.kafka-channel.zookeeperConnect = zkhost1:2181,zkhost2:2181,zkhost3:2181/kafka
## Configuration End:TwitterMalaysia 
```
-- ----------------------
-- 6. Hive configuration:
-- ----------------------
Points to cover:
	a. collect/build json sarde jar for hive
	b. create and external table pointing to the HDFS location where flume stores tweets
	c. create parquet table which will hold tweets for long term historical usage
	d. collect/build solr-hive-serde.jar
	e. create another external table with pointing to solr index[7.a]

Details of above is attached ./code/hive_configuration.txt

-- ----------------------
-- 7. Solr configuration:
-- ----------------------
  
  a. create generic collection for tweeter with 12 shards and replication level 2
  
  b. create collection for tweeter sentiment with 12 shards and replication level 2
Details of above is attached ./code/solr_configuration.txt

-- ----------------------
-- 8. Sentiment Analysis:
-- ----------------------

--preparation

a. Download Twitter Sentiment Analysis Dataset from http://thinknook.com/wp-content/uploads/2012/09/Sentiment-Analysis-Dataset.zip

b. sample datasets above file for modeling purpose

c. translate a portion of it to malay

d. download affin-111 dataset from http://www2.imm.dtu.dk/pubdb/views/edoc_download.php/6010/zip/imm6010.zip

e. default version save as AFINN-111_eng.txt and malay translated version of the same dataset save as AFINN-111_malay.txt

f. Extract features from tweets and store final dataset for sentiment analysis in file TrainingTweetsScore.csv

Details of above steps [a-f] are in [./code/sentiment_preparation.py]  

-- modelling
g. Start modelling with R, finally we used random-forest and got ~70% accuracy
Details [./code/modeling.r]

h. Replicate same method as r in pyspark. Found same result, ~70% accuracy. Save the model for later usage.
Details [./code/modeling.pyspark]

i. Test prediction via pyspark
Details [./code/prediction.pyspark]

-- -------------------
-- 9. Spark Streaming:
-- -------------------

Streaming application needs to consider below points:
a. Read data from kafka topic. For better performance (avoiding Write Ahead Log), we will be using direct method[connect brokers instead of via zookeepers]
b. will perform feature extraction on the fly
c. preform sentiment scoring on the fly using MLlib model built in above step
d. Store the tweet along with sentiment score in a solr collection
e. this application requires Malay and English Affin words [8.e] stored on HDFS 
f. The MLlib model[8.h] also should be stored in HDFS 

Full spark-streaming code is attached [./code/tweetSentimentutil.py]

-- -------------
-- 9.Deployment:
-- -------------

a. Start Kafka cluster it not started yet.

b. Start flume agent configured in step 5. It will start storing tweets in below locations:
	- HDFS: /user/etl/prod/internet/twitter/malaysia/flume/
	- Kafka topic: flume.MalaysiaTweets

c. Insert tweeter generic data into parquet table & solr index:
   Create an oozie workflow with below sequential components:
      i. archive data from HDFS location pointed by hive json external table to HDFS archive area
	  ii. Move file from flume HDFS sink area to staging area(hive json ext table location [6.b])
	  iii. Load data from json ext table[6.b] to parquet table[6.c] using hive [./code/f_malaysia_tweets.sql]
	  iv. Update impala metadata
	  v. Load data from json ext table to solr external [6.e] table using hive [./code/idx_malaysia_tweets.sql]. This will eventually load data in solr collection.
	  vi. Schedule this workflow under coordinator running every 10 minutes
	  
Workflow configuration is as [./code/Workflow wf__INTERNET__TWITTER_MALAYSIA.xml]. Corresponding scripts are also under [./code/]
	  
c. Insert tweeter sentiment scored data into solr index:
   Create an oozie ssh-action workflow with below sequential components:
	  i. Kill any existing same application running [./code/KillTweetSentimentSubmit.sh]
	  ii. run spark streaming application to score tweet sentiment[./code/tweetSentimentSubmit.sh] 
	      *** Avoid domain part of the hostname, in case you have internet proxy(squid). If request goes through squid, this will limit the URL/header size.
	  iii. Just submit this workflow, it will run continuously, no need coordinator.
	  
Workflow configuration is as [./code/Workflow wf__tweet__sentiments.xml]. Corresponding scripts are also under [./code/]
	  
d. Access tweet_malaysia & tweeter_sentiment solr indexes using hue search option
	  
