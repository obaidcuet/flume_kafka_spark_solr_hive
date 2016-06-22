#!/bin/bash

. ~/.bash_profile

/usr/bin/spark-submit --driver-library-path /opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native/ /home/etl/SCRIPTS/tweetSentimentutil.py brokerhost1:9092,brokerhost2:9092 flume.MalaysiaTweets solrhost solrport  hdfs://nameservice1/user/etl/prod/internet/twitter/tweetSentimentMalaysia/ twitter_sentiment 100

