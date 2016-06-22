#!/bin/bash

. ~/.bash_profile

pids=( $(ps -ef|grep tweetSentimentSubmit.sh|grep -v oozie-oozi|grep -v grep|awk '{ print $2 }') )
for (( i=0; i<${#pids[@]}; i++ )); do kill -9 ${pids[i]}; done


pids=( $(ps -ef|grep tweetSentimentutil.py|grep -v oozie-oozi|grep -v grep|awk '{ print $2 }') )
for (( i=0; i<${#pids[@]}; i++ )); do kill -9 ${pids[i]}; done


