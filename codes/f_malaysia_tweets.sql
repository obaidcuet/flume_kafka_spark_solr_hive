ADD JAR /var/lib/hive/jars/hive-json-serdes.jar;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles=true;
set dfs.block.size=1073741824;
set hive.merge.size.per.task=536870912;
set hive.merge.smallfiles.avgsize=268435456;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts=-Xmx4096M;
set mapred.max.map.failures.percent=10;

use prod_internet_db;

insert into table f_malaysia_tweets PARTITION(MONTH_ID, DATE_ID)
select
id,
created_at,
from_unixtime(unix_timestamp(concat(split(created_at,' ')[2],'-',split(created_at,' ')[1],'-',split(created_at,' ')[5],' ',split(created_at,' ')[3]), 'dd-MMM-yyyy HH:mm:ss')+8*60*60) tweet_timestamp,
source,
favorited,
retweet_count,
entities.hashtags,
entities.user_mentions,
entities.urls,
text,
user.screen_name,
user.name,
user.description,
user.friends_count,
user.followers_count,
user.statuses_count,
user.verified,
user.utc_offset,
user.time_zone,
user.location,
place.full_name,
place.name ,
place.country,
place.country_code,
place.place_type,
place.bounding_box.type,
place.bounding_box.coordinates,
in_reply_to_screen_name,
cast(from_unixtime(unix_timestamp(concat(split(created_at,' ')[2],'-',split(created_at,' ')[1],'-',split(created_at,' ')[5],' ',split(created_at,' ')[3]), 'dd-MMM-yyyy HH:mm:ss')+8*60*60,"HH") as int) HOUR_ID,
cast(from_unixtime(unix_timestamp(concat(split(created_at,' ')[2],'-',split(created_at,' ')[1],'-',split(created_at,' ')[5],' ',split(created_at,' ')[3]), 'dd-MMM-yyyy HH:mm:ss')+8*60*60,"yyyyMM") as int) MONTH_ID,
cast(from_unixtime(unix_timestamp(concat(split(created_at,' ')[2],'-',split(created_at,' ')[1],'-',split(created_at,' ')[5],' ',split(created_at,' ')[3]), 'dd-MMM-yyyy HH:mm:ss')+8*60*60,"yyyyMMdd") as int) DATE_ID
from
stg_malaysia_tweets;