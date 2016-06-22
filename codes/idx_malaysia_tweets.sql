set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1536M;
set mapred.max.map.failures.percent=10;
set mapreduce.task.timeout=1200000;

add jar /var/lib/hive/jars/solr-hive-serde.jar;

use prod_internet_db;

insert into idx_malaysia_tweets
select 
  concat(id,'-',cast(from_unixtime(unix_timestamp(concat(split(created_at,' ')[2],'-',split(created_at,' ')[1],'-',split(created_at,' ')[5],' ',split(created_at,' ')[3]), 'dd-MMM-yyyy HH:mm:ss')+8*60*60,"yyyyMMdd") as int)) id,
  created_at,
  regexp_replace(source,"</?[^>]*>","") source,
  favorited,
  nvl(retweet_count,0),
  concat_ws(',', entities.hashtags.text) hashtags,
  concat_ws(',', entities.user_mentions.screen_name) user_mentions_screen_name,
  concat_ws(',', entities.user_mentions.name) user_mentions_name,
  text,
  user.screen_name,
  user.name,
  user.location,
  cast(from_unixtime(unix_timestamp(concat(split(created_at,' ')[2],'-',split(created_at,' ')[1],'-',split(created_at,' ')[5],' ',split(created_at,' ')[3]), 'dd-MMM-yyyy HH:mm:ss')+8*60*60,"HH") as int) HOUR_ID,
  cast(from_unixtime(unix_timestamp(concat(split(created_at,' ')[2],'-',split(created_at,' ')[1],'-',split(created_at,' ')[5],' ',split(created_at,' ')[3]), 'dd-MMM-yyyy HH:mm:ss')+8*60*60,"yyyyMM") as int) MONTH_ID,
  cast(from_unixtime(unix_timestamp(concat(split(created_at,' ')[2],'-',split(created_at,' ')[1],'-',split(created_at,' ')[5],' ',split(created_at,' ')[3]), 'dd-MMM-yyyy HH:mm:ss')+8*60*60,"yyyyMMdd") as int) DATE_ID
from stg_malaysia_tweets;