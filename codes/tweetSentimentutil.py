### twitter sentiment related functions
### sample run : spark-submit --driver-library-path /opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native/ tweetSentimentutil.py brokerhost1:9092,brokerhost2:9092 flume.MalaysiaTweets solrhost solrport hdfs://<hdfslocation>/ twitter_sentiment 100


## imports
import re, string, sys, json, subprocess
from datetime import datetime, timedelta

from pyspark.mllib.tree import RandomForest, RandomForestModel

from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


## replace all except alphabets and apostrophe
def numPuntuationCleaner(inputStr):
    # below steps should be done in sequence as here
    # 1.replace all back tick with single quote
    inputStr = re.sub(r"`", "'", inputStr)
    # 2.replace all except single quote & words 
    inputStr = re.sub(r"[^\w' ]", "", inputStr)
    # 3.replace all under score "_" with space
    inputStr = re.sub(r"[_]", " ", inputStr)
    # 4.replace all single quotes except apostrophe s,t,d & ve.
    # This should be done after replace all except single quote & words 
    inputStr = re.sub(r"(?!'[stdm]|'ve|'re)'", " ", inputStr)
    # 5. Replace all standalone numbers and numbers that are start of word
    inputStr = re.sub("^\d+| \d+|\t+\d+", " ", inputStr)
    # 6. Replace extra spaces and tabs with single spaces
    inputStr=re.sub(r" +|\t+", " ", inputStr)
    # 7. Repace starting and trailing spaces and tabs
    out=re.sub(r"^ +| +$|^\t+|\t+$", "", inputStr)
    ## return
    return out


## returns a dictionary of ngrams	
def ngrams(input, n):
    input = input.split(' ')
    output = {}
    for i in range(len(input)-n+1):
        g = ' '.join(input[i:i+n])
        output.setdefault(g, 0)
        output[g]+=1
    ## return
    return output


## sentiment score summary
def sentimetSummaryScore(words, affin):
    score=0
    totalMatch=0
    for word in words:
        #currentVal=fuzzyDictMatch(word, affin, 95)
        currentVal=float(affin.get(word, 0))
        if currentVal != 0:
            totalMatch=totalMatch+1
            score=score+currentVal
    ## return
    return float(score)/totalMatch


## sentiment score: positive, negative
def sentimetScores(words, affin):
    positiveScore=0
    negativeScore=0
    for word in words:
        currentVal=float(affin.get(word, 0))
        if currentVal > 0 :
            positiveScore=positiveScore+currentVal
        elif currentVal < 0 :    
            negativeScore=negativeScore+currentVal
    ## return        
    return float(positiveScore), float(negativeScore)


## load dictionary
def loadDict(affinList, seperator):
    dict = {} 
    for item in affinList: 
        d = str(item.encode('utf-8', "ignore")).strip().split(seperator) 
        dict[d[0]] = d[1]
    ## return
    return dict

## Collect Character level Counters
def collectCharCounters(sentence):
    length=float(len(sentence))
    letters=0.0
    numbers=0.0
    spaces=0.0
    questions=0.0
    exclamation=0.0
    others=0.0
    for c in sentence:
        ## letters
        if c.isalpha():
            letters+=1
        ## numbers
        elif c.isdigit():
            numbers+=1
        ## space
        elif c.isspace():
            spaces+=1		
        elif c == '?':
            questions+=1
    	## exclamation
        elif c == '!':
            exclamation+=1
        ## other than alphanumeric
        else:
            others+=1
    #return 
    return length, letters, numbers, spaces, questions, exclamation, others


### Language/word level feature extraction
def languageFeatures(sentence, affinEnglish, affinMalay):
    ## get words as list from clean & lower case sentence
    cleanSentence=numPuntuationCleaner(sentence.lower())
    words = ngrams(cleanSentence,1).keys()
    number_of_words=float(len(words))
    ##get English & Malay affin score
    positiveEng, negativeEng = sentimetScores(words, affinEnglish)
    positiveMalay, negativeMalay = sentimetScores(words, affinMalay)
    ## ratio of positive and negative words
    positive_score = positiveEng + positiveMalay
    negative_score = negativeEng + negativeMalay
    ## return
    return number_of_words, positive_score, negative_score


### Twitter specific counters
def twitterCounters(sentence):
    hashtags = len(re.findall(r"#(\w+)", sentence))
    name_mentions = len(re.findall(r"@(\w+)", sentence))
    urls = len(re.findall(r"http://(\w+)", sentence))
    ## return
    return hashtags, name_mentions, urls


### Emoji/smily specific counters(ASCII)
def emojiCountersASCII(sentence):
    # ASCII emoji
    happyRe="[:;8xX=]'?.?[\)D\}\]3]|\^.?\^|#.?\)"
    sadRe="[:;]'?.?[\(D\{\[<Cc#]"
    angryRe= ":.?[\|@]|>:.?\("
    horrorRe="D'?:?[<8;:X]|v.?v"
    embarashedRe=":'?.?\$"
    happy=len(re.findall(happyRe, sentence))
    sad=len(re.findall(sadRe, sentence))
    angry=len(re.findall(angryRe, sentence))
    horror=len(re.findall(horrorRe, sentence))
    embarashed=len(re.findall(embarashedRe, sentence))
    positive = happy
    negative = sad+angry+horror+embarashed
    #return
    return positive, negative

    
## combine all feature
def getFeatures(text, affinEnglish, affinMalay):
    ## character level features
    f_char_length, \
    f_letters, \
    f_numbers, \
    f_spaces, \
    f_questions, \
    f_exclamations, \
    f_others \
    =collectCharCounters(text)
    ## language level features
    f_number_of_words, f_positive_score, f_negative_score = \
    languageFeatures(text, affinEnglish, affinMalay)
    ## twitter specific features
    f_hashtags, f_name_mentions, f_urls = twitterCounters(text) 
    ## Emoji/smily specific counters
    f_emoji_positive, f_emoji_negative = emojiCountersASCII(text)
    ## return as csv
    return str(f_char_length)+","+str(f_letters)+","+str(f_numbers)+","+str(f_spaces)+","+str(f_questions)+","+str(f_exclamations)+","+str(f_others)+","+str(f_number_of_words)+","+str(f_positive_score)+","+str(f_negative_score)+","+str(f_hashtags)+","+str(f_name_mentions)+","+str(f_urls)+","+str(f_emoji_positive)+","+str(f_emoji_negative)


### spark streaming related functions

def utf8_decoder_ignore_error(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('utf-8', "ignore")


#def scoreSentiment(texts, affinEnglish, affinMalay, twitterSentimentRF):
#    # using .transform operation to perform RDD-to-RDD operations on Dstream
#    predictions = texts.map( lambda txt :  getFeatures(txt, affinEnglish, affinMalay) ).\
#                    map(lambda features : features.split(',') ).\
#                    map( lambda features : [float(i) for i in features] ).\
#                    transform(lambda  rdd: twitterSentimentRF.predict(rdd)).\
#                    map(lambda x: str(x)).cogroup(texts)
#    return predictions


def scoreSentiment(tweets, affinEnglish, affinMalay, twitterSentimentRF):
    # using .transform operation to perform RDD-to-RDD operations on Dstream
    #length=texts.count()
    predictions = tweets.map( lambda (id, created_at, text) :  ((id, created_at, text) , getFeatures(text, affinEnglish, affinMalay)) ).\
     map(lambda (tweet, features) : (tweet ,(features.split(','))) ).\
     map( lambda (tweet, features) : (tweet, ([float(i) for i in features])) ).\
     transform( lambda  rdd: sc.parallelize(\
       map( lambda x,y:(x,y), twitterSentimentRF.predict(rdd.map(lambda (x,y):y)).collect(),rdd.map(lambda (x,y):x).collect() )\
       )\
     )
    return predictions


def updateSolrIdx(jsonVals, solrHost, solrPort, collection):
    # solr indexing
    for item in jsonVals: 
        solrData = str(item).replace('\'{','{').replace('}\'','}')
        pipe = subprocess.Popen(['curl', '-X', 'POST', '-H', 'Content-Type: application/json', 'http://'+solrHost+':'+str(solrPort)+'/solr/'+collection+'/update/json/docs', '--data-binary', solrData], stdout=subprocess.PIPE)
        # collect status
        out, err = pipe.communicate()
        print out


def updateSolrIdxBatch(jsonVals, solrHost, solrPort, collection, solrBatchSize):
    # solr indexing
    solrDataBatch=''
    count=0
    solrBatchSize=int(solrBatchSize)
    for item in jsonVals:
        solrData = str(item).replace('\'{','{').replace('}\'','}')
        solrDataBatch=solrData+','+solrDataBatch
        count=count+1
        if count%solrBatchSize == 0:
            solrDataBatch='['+solrDataBatch[:-1]+']'
            pipe = subprocess.Popen(['curl', '-X', 'POST', '-H', 'Content-Type: application/json', 'http://'+solrHost+':'+str(solrPort)+'/solr/'+collection+'/update/json/docs', '--data-binary', solrDataBatch], stdout=subprocess.PIPE)
            # collect status
            out, err = pipe.communicate()
            print out
            print solrDataBatch
            solrDataBatch=''

    if len(solrDataBatch)>10:
        solrDataBatch='['+solrDataBatch[:-1]+']'
        pipe = subprocess.Popen(['curl', '-X', 'POST', '-H', 'Content-Type: application/json', 'http://'+solrHost+':'+str(solrPort)+'/solr/'+collection+'/update/json/docs', '--data-binary', solrDataBatch], stdout=subprocess.PIPE)
        # collect status
        out, err = pipe.communicate()
        print out



### streaming main function
if __name__ == "__main__":
    # check there are 6 parameters
    # 0: script itself
    # 1: kafka broker list (host:port)
    # 2: kafka topic name
    # 3: solr host/ip
    # 4: solr port
    # 6: configuration files location in HDFS
    # 7: solr collection/index name
    # 8: number of json doc/item to index per batch
    if len(sys.argv) != 8:
        exit(-1)

    ## initiate spark context
    sc = SparkContext(appName="PythonStreamingDirectKafkaTweetSentiments")
    brokers, topic, solrHost, solrPort, confDirHDFS, solrCollection, solrBatchSize = sys.argv[1:]

    ## load libraries & model ###########
    affinListEng=sc.textFile(confDirHDFS+'/AFINN-111_eng.txt')
    affinListMalay=sc.textFile(confDirHDFS+'/AFINN-111_malay.txt')
    affinEnglish=loadDict(affinListEng.collect(), ',')
    affinMalay=loadDict(affinListMalay.collect(), ',')

    twitterSentimentRF = RandomForestModel.load(sc, confDirHDFS+"/twitterSentimentRF.model")

    ## initiate spark streaming context
    ssc = StreamingContext(sc, 2)

    # parse tweets and collect text
    # brokers, topic, solrHost, solrPort = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}, keyDecoder=utf8_decoder_ignore_error, valueDecoder=utf8_decoder_ignore_error )
    lines = kvs.map(lambda x: x[1])
    # keep all the texts between widest curly braces
    lines = lines.map(lambda x: "{"+re.findall(r'\{(.+)\}',x)[0]+"}")
    # convert to ascii	
    #texts = lines.map(lambda x: json.loads(x)['text'].encode('ascii', "ignore"))
    tweets = lines.map(lambda x :(\
                          json.loads(x)['id'], 
                          json.loads(x)['created_at'].encode('ascii', "ignore"),\
                          json.loads(x)['text'].encode('ascii', "ignore")\
                      )\
             ) 
    #tweets.pprint()
    ## calculate sentiment
    predictions = scoreSentiment(tweets, affinEnglish, affinMalay, twitterSentimentRF)
    #predictions.pprint()   
    # convert to json format. Here created_at is GMT but hout_id & date_id are local time(MYT)
    jsonPreds = predictions. map(lambda (sentiment,(id, created_at, text)): \
                            (id, datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y'),text,sentiment)\
                 ).map(lambda (id, created_at, text, sentiment):\
                               '{"id":"'+str(id)+'",'\
                               '"created_at":"'+created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')+'",'\
                               '"date_id":"'+str((created_at+timedelta(hours=8)).strftime('%Y%m%d'))+'",'\
                               '"hour_id":"'+str((created_at+timedelta(hours=8)).strftime('%H'))+'",'\
                               '"text":"'+text+'",'\
                               '"sentiment":"'+str(int(sentiment))+'"}'
                 )
    
    #jsonPreds.map(lambda x:str(x)).pprint()

    jsonPreds.foreachRDD( lambda rdd: updateSolrIdxBatch( rdd.collect() , solrHost, solrPort, solrCollection, solrBatchSize) ) 
 
    ssc.start()
    ssc.awaitTermination()

 
