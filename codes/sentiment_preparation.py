import re, string

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
def loadDict(filename, seperator):
    file = open(filename, 'r')
    dict = {} 
    for line in file.readlines(): 
        d = line.strip().split(seperator) 
        dict[d[0]] = d[1]
    file.close()   
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


############ load libraries ###########	
fileEngDict='AFINN-111_eng.txt'
fileMalayDict='AFINN-111_malay.txt'
sep=','
affinEnglish=loadDict(fileEngDict, sep)
affinMalay=loadDict(fileMalayDict, sep)


###############test#############3

sentence='I @obaid@obaidsce agreeeee :) :@  @obaidul ?? #haha ðŸ’• on ðŸ˜¬111 #hoho hina 12!!! @@@$%'
getFeatures(sentence, affinEnglish, affinMalay)


######preparing training set######
## divide the trainig set to a 10K english and 2.5K malay dataset
import random
nums = [x for x in range(1578627)]
random.seed(1234567890)
random.shuffle(nums)
EngIdx=nums[:10000]
MalIdx=nums[10000:12500]


filenameSource="Sentiment Analysis Dataset.csv"
filenameEng="Sentiment_Analysis_Dataset_Eng.csv"
filenameMal="Sentiment_Analysis_Dataset_Mal.csv"

fileSource = open(filenameSource, 'r')
fileSource.readline()

fileEng = open(filenameEng, 'w')
fileMal = open(filenameMal, 'w')

count=0

for line in fileSource.readlines():
    print count
    if count in EngIdx:
        fileEng.write(line)
    elif count in MalIdx:
        fileMal.write(line)
    count+=1

fileSource.close()
fileEng.close()
fileMal.close()


## Translate 5K dataset to malay
"Sentiment_Analysis_Dataset_Mal.csv"

## then combine English and malay tweets a TrainingTweets.csv

### extract features from tweets
import os, csv

filenameSource="TrainingTweets.csv"
filenameTarget="TrainingTweetsScore.csv"

fileSource = open(filenameSource, 'r')
fileTarget = open(filenameTarget, 'w')

#write header as csv
fileTarget.write("sentiment,char_length,letters,numbers,spaces,questions,exclamations,others,number_owords,positive_score,negative_score,hashtags,name_mentions,urls,emoji_positive,emoji_negative\n")
# read as csv
reader = csv.reader(fileSource)
for line in reader:
    id=line[0]
    f_sentiment=line[1]
    f_text=line[2]
    print f_sentiment+":"+f_text
    
    f_features=getFeatures(f_text, affinEnglish, affinMalay)
    fileTarget.write(f_sentiment+","+f_features+"\n")

fileSource.close()
fileTarget.close()


