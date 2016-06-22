## split a frame to train and test sets
splitdf <- function(dataframe, trainingpct=70 ,seed=NULL) {
	if (!is.null(seed)) set.seed(seed)
	index <- 1:nrow(dataframe)
	trainindex <- sample(index, trunc(length(index) * trainingpct/100))
	trainset <- dataframe[trainindex, ]
	testset <- dataframe[-trainindex, ]
	list(trainset=trainset,testset=testset)
}


#split datasets in train and test
data <- read.csv("TrainingTweetsScore.csv")
data$sentiment <- as.factor(data$sentiment)

dataset <- splitdf(data, trainingpct=80, seed=1234)
train <- dataset$trainset
test <- dataset$testset


--random forest
library(randomForest)
rf_model1 <- randomForest(sentiment ~ . , data=train, ntree=100, importance=T)
table(predict(rf_model1, test) == test$sentiment)
FALSE  TRUE 
  819  1681
 
## filter variables with importance
imp <- as.data.frame(importance(rf_model1))
imp[imp$MeanDecreaseAccuracy > 5, ]
rownames(imp[imp$MeanDecreaseAccuracy > 5, ])

## after removing less important variables along with a consecutive char variable(as calculating them are costly)
## still keeping emoji negative
rf_model2 <- randomForest(sentiment ~ char_length+letters+spaces+exclamations+others+number_owords+positive_score+negative_score+name_mentions+urls+emoji_positive+emoji_negative, data=train, ntree=100, importance=T)

table(predict(rf_model2, test) == test$sentiment)
FALSE  TRUE 
  832  1668

test_out <- as.data.frame(predict(rf_model2, test, "prob"))

# So, we will build random forest with 100 trees and all other default values
# almost 70% accurate
# So, while feature extraction ,we will not calculate those consecutive char variable and final model will be as below:
names(data)
 [1] "sentiment"      "char_length"    "letters"        "numbers"        "spaces"        
 [6] "questions"      "exclamations"   "others"         "number_owords"  "positive_score"
[11] "negative_score" "hashtags"       "name_mentions"  "urls"           "emoji_positive"
[16] "emoji_negative"

final_rf <- randomForest(sentiment ~ . , data=train, ntree=100, importance=T)

table(predict(final_rf, test) == test$sentiment)
FALSE  TRUE 
  826  1674 
  
test_out <- as.data.frame(predict(final_rf, test, "prob"))

test_out$sentiment <- ifelse(abs(test_out$0 - test_out$1) <20 ,0, '')




  