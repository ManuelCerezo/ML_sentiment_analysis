import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="LOGISTICREG")
from pyspark.sql.session import SparkSession
spark = SparkSession(sc)

from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

def read_csv():
    df=spark.read.csv('data.csv',inferSchema=True,header=True)
    return df

def tokenizazor(df=read_csv()):
    tokenization=Tokenizer(inputCol='news',outputCol='tokens')
    tokenized_df=tokenization.transform(df)
    return tokenized_df
    
def remove_stopwords(tokenized_df=tokenizazor()):
    stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')
    refined_df=stopword_removal.transform(tokenized_df)
    return refined_df

def vectorizer(refined_df=remove_stopwords()):
    count_vec=CountVectorizer(inputCol='refined_tokens',outputCol='features')
    cv_df=count_vec.fit(refined_df).transform(refined_df)
    return cv_df

def colum_token_count(cv_df=vectorizer()):
    len_udf = udf(lambda s: len(s), IntegerType())
    refined_text_df = cv_df.withColumn("token_count", len_udf(col('refined_tokens')))
    return refined_text_df

def cast_to_float_label(refined_text_df=colum_token_count()):
    refined_text_df = refined_text_df.withColumn("Label", refined_text_df.final_manual_labelling.cast('float')).drop('final_manual_labelling')
    return refined_text_df

def negative_positive_models(refined_text_df=cast_to_float_label()):
    funct_negative_label = udf(lambda x: 1.00 if x == -1 else 0.00, FloatType())
    func_positive_label = udf(lambda x: 1.00 if x == 1 else 0.00, FloatType())

    refined_text_df = refined_text_df.withColumn("LabelNegative",funct_negative_label('Label'))
    refined_text_df = refined_text_df.withColumn("LabelPositive",func_positive_label('Label'))

    df_assembler = VectorAssembler(inputCols=['features','token_count'],outputCol='features_vec')
    model_text_df = df_assembler.transform(refined_text_df)
    #split the data 
    training_df_negative,test_df_negative=model_text_df.randomSplit([0.75,0.25])
    #split the data 
    training_df_positive,test_df_positive=model_text_df.randomSplit([0.75,0.25])
    log_reg_positive = LogisticRegression(featuresCol='features_vec',labelCol='LabelPositive').fit(training_df_positive)
    log_reg_negative = LogisticRegression(featuresCol='features_vec',labelCol='LabelNegative').fit(training_df_negative)
    results_positive = log_reg_positive.evaluate(test_df_positive).predictions
    results_negative = log_reg_negative.evaluate(test_df_negative).predictions
    view_results(results_positive,"Positive Model")
    view_results(results_negative,"Negative Model")
    return log_reg_positive,log_reg_negative

def view_results(result,model):
    true_postives = result[(result.Label == 1) & (result.prediction == 1)].count()
    true_negatives = result[(result.Label == 0) & (result.prediction == 0)].count()
    false_positives = result[(result.Label == 0) & (result.prediction == 1)].count()
    false_negatives = result[(result.Label == 1) & (result.prediction == 0)].count()
    recall = float(true_postives)/(true_postives + false_negatives)
    precision = float(true_postives) / (true_postives + false_positives)
    accuracy=float((true_postives+true_negatives) /(result.count()))

    print("-----",model,"-----\n")
    print("Recall: ",recall)
    print("Precision: ",precision)
    print("Accuracy: ",accuracy,"\n")



if __name__ == "__main__":
    negative_positive_models()