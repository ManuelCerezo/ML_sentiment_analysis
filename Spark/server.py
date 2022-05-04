import requests
from pyspark import  SparkContext
from pyspark.streaming import StreamingContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.session import SparkSession
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.functions import *


#Inicializacion de Contexto1
import findspark
import pandas as pd
findspark.init()
sc = SparkContext(appName="SERVER")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
vader_analyzer = SentimentIntensityAnalyzer()

#Inicializacion de Contexto2
ssc = StreamingContext(sc,1) #lectura cada 3s
lines = ssc.socketTextStream("localhost",12345)

#Creacion clase

#Required libraries
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF

from pyspark.ml.classification import RandomForestClassificationModel


rf_model = RandomForestClassificationModel.load("./Model_RF_V1")
 
# Para que funcione la clase es necesario que se cree primero 
# el modelo contenido en RandomForest.ipnb

#Inicializacion de Funciones UDF
apply_vader = udf(lambda x: vader_analyzer.polarity_scores(x)['compound'],FloatType())
apply_textBlob = udf(lambda x: TextBlob(x).sentiment.polarity,FloatType())
apply_cast_mymodel = udf(lambda x: x-1,FloatType())
#apply_mymodel = udf(lambda x: predict(x),FloatType())

def apply_myModel(df):
    tokenization=Tokenizer(inputCol='notice',outputCol='tokens')
    tokenized_df=tokenization.transform(df)
    stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')
    refined_df=stopword_removal.transform(tokenized_df)
    hashingTF = HashingTF(inputCol="refined_tokens", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(refined_df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    predictions = rf_model.transform(rescaledData)
    
    return predictions


def data_serialize(rdd): 

    df = rdd.toDF(['fuente','url','notice','notice_date','process_date'])
    df = df.withColumn("vader_polarity", apply_vader(col('notice')))
    df = df.withColumn("textBlob_polarity", apply_textBlob(col('notice')))
    df = apply_myModel(df)
    df = df.withColumn("textBlob_polarity", apply_textBlob(col('notice')))
    df = df.withColumn("myModel_prediction", apply_cast_mymodel(col('prediction')))

    df = df.drop(*("tokens", "refined_tokens", "rawFeatures", "features", "rawPrediction", "prediction","probability"))

    df.createOrReplaceTempView("cryptonews")
    df.show(20)
    send_to_server()

    

def send_to_server():
    global spark
    data_to_sent = {}

    ## obtener datos del dataframe
    ##### SEND TO WEB SERVER ####
    df = spark.sql('select vader_polarity, textBlob_polarity, myModel_prediction, process_date from cryptonews')
    df.show(5)


    data_to_sent['labels'] = df.select("process_date").toPandas().values.tolist()
    data_to_sent['vader'] = df.select("vader_polarity").toPandas().values.tolist()
    data_to_sent['textblob'] = df.select("textBlob_polarity").toPandas().values.tolist()
    data_to_sent['mymodel'] = df.select("myModel_prediction").toPandas().values.tolist()

    
    r = requests.post('http://localhost:5000/puerta-enlace/setdatos', json={
        "data": data_to_sent
    })
    


#===================== Procesamiento de los datos =======================
lines.window(10,3).map(lambda x:x.split(';')).foreachRDD(data_serialize)
#========================================================================
#Fin del Bucle
ssc.start()
ssc.awaitTermination()