from concurrent.futures import process
import requests
from pyspark import Row, SparkContext
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


rf_model = RandomForestClassificationModel.load("./Spark/Model_RF_V1")
 
# Para que funcione la clase es necesario que se cree primero 
# el modelo contenido en RandomForest.ipnb

def predict(data):
    df1 = spark.createDataFrame([(data,)], ["news"])
    tokenization=Tokenizer(inputCol='news',outputCol='tokens')
    tokenized_df=tokenization.transform(df1)
    stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')
    refined_df=stopword_removal.transform(tokenized_df)
    hashingTF = HashingTF(inputCol="refined_tokens", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(refined_df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    predictions = rf_model.transform(rescaledData)
    probabilidad = predictions.head().probability
    if probabilidad[0] > (probabilidad[1] and probabilidad[2]):#negativo
        #print('negativo')
        probabilidad = probabilidad[0]*-1
    elif probabilidad[1] > (probabilidad[0] and probabilidad[2]):#neutro
        #print('neutro')
        probabilidad = 1 - probabilidad[1]
    elif probabilidad[2] >(probabilidad[0] and probabilidad[1]):#positivo
        #print('positivo')
        probabilidad = probabilidad[2]
    print(float(probabilidad))
    print("siguiente")
    return float(probabilidad)


#Inicializacion de Funciones UDF
apply_vader = udf(lambda x: vader_analyzer.polarity_scores(x)['compound'],FloatType())
apply_textBlob = udf(lambda x: TextBlob(x).sentiment.polarity,FloatType())
#apply_mymodel = udf(lambda x: predict(x),FloatType())

def apply_myModel(df):
    tokenization=Tokenizer(inputCol='_3',outputCol='tokens')
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
    df = rdd.toDF()

    df = df.withColumn("vader_polarity", apply_vader(col('_3')))
    df = df.withColumn("textBlob_polarity", apply_textBlob(col('_3')))
    df = apply_myModel(df)
    df = df.withColumn("textBlob_polarity", apply_textBlob(col('_3')))
    df = df.withColumn("myModel_polarity", col('prediction'))

    df = df.drop(*("tokens", "refined_tokens", "rawFeatures", "features", "rawPrediction", "prediction"))

    df.show(2)
    #rowRdd = rdd.map(lambda x: Row(fuente = x[0], url = x[1], notice = x[2], notice_date = x[3], process_date = x[4], myModel = predict(x[2])))
    #df1 = spark.createDataFrame(rowRdd)
    #df1.show(2)
    # df = rdd.toDF(['fuente','url','notice','notice_date','process_date'])
    # df = df.withColumn("vader_polarity", apply_vader(col('notice')))
    # df = df.withColumn("textBlob_polarity", apply_textBlob(col('notice')))
    # df = df.withColumn("mymodel_polarity", apply_mymodel(col('notice')))

    # df.createOrReplaceTempView("cryptonews")
    # df.show(2)
    #send_to_server()

    

# def send_to_server():
#     global spark
#     data_to_sent = {}

#     ## obtener datos del dataframe
#     ##### SEND TO WEB SERVER ####
#     df = spark.sql('select vader_polarity, textBlob_polarity, mymodel_polarity, process_date from cryptonews')
#     df.show(5)


#     data_to_sent['labels'] = df.select("process_date").toPandas().values.tolist()
#     data_to_sent['vader'] = df.select("vader_polarity").toPandas().values.tolist()
#     data_to_sent['textblob'] = df.select("textBlob_polarity").toPandas().values.tolist()
#    # data_to_sent['mymodel'] = df.select("mymodel_polarity").toPandas().values.tolist()

    
#     r = requests.post('http://localhost:5000/puerta-enlace/setdatos', json={
#         "data": data_to_sent
#     })
    


#===================== Procesamiento de los datos =======================
lines.window(10,3).map(lambda x:x.split(';')).foreachRDD(data_serialize)
#========================================================================
#Fin del Bucle
ssc.start()
ssc.awaitTermination()