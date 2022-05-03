import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.session import SparkSession
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.functions import *


#Inicializacion de Contexto1
sc = SparkContext(appName="SERVER")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
vader_analyzer = SentimentIntensityAnalyzer()

#Inicializacion de Contexto2
ssc = StreamingContext(sc,1) #lectura cada 3s
lines = ssc.socketTextStream("localhost",12345)

#Inicializacion de Funciones UDF
apply_vader = udf(lambda x: vader_analyzer.polarity_scores(x)['compound'],FloatType())
apply_textBlob = udf(lambda x: TextBlob(x).sentiment.polarity,FloatType())

def data_serialize(rdd):    
    df = rdd.toDF(['fuente','url','notice','notice_date','process_date'])
    df = df.withColumn("vader_polarity", apply_vader(col('notice')))
    df = df.withColumn("textBlob_polarity", apply_textBlob(col('notice')))
    df.createOrReplaceTempView("cryptonews")
    send_to_server()

    

def send_to_server():
    global spark
    data_to_sent = {}

    ## obtener datos del dataframe
    ##### SEND TO WEB SERVER ####
    df = spark.sql('select vader_polarity, textBlob_polarity, process_date from cryptonews')
    df.show(5)
    data_to_sent['labels'] = df.select("process_date").toPandas().values.tolist()
    data_to_sent['vader'] = df.select("vader_polarity").toPandas().values.tolist()
    data_to_sent['textblob'] = df.select("textBlob_polarity").toPandas().values.tolist()

    
    r = requests.post('http://localhost:5000/puerta-enlace/setdatos', json={
        "data": data_to_sent
    })
    


#===== Procesamiento de los datos =======
lines.window(10,3).map(lambda x:x.split(';')).foreachRDD(data_serialize)
#========================================

#Fin del Bucle
ssc.start()
ssc.awaitTermination()