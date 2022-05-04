import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from soupsieve import select
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.session import SparkSession
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.functions import *
from pyspark.sql.types import *

CODE_SPLIT ='A9RTp15Z'

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
    global indice  
    df = rdd.toDF(['fuente','url','notice','notice_date','process_time'])
    df = df.withColumn("vader_polarity", apply_vader(col('notice')))
    df = df.withColumn("textBlob_polarity", apply_textBlob(col('notice')))
    
    df.createOrReplaceTempView("cryptonews")
    send_to_server()


##### SEND TO WEB SERVER ####
def send_to_server():
    global spark
    global indice
    data_to_sent = {}
    df = spark.sql('select vader_polarity, textBlob_polarity, process_time from cryptonews')
    print("cantidad: ",df.count())

    try:
        data_to_sent['labels'] = df.select('process_time').rdd.flatMap(lambda x: x).collect()
        data_to_sent['vader'] = df.select("vader_polarity").rdd.flatMap(lambda x: x).collect()
        data_to_sent['textblob'] = df.select("textBlob_polarity").rdd.flatMap(lambda x: x).collect()
    except:
        print("Error en recoleccion de'collect()' datos de columnas...")
    
    requests.post('http://localhost:5000/puerta-enlace/setdatos', json={
        "data": data_to_sent
    })
    


#===================== Procesamiento de los datos =======================
lines.window(5,2).map(lambda x:x.split(CODE_SPLIT)).foreachRDD(data_serialize)
#========================================================================
#Fin del Bucle
ssc.start()
ssc.awaitTermination()