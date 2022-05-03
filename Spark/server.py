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
sc = SparkContext(appName="SERVER")
sc.setLogLevel("ERROR") ##-> Ocultar warnings
spark = SparkSession(sc)
vader_analyzer = SentimentIntensityAnalyzer()

#Inicializacion de Contexto2
ssc = StreamingContext(sc,1) #lectura cada 3s
lines = ssc.socketTextStream("localhost",12345)


apply_vader = udf(lambda x: vader_analyzer.polarity_scores(x)['compound'],FloatType())
apply_textBlob = udf(lambda x: TextBlob(x).sentiment.polarity,FloatType())

def data_serialize(rdd):
    global spark
    global vader_analyzer
    print(type(rdd))
    #print(rdd.collect())
    df = rdd.toDF(['fuente','url','notice','notice-date','process-date'])
    df = df.withColumn("notice", apply_vader(col('vader-polarity')))
    df = df.withColumn("notice", apply_textBlob(col('textBlob-polarity')))
    #df = spark.createDataFrame(['data0','data1','data2','data3','data4'],rdd[0])
    #Creacion de dataframe de ventana de trabajo
    # rowRdd = rdd.map(lambda x: Row( fuente = x[0],url=x[1],news = x[2],notice_date=x[3],\
    #     vader_polarity = vader_analyzer.polarity_scores(x[2])['compound']\
    #         ,textblob_polarity = TextBlob(x[2]).sentiment.polarity,\
    #             process_time = x[4]))
    df.show(6)
    # df = spark.createDataFrame(rowRdd) #Almacenamos los datos temporalmente
    # df.createOrReplaceTempView("cryptonews")
    # #df.show(5)
    # print("tamaño: ",df.count())
    # df = df.toPandas()
    
    # #print(df['fuente'].values.tolist())
    # a = df['process_time'].values.tolist()
    # b = df['vader_polarity'].values.tolist()
    

def send_to_server():
    ## obtener datos del dataframe
    ##### SEND TO WEB SERVER ####
    try:
        r = requests.post('http://localhost:5000/puerta-enlace/setdatos', json={
            "process_time": 'a',
            "vader_polarity": 'a'
        })
    except:
        print("Error al mandar los datos al servidor web")


    pass


def prueba(): #FUNCION PARA HACER PRUEBAS
    df_subset = spark.sql("select fuente, news, vader_polarity, process_time from cryptonews")
    print(df_subset.show())
    print(df_subset.count())


#===== Procesamiento de los datos =======
lines.window(10,3).map(lambda x:x.split(';')).foreachRDD(data_serialize)
#========================================

#Fin del Bucle
ssc.start()
ssc.awaitTermination()