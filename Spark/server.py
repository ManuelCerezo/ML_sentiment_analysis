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
from pyspark.ml.classification import RandomForestClassificationModel

#Required libraries for implementing model
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF

CODE_SPLIT ='A9RTp15Z'

#Inicializacion de Contexto1
import findspark
findspark.init()
sc = SparkContext(appName="SERVER")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
vader_analyzer = SentimentIntensityAnalyzer()

#Inicializacion de Contexto2
ssc = StreamingContext(sc,1) #lectura cada 3s
lines = ssc.socketTextStream("localhost",12345)

#Implementamos Modelo
rf_model = RandomForestClassificationModel.load("./Model_RF_V1")
#Inicializacion de Funciones UDF
apply_vader = udf(lambda x: vader_analyzer.polarity_scores(x)['compound'],FloatType())
apply_textBlob = udf(lambda x: TextBlob(x).sentiment.polarity,FloatType())
apply_cast_mymodel = udf(lambda x: x-1,FloatType())

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
    global indice  
    df = rdd.toDF(['fuente','url','notice','notice_date','process_time'])
    df = df.withColumn("vader_polarity", apply_vader(col('notice')))
    df = df.withColumn("textBlob_polarity", apply_textBlob(col('notice')))
    df = apply_myModel(df)
    df = df.withColumn("textBlob_polarity", apply_textBlob(col('notice')))
    df = df.withColumn("myModel_prediction", apply_cast_mymodel(col('prediction')))

    df = df.drop(*("tokens", "refined_tokens", "rawFeatures", "features", "rawPrediction", "prediction","probability"))

    df.createOrReplaceTempView("cryptonews")
    send_to_server()


##### SEND TO WEB SERVER ####
def send_to_server():
    global spark
    global indice
    data_to_sent = {}
    df = spark.sql('select vader_polarity, textBlob_polarity, myModel_prediction,process_time from cryptonews')
    print("cantidad: ",df.count())
    print("total datos: ",(df.count()*3))

    try:
        data_to_sent['labels'] = df.select('process_time').rdd.flatMap(lambda x: x).collect()
        data_to_sent['vader'] = df.select("vader_polarity").rdd.flatMap(lambda x: x).collect()
        data_to_sent['textblob'] = df.select("textBlob_polarity").rdd.flatMap(lambda x: x).collect()
        data_to_sent['mymodel'] = df.select("myModel_prediction").rdd.flatMap(lambda x: x).collect()
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