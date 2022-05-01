from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.session import SparkSession
from textblob import TextBlob


#Inicializacion de Contexto
sc = SparkContext(appName="SERVER")
sc.setLogLevel("ERROR") ##-> Ocultar warnings
ssc = StreamingContext(sc,5) #lectura cada 3s
spark = SparkSession(sc)

lines = ssc.socketTextStream("localhost",12345)
# lines2 = spark.readStream.format("socket").option("host", "localhost").option("port", 12345).load()
#df = spark.createDataFrame(['url','news','date'],str(lines))

#===== Procesamiento de los datos =======
words = lines.flatMap(lambda x : x.split(' '))
words.pprint()

# lines.window(5,5).map(lambda x : vader_analyzer.polarity_scores(x.split(';')[1])['compound']).pprint() #vader
# lines.window(5,5).map(lambda x: TextBlob(x.split(';')[1]).sentiment.polarity).pprint() #TextBlob

#========================================

#Fin del Bucle
ssc.start()
ssc.awaitTermination()
