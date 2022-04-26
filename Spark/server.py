import findspark
import pyspark
findspark.init()
sc = pyspark.SparkContext(appName="SERVER")
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
spark = SparkSession(sc)
from operator import add

sc.setLogLevel("ERROR")
ssc = StreamingContext(sc,5000)

# We need to create the checkpoint
ssc.checkpoint("checkpoint")

# Create a DStream that will connect to hostname:port, like localhost:9999

lines = ssc.socketTextStream("localhost", 10002)


try:
    print(lines.pprint())
    #    print(lines.flatMap(lambda x: x.split(' ')) \
    #           .map(lambda word : (word, 1)) \
    #           .reduceByKeyAndWindow(add,30,20).pprint())
except:
       print("se necesitan datos bro")


ssc.start()             # Start the computation
ssc.awaitTermination() 