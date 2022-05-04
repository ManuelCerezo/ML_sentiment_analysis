import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="RANDFORSEST")
from pyspark.sql.session import SparkSession
spark = SparkSession(sc)

#read the dataset
df=spark.read.csv('data.csv',inferSchema=True,header=True)

from pyspark.ml.feature import Tokenizer
tokenization=Tokenizer(inputCol='news',outputCol='tokens')
tokenized_df=tokenization.transform(df)

from pyspark.ml.feature import StopWordsRemover
stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')
refined_df=stopword_removal.transform(tokenized_df)

from pyspark.ml.feature import HashingTF, IDF

hashingTF = HashingTF(inputCol="refined_tokens", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(refined_df)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

from pyspark.sql.functions import col, lit

column = 'final_manual_labelling'

rescaledData = rescaledData.withColumn(column, col(column) + lit(1))


train, test = rescaledData.randomSplit([0.7, 0.3], seed = 2018)

from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'final_manual_labelling')
rfModel = rf.fit(train)
rfModel.write().save("./Model_RF_V1")
print("terminado create_model")