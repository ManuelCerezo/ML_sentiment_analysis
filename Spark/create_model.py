from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql.functions import col, lit
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer

import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="RANDFORSEST")
from pyspark.sql.session import SparkSession
spark = SparkSession(sc)

#read the dataset
df=spark.read.csv('data.csv',inferSchema=True,header=True)

tokenization=Tokenizer(inputCol='news',outputCol='tokens')
tokenized_df=tokenization.transform(df)

stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')
refined_df=stopword_removal.transform(tokenized_df)



hashingTF = HashingTF(inputCol="refined_tokens", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(refined_df)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

column = 'final_manual_labelling'

rescaledData = rescaledData.withColumn(column, col(column) + lit(1))
train, test = rescaledData.randomSplit([0.7, 0.3], seed = 2018)

rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'final_manual_labelling')
rfModel = rf.fit(train)
predictions = rfModel.transform(test)

true_postives = predictions[(predictions.final_manual_labelling == 2) & (predictions.prediction == 2)].count()
true_negatives = predictions[(predictions.final_manual_labelling == 0) & (predictions.prediction == 0)].count()
true_neutral = predictions[(predictions.final_manual_labelling == 1) & (predictions.prediction == 1)].count()


false_postives = predictions[(predictions.final_manual_labelling == 2) & (predictions.prediction != 2)].count()
false_negatives = predictions[(predictions.final_manual_labelling == 0) & (predictions.prediction != 0)].count()
false_neutral = predictions[(predictions.final_manual_labelling == 1) & (predictions.prediction != 1)].count()

evaluator = MulticlassClassificationEvaluator(labelCol="final_manual_labelling", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)

rfModel.write().save("./Model_RF_V1")

print("Creacion del modelo terminado")