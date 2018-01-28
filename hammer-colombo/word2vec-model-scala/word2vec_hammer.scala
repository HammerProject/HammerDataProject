// run: spark
// /Users/mauropelucchi/Desktop/My_Home/Tools/spark-2.2.0-bin-hadoop2.7/bin/spark-shell --master=local --driver-memory=4g --name Hammer --conf="spark.driver.maxResultSize=2048"
//


// IMPORT LIBRARIES


import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.classification.{LinearSVC, LogisticRegression, OneVsRest, OneVsRestModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.feature.Word2Vec
import com.mongodb.spark._
import com.mongodb.spark.config._


val readConfig = ReadConfig(Map("collection" -> "dataset_ny", "uri" -> "mongodb://ma-ha-1:27017/hammer.dataset_ny?readPreference=primaryPreferred"))
val dataset = MongoSpark.load(sc, readConfig).toDF

dataset.count // 1711
dataset.printSchema


//val  concatenateMeta = udf((meta: Seq[String], othertags: Seq[String])  => {if(meta != null) meta.union(othertags) else othertags}: Seq[String])
// 
val  concatenateMeta = udf((meta: Seq[String], othertags: Seq[String])  => { othertags }: Seq[String])

// concatenate meta attribute
val dataset_meta = dataset.withColumn("meta_info", concatenateMeta(col("meta"), col("other_tags"))).filter(col("meta_info").isNotNull)
dataset_meta.count

// remove stopwords 
// 
import org.apache.spark.ml.feature.StopWordsRemover
val stopwordremover = new StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english")).setInputCol("meta_info").setOutputCol("meta_filtered")
val dataset_filtered = stopwordremover.transform(dataset_meta)
dataset_filtered.show(10)

// clean set
// remove token with len < 3
// remove special char
val  cleanText = udf((tokens: Seq[String])  => {
  tokens.map(_.replaceAll("[^A-Za-z0-9]","").toLowerCase).filter(token => token.size > 3).toSeq
}: Seq[String])
val dataset_cleaned = dataset_filtered.withColumn("meta_cleaned", cleanText(col("meta_filtered")))
// check documents
dataset_cleaned.select("meta_cleaned").show(1600)



// create a list of "rare" tokens (with TF <= 1)
// and a list of 20 most common tokens
def getDict(line: Seq[String]): Seq[String] =  {
  line.map(d => d.toLowerCase)
}
val word_count = dataset_cleaned.flatMap(d => getDict(d.getSeq(d.fieldIndex("meta_cleaned")))).rdd.map(t => (t,1)).reduceByKey(_ + _)
val rarewords = word_count.filter(_._2 <= 1).map(_._1).collect.toSet
val stopwords = word_count.sortBy(-_._2).map(_._1).take(20).toSet

// total words 3861
// rare words 1594

// clean
// remove rare words
val clean_terms = udf((tokens: Seq[String]) => tokens.filterNot(token => rarewords.contains(token)).toSeq)
val my_terms = dataset_cleaned.withColumn("my_terms",clean_terms(col("meta_cleaned")))
my_terms.show

// show the schema
my_terms.printSchema


// create dictionary
// term, frequency
//val dict = my_terms.flatMap(d => getDict(d.getSeq(d.fieldIndex("my_terms"))).map(x => (x,1))).groupByKey(x=> x._1).reduceGroups((x,y) => (y._1, x._2 + y._2)).map(x => x._2).toDF("term","tf").sort(desc("tf"))
//dict.count
// check tokens
//dict.show



val word2vec = new Word2Vec().setSeed(1234).setInputCol("my_terms").setMinCount(2)
val word2vecModel = word2vec.fit(my_terms)
word2vecModel.findSynonyms("school",5).map(x => x.getString(0)).collectAsList
//  [system, exam, year, years, take]

word2vecModel.findSynonyms("city",5).map(x => x.getString(0)).collectAsList
// [housing, facilities, centers, parks, temporary]

word2vecModel.findSynonyms("infraction",5).map(x => x.getString(0)).collectAsList
// [skills, charged, events, driving, sustainability]

// save the model
word2vecModel.save("/Users/mauropelucchi/Desktop/word2vecModel_hammer_ny")




