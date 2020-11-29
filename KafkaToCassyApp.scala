package com.practice.streaming.runner

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Date

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector._

object KafkaToCassyApp extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Streaming Example")
    .enableHiveSupport()
    .getOrCreate()

  //localhost:9042

  val cluster = Cluster.builder().addContactPoint("localhost").withCredentials("cassandra","cassandra").withPort(9042).build()
  val session = cluster.connect("sasidhar")

  session.execute("CREATE TABLE IF NOT EXISTS word_count (word text, ts timestamp, count int, PRIMARY KEY(word, ts))")
  session.close()


  val ssc = new StreamingContext(spark.sparkContext,Seconds(3))

  val kafkaParams = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
  ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
  ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
  ConsumerConfig.GROUP_ID_CONFIG -> "quickstart-events51")

  val topicSet = Set[String]("quickstart-events")

  val messages = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String, String](topicSet, kafkaParams))

  messages.map(x=>{
    println(s"${x.key()} is key ${x.value()} is value ${x.offset()} as offset ${x.partition()} as partition ${x.timestamp()} as tiimestamp " +
      s"${x.topic()} as topic")
    x.value().toString
  }).foreachRDD(rdd=> {
    rdd.flatMap(x=>x.split(" "))
      .map(x => (x,1))
      .reduceByKey(_+_)
      .saveToCassandra("sasidhar", "wordcounts")
  })

  ssc.start()
  ssc.awaitTermination()

}
