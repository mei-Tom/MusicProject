package com.scala.musicproject.streaming

import java.util
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import com.alibaba.fastjson.JSON
import com.scala.musicproject.base.RedisClient
import com.scala.musicproject.common.ConfigUtils
import redis.clients.jedis.Pipeline

import scala.collection.mutable

/**
  * 此类是实时获取用户的登录系统数据，每隔5秒统计在线用户的pv，uv
  */
object RealTimePVUV {

  private val localrun: Boolean = ConfigUtils.LOCAL_RUN
  private val topic = ConfigUtils.TOPIC
  private val kafkaBrokers = ConfigUtils.KAFKA_BROKERS
  private val redisOffsetDb = ConfigUtils.REDIS_OFFSET_DB
  private val redisDb = ConfigUtils.REDIS_DB
  private var sparkSession : SparkSession = _
  private var sc: SparkContext = _

  def main(args: Array[String]): Unit = {
    if(localrun){
      sparkSession = SparkSession.builder()
        .master("local")
        .appName("ReadTimePVUVInfo")
        .enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
    }else{
      sparkSession = SparkSession.builder().appName("ReadTimePVUVInfo").getOrCreate()
      sc = sparkSession.sparkContext
    }

    sparkSession.sparkContext.setLogLevel("Error")
    val ssc = new StreamingContext(sc,Durations.seconds(5))
    /**
      * 从Redis 中获取消费者offset
      */
    val currentTopicOffset: mutable.Map[String, String] = getOffSetFromRedis(redisOffsetDb,topic)
    //初始读取到的topic offset:
    currentTopicOffset.foreach(tp=>{println(s" 初始读取到的offset: $tp")})
    //转换成需要的类型
    val fromOffsets: Predef.Map[TopicPartition, Long] = currentTopicOffset.map { resultSet =>
      new TopicPartition(topic, resultSet._1.toInt) -> resultSet._2.toLong
    }.toMap

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MyGroupId11",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)//默认是true
    )
    /**
      * 将获取到的消费者offset 传递给SparkStreaming
      */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )


    //统计pv
    stream.map(cr=>{
      val jsonObject = JSON.parseObject(cr.value())
      val mid = jsonObject.getString("mid")
      val uid = jsonObject.getString("uid")
      (mid,1)
    }).reduceByKeyAndWindow((v1:Int,v2:Int)=>{v1+v2},
      Durations.minutes(1),
      Durations.seconds(5))
      .foreachRDD(rdd=>{
        rdd.foreachPartition(iter=>{
          //将结果存储在redis中，格式为K:pv V：mid ,pv
          savePVToRedis(redisDb,iter)
        })
      })

    //统计uv
    stream.map(cr=>{
      val jsonObject = JSON.parseObject(cr.value())
      val mid = jsonObject.getString("mid")
      val uid = jsonObject.getString("uid")
      (mid,uid)
    }).transform(rdd=>{
      val distinctRDD = rdd.distinct()
      distinctRDD.map(tp=>{(tp._1,1)})
    }).reduceByKeyAndWindow((v1:Int,v2:Int)=>{v1+v2},
      Durations.seconds(60),Durations.seconds(5))
      .foreachRDD(rdd=>{
        rdd.foreachPartition(iter=>{
          //将结果存储在redis中，格式为K:uv V：mid ,pv
          saveUVToRedis(redisDb,iter)
        })
      })

    stream.foreachRDD { (rdd:RDD[ConsumerRecord[String, String]]) =>
      println("所有业务完成")
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //将当前批次最后的所有分区offsets 保存到 Redis中
      saveOffsetToRedis(redisOffsetDb,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

  /**
    * 将pv值保存在Redis中
    *
    */
  def savePVToRedis(db:Int,iter:Iterator[(String,Int)]) = {
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    //pipelined()方法获取管道,它可以实现一次性发送多条命令并一次性返回结果,这样就大量的减少了客户端与Redis的通信次数
    val pipeline: Pipeline = jedis.pipelined()
    iter.foreach(tp=>{
      pipeline.hset("pv",tp._1,tp._2.toString)
    })
    pipeline.sync()
    println("PV保存成功")
    RedisClient.pool.returnResource(jedis)
  }
  /**
    * 将UV值保存在Redis中
    *
    */
  def saveUVToRedis(db:Int,iter:Iterator[(String,Int)]) = {
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    //pipelined()方法获取管道,它可以实现一次性发送多条命令并一次性返回结果,这样就大量的减少了客户端与Redis的通信次数
    val pipeline: Pipeline = jedis.pipelined()
    iter.foreach(tp=>{
      pipeline.hset("uv",tp._1,tp._2.toString)
    })
    pipeline.sync()
    println("UV保存成功")
    RedisClient.pool.returnResource(jedis)
  }

  //获取redis中已经存在的 当前toppic的 offset
  def getOffSetFromRedis(db:Int,tp:String)  ={
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    val result: util.Map[String, String] = jedis.hgetAll(topic)
    RedisClient.pool.returnResource(jedis)
    if(result.size()==0){
      result.put("0","0")
      result.put("1","0")
      result.put("2","0")
    }
    import scala.collection.JavaConversions.mapAsScalaMap
    val offsetMap: scala.collection.mutable.Map[String, String] = result
    offsetMap
  }

  /**
    * 将消费者offset 保存到 Redis中
    *
    */
  def saveOffsetToRedis(db:Int,offsetRanges:Array[OffsetRange]) = {
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    offsetRanges.foreach(offsetRange=>{
      println(s"topic:${offsetRange.topic}  partition:${offsetRange.partition}  fromOffset:${offsetRange.fromOffset}  untilOffset: ${offsetRange.untilOffset}")
      jedis.hset(offsetRange.topic, offsetRange.partition.toString,offsetRange.untilOffset.toString)
    })
    println("保存成功")
    RedisClient.pool.returnResource(jedis)
  }

}
