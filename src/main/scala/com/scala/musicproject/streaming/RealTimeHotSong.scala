package com.scala.musicproject.streaming

import java.util
import com.alibaba.fastjson.JSON
import com.scala.musicproject.base.RedisClient
import com.scala.musicproject.common.ConfigUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

/**
  * 此类是实时获取用户点播歌曲日志，每隔30秒，获取最近10分钟歌曲的点播热度，并将结果存入MySQL中。
  */

case class HotSongInfo(songName:String,times:Int)

object RealTimeHotSong {

  private val localrun: Boolean = ConfigUtils.LOCAL_RUN
  private val userPlaySongTopic = ConfigUtils.USER_PLAY_SONG_TOPIC
  private val kafkaBrokers = ConfigUtils.KAFKA_BROKERS
  private val redisOffsetDb = ConfigUtils.REDIS_OFFSET_DB
  private val mysqlUrl = ConfigUtils.MYSQL_URL
  private val mysqlUser = ConfigUtils.MYSQL_USER
  private val mysqlPassWord = ConfigUtils.MYSQL_PASSWORD
  private var sparkSession : SparkSession = _
  private var sc: SparkContext = _

  def main(args: Array[String]): Unit = {
    if(localrun){
      sparkSession = SparkSession.builder()
        .master("local")
        .config("spark.sql.shuffle.partitions",2)
        .appName("RealTimeHotSongInfo").getOrCreate()
      sc = sparkSession.sparkContext
    }else{
      sparkSession = SparkSession.builder().appName("RealTimeHotSongInfo")
        .config("spark.sql.shuffle.partitions",2).getOrCreate()
      sc = sparkSession.sparkContext
    }

    sparkSession.sparkContext.setLogLevel("Error")
    val ssc = new StreamingContext(sc,Durations.seconds(20))
    /**
      * 从Redis 中获取消费者offset
      */
    val currentTopicOffset: mutable.Map[String, String] = getOffSetFromRedis(redisOffsetDb,userPlaySongTopic)
    //初始读取到的topic offset:
    currentTopicOffset.foreach(tp=>{println(s" 初始读取到的offset: $tp")})
    //转换成需要的类型
    val fromOffsets: Predef.Map[TopicPartition, Long] = currentTopicOffset.map { resultSet =>
      new TopicPartition(userPlaySongTopic, resultSet._1.toInt) -> resultSet._2.toLong
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

    //统计实时热门歌曲
    stream.map(cr=>{
      val jsonObject = JSON.parseObject(cr.value())
      val songId = jsonObject.getString("songid")
      val songName = jsonObject.getString("songname")
      (songName,1)
    }).reduceByKeyAndWindow((v1:Int,v2:Int)=>{v1+v2},
      Durations.minutes(1),
      Durations.seconds(20))
      .foreachRDD(rdd=>{
        println("current is start ... ...")
        //将top30热度最高的歌曲，结果保存到MySQL 表 hotsong中
        val hotSongInfo: RDD[HotSongInfo] = rdd.map(tp => {
          val songName = tp._1
          val playTimes = tp._2
          HotSongInfo(songName, playTimes)
        })
        val session = sparkSession.newSession()
        import session.implicits._
        hotSongInfo.toDF().createTempView("temp_song_info")
        session.sql(
          """
            |select
            | songname,times,row_number() over (partition by 1 order by times desc ) as rank
            |from temp_song_info
          """.stripMargin)
            .filter("rank <=30")
            .write.format("jdbc")
            .mode(SaveMode.Overwrite)
            .option("url",mysqlUrl)
            .option("user",mysqlUser)
            .option("password",mysqlPassWord)
            .option("driver","com.mysql.jdbc.Driver")
            .option("dbtable","hotsong")
            .save()

        println("current is finished ... ...")

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

  //获取redis中已经存在的 当前toppic的 offset
  def getOffSetFromRedis(db:Int,tp:String)  ={
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    val result: util.Map[String, String] = jedis.hgetAll(tp)
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
