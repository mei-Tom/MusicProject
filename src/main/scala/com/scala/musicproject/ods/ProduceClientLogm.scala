package com.scala.musicproject.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.scala.musicproject.base.PairRDDMultipleTextOutputFormat
import com.scala.musicproject.common.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 将运维人员每日上传的用户行为数据进行归类整理，并取出用户点歌行为数据，
 * 将数据上传至Hive中的TO_CLIENT_SONG_OPERATE_REQ_D表中；
 *
 * @author meii
 * @create 2023-02-27-14:06
 */
object ProduceClientLogm {
  private val localrun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDataBase = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _
  private var sc: SparkContext = _
  private val hdfsclientlogpath: String = ConfigUtils.HDFS_CLIENT_LOG_PATH
  private var clientLogInfos: RDD[String] = _

  def main(args: Array[String]): Unit = {
    /*
    Part1.首先判断是否传入参数，参数格式：20230203，获取日期数据
     */
    if (args.length < 1) {
      println(s"需要指定日期数据：(参考格式：20230101)")
      System.exit(1)
    }
    val logDate = args(0)

    /*
    Part2.判断运行模式，本地运行还是集群运行
     */
    if (localrun) {
      sparkSession = SparkSession.builder()
        .master("local")
        .appName("ProduceClientLogM")
        .config("hive.metastore.uris", hiveMetaStoreUris)
        .enableHiveSupport()
        .getOrCreate()
    } else {
      sparkSession = SparkSession.builder()
        .appName("ProduceClientLogM")
        .enableHiveSupport()
        .getOrCreate()
      val sc: SparkContext = sparkSession.sparkContext
      val clientLogInfos: RDD[String] = sc.textFile(s"${hdfsclientlogpath}/currentday_clientlog.tar.gz")
    }

    /*
    Part3.对运维上传的日志信息进行处理，分别上传至对应的HDFS路径上
     */
    //1.提取用户行为日志信息
    val tableNameAndInfos: RDD[(String, String)] = clientLogInfos.map(line => line.split("&"))
      .filter(item => item.length == 6)
      .map(line => (line(2), line(3)))

    //2.获取当天用户行为的所有请求类型
    val allTableName: Array[String] = tableNameAndInfos.keys.distinct().collect()

    //3.将各种请求数据以表名为路径名存储在HDFS上
    tableNameAndInfos.map(tp => {
      val tableName: String = tp._1
      val tableInfos: String = tp._2
      if ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(tableName)) {
        val jsonObject: JSONObject = JSON.parseObject(tableInfos)
        val songid: String = jsonObject.getString("songid")
        val mid: String = jsonObject.getString("mid")
        val optrateType: String = jsonObject.getString("optrate_type")
        val uid: String = jsonObject.getString("uid")
        val consumeType: String = jsonObject.getString("consume_type")
        val durTime: String = jsonObject.getString("dur_time")
        val sessionId: String = jsonObject.getString("session_id")
        val songName: String = jsonObject.getString("songname")
        val pkgId: String = jsonObject.getString("pkg_id")
        val orderId: String = jsonObject.getString("order_id")
        (tableName, songid + "\t" + mid + "\t" + optrateType + "\t" + uid + "\t" + consumeType + "\t" + durTime + "\t" + sessionId + "\t" + songName + "\t" + pkgId + "\t" + orderId)
      }else{
        //其他类型的请求数据直接以json格式存储在目录中
        tp
      }
    }).saveAsHadoopFile(
      s"${hdfsclientlogpath}/all_client_tables/${logDate}",
      // 由于saveASHadoopFile算子属于PairRDDFunction底下的，传入的参数应该是pairRDD也就是有键值对的RDD，
      // 因为上面的map操作已经将原来的RDD转化为了有键值对的RDD，所以这需要传入key和value的所属类型
      classOf[String],  //key的类型
      classOf[String],  //value的类型
      classOf[PairRDDMultipleTextOutputFormat]
    )

    /*
    Part4.在Hive中建立TO_CLIENT_SONG_OPERATE_REQ_D表，并将数据导入到表中
     */
    //创建ODS层的TO_CLIENT_SONG_OPERATE_REQ_D表
    sparkSession.sql(s"use $hiveDataBase")
    sparkSession.sql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS `TO_CLIENT_SONG_PLAY_OPERATE_REQ_D`(
        | `SONGID` string,  --歌曲ID
        | `MID` string,     --机器ID
        | `OPTRATE_TYPE` string,  --操作类型
        | `UID` string,     --用户ID
        | `CONSUME_TYPE` string,  --消费类型
        | `DUR_TIME` string,      --时长
        | `SESSION_ID` string,    --sessionID
        | `SONGNAME` string,      --歌曲名称
        | `PKG_ID` string,        --套餐ID
        | `ORDER_ID` string       --订单ID
        |)
        |partitioned by (data_dt string)
        |ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
        |LOCATION 'hdfs://hadoop102/user/hive/warehouse/data/song/TO_CLIENT_SONG_PLAY_OPERATE_REQ_D'
      """.stripMargin)

    //将数据导入到表中
    sparkSession.sql(
      s"""
        |load data inpath
        |'${hdfsclientlogpath}/all_client_tables/${logDate}/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ'
        |into table TO_CLIENT_SONG_PLAY_OPERATE_REQ_D partition (data_dt='${logDate}')
        |""".stripMargin
    )

    println("*****All Finished*****")




  }

}
