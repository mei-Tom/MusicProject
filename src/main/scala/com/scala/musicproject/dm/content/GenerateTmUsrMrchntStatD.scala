package com.scala.musicproject.dm.content

import com.scala.musicproject.common.ConfigUtils

import java.util.Properties
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *  根据 EDS 层 TW_MAC_STAT_D 机器营收日统计表 统计得到 TM_USR_MRCHNT_STAT_D 商户营收日统计表
  */
object GenerateTmUsrMrchntStatD {
  val localRun : Boolean = ConfigUtils.LOCAL_RUN
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDataBase = ConfigUtils.HIVE_DATABASE
  var sparkSession : SparkSession = _

  private val mysqlUrl = ConfigUtils.MYSQL_URL
  private val mysqlUser = ConfigUtils.MYSQL_USER
  private val mysqlPassword = ConfigUtils.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {
    if(localRun){//本地运行
      sparkSession = SparkSession.builder().master("local")
        .config("hive.metastore.uris",hiveMetaStoreUris)
        .config("spark.sql.shuffle.partitions",10)
        .enableHiveSupport().getOrCreate()
    }else{//集群运行
      sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions",10).enableHiveSupport().getOrCreate()
    }

    if(args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20201231)")
      System.exit(1)
    }
    val analyticDate = args(0)

    sparkSession.sparkContext.setLogLevel("Error")
    sparkSession.sql(s"use $hiveDataBase ")

    /**
      * 查询 EDS层 TW_MAC_STAT_D 机器营收日统计表 与 用户基本信息表进行关联 查询对应的用户营收情况。
      * 注意：这里不再关联 用户基本信息表
      */
    sparkSession.sql(
      s"""
        | select
        |   AGE_ID AS ADMIN_ID,   --代理人
        |   PAY_TYPE,
        |   SUM(REV_ORDR_CNT) AS REV_ORDR_CNT,  --总营收订单数
        |   SUM(REF_ORDR_CNT) AS REF_ORDR_CNT,  --总退款订单数
        |   CAST(SUM(TOT_REV) AS Double) AS TOT_REV,  --总营收
        |   CAST(SUM(TOT_REF) AS Double) AS TOT_REF,  --总退款
        |   CAST(SUM(TOT_REV * NVL(INV_RATE,0)) AS DECIMAL(10,4)) AS TOT_INV_REV,  --投资人营收
        |   CAST(SUM(TOT_REV * NVL(AGE_RATE,0)) AS DECIMAL(10,4)) AS TOT_AGE_REV,  --代理人营收
        |   CAST(SUM(TOT_REV * NVL(COM_RATE,0)) AS DECIMAL(10,4)) AS TOT_COM_REV,  --公司营收
        |   CAST(SUM(TOT_REV * NVL(PAR_RATE,0)) AS DECIMAL(10,4)) AS TOT_PAR_REV    --合伙人营收
        | from TW_MAC_STAT_D
        | WHERE DATA_DT = ${analyticDate}
        | GROUP BY AGE_ID,PAY_TYPE
      """.stripMargin).createTempView("TEMP_USR_MRCHNT_STAT")

    //将以上结果保存到分区表 TM_USR_MRCHNT_STAT_D  商户日营收统计表 中。
    sparkSession.sql(
      s"""
        | insert overwrite table TM_USR_MRCHNT_STAT_D partition (data_dt=${analyticDate}) select * from TEMP_USR_MRCHNT_STAT
      """.stripMargin)

    /**
      * 同时将以上结果保存至 mysql songresult 库中的 machine_infos 中,作为DM层结果
      */
    val properties  = new Properties()
    properties.setProperty("user",mysqlUser)
    properties.setProperty("password",mysqlPassword)
    properties.setProperty("driver","com.mysql.cj.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${analyticDate} as data_dt ,* from TEMP_USR_MRCHNT_STAT
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"tm_usr_mrchnt_stat_d",properties)

    println("**** all finished ****")

  }
}
