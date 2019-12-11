package com.cmdi.data_analysis.submit

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.cmdi.data_analysis.step.MroAnalysisStep
import com.cmdi.data_analysis.udf.MySparkUdf
import com.cmdi.data_analysis.util.{HdfsUtilAnalysisData, PgMergeLogicTb, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/***
  * mro业务逻辑解析程序
  */

object MroDataAnalysis {

 val sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  //hdfs上存放city_date_hour的文件夹
  var comp_merge:String = _

  def main(args: Array[String]): Unit = {
    executeDataAnalysis(args)
  }

  /**
    * 业务逻辑执行主要的方法，
    * @param args arg(0)为mrodataanalysis-conf.properties文件路径
    * @param hadoopConfMap 测试所用hadoopCnf
    */
  def executeDataAnalysis(args: Array[String],hadoopConfMap:Map[String,String]=null): Unit = {
    println(sm.format(new Date())+"INFO MroDataAnalysis:开始执行mro业务逻辑程序")
    val argsLen = args.length
    var confPath = ".\\conf\\mrodataanalysis-conf.properties"
    if(argsLen<1){
      System.err.println("使用方法：com.cmdi.data_analysis.submit <配置文件路径>")
      System.exit(1)
    }else{
      confPath = args(0)
    }

    //加载配置文件
    val confPro = PropertiesUtil.getDiskProperties(confPath)
    val log_level = confPro.getProperty("log_level")
    comp_merge = confPro.getProperty("comp_merge")
    val pgdb_ip = confPro.getProperty("pgdb_ip")
    val pgdb_port = confPro.getProperty("pgdb_port").toInt
    val pgdb_user = confPro.getProperty("pgdb_user")
    val pgdb_passwd = confPro.getProperty("pgdb_passwd")
    val pgdb_db = confPro.getProperty("pgdb_db")


    //设置日志级别
    Logger.getLogger("org").setLevel(Level.toLevel(log_level,Level.INFO))
    Logger.getLogger("hive").setLevel(Level.toLevel(log_level,Level.INFO))
    val hadoopConf = new Configuration()
    if (hadoopConfMap != null) {
      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      hadoopConfMap.foreach(item => {
        if (item._1.startsWith("spark.hadoop.")) {
          hadoopConf.set(item._1.substring(13), item._2)
        }
      })
    }
    //初始化pg数据库
    PgMergeLogicTb.init(pgdb_ip, pgdb_port, pgdb_user, pgdb_passwd, pgdb_db)

    val sparkConf = new SparkConf().setAppName("MroDataAnalysis")
    sparkConf.set("spark.debug.maxToStringFields","50")
    if (hadoopConfMap != null) {
      // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      // 测试用
      hadoopConfMap.foreach(item=>{
        sparkConf.set(item._1, item._2)
      })
      // 本地测试用，设置 Master，默认为 local[2]
      sparkConf.setMaster(hadoopConfMap.getOrElse("spark_master", "local[4]"))
    }

    //设置HdfsUtilAnalysisData文件系统
    HdfsUtilAnalysisData.setFileSystem(hadoopConf)
    //获取city_date_hour文件
    val city_date_hour_pro_arr = HdfsUtilAnalysisData.getLinkFileNames(comp_merge)
    if(city_date_hour_pro_arr == null) {
      println(sm.format(new Date())+s"INFO 没有需要执行的数据，$comp_merge 文件夹无文件")
      System.exit(0)
    }
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val calDistanceUdf = MySparkUdf.calDistanceUdf

    spark.udf.register("caldistanceudf",calDistanceUdf)
    city_date_hour_pro_arr.filter(city_date_hour_pro=>{
      val linkFilePro = city_date_hour_pro._2
      val if_to_analyze_mro = linkFilePro.getProperty("if_to_analyze_mro","false").toBoolean
      if(!if_to_analyze_mro) {
        val city_date_hour = city_date_hour_pro._1
        HdfsUtilAnalysisData.getFileSystem().deleteOnExit(new Path(String.join("/",comp_merge,city_date_hour)))
      }
      if_to_analyze_mro
    }).foreach(city_date_hour_pro=>{
       executeMroDataLogic(spark,city_date_hour_pro)
    })
    spark.stop()
    PgMergeLogicTb.end()
  }

  def executeMroDataLogic(spark:SparkSession,city_date_hour_pro:(String,Properties)):Unit={
    val city_date_hour = city_date_hour_pro._1
    val linkFilePro = city_date_hour_pro._2
    val taskid = linkFilePro.getProperty("taskid","2").toInt

    val city_date_hour_arr = city_date_hour.split("_")
    val city = city_date_hour_arr(0)
    val logdate = city_date_hour_arr(1)
    val hour = city_date_hour_arr(2)

    println(sm.format(new Date())+s"-----------------开始执行小时粒度$city $logdate $hour 业务逻辑程序----------------------------------------")
    PgMergeLogicTb.insert(taskid,city,logdate,hour,"callogic_hour","start")
    PgMergeLogicTb.insert(taskid,city,logdate,hour,"base_step1","start")
    val mroAnalysisStep = new MroAnalysisStep(city,logdate,hour)
    val arrStep1 = mroAnalysisStep.getBaseStep1()
    arrStep1.foreach(sqlStr=>{
      spark.sql(sqlStr)
    })
    PgMergeLogicTb.insert(taskid,city,logdate,hour,"base_step1","stop")

    PgMergeLogicTb.insert(taskid,city,logdate,hour,"base_step2","start")
    val arrStep2 = mroAnalysisStep.getBaseStep2()
    arrStep2.foreach(sqlStr=>{
      spark.sql(sqlStr)
    })
    PgMergeLogicTb.insert(taskid,city,logdate,hour,"base_step2","stop")

    PgMergeLogicTb.insert(taskid,city,logdate,hour,"base_step3","start")
    val arrStep3 = mroAnalysisStep.getBaseStep3()
    arrStep3.foreach(sqlStr=>{
      spark.sql(sqlStr)
    })
    PgMergeLogicTb.insert(taskid,city,logdate,hour,"base_step3","stop")

    PgMergeLogicTb.insert(taskid,city,logdate,hour,"nas_pmandmrotable","start")
    val nas_pmandmrotable = mroAnalysisStep.get_nas_pmandmrotable()
    nas_pmandmrotable.foreach(sqlStr=>{
      spark.sql(sqlStr)
    })
    PgMergeLogicTb.insert(taskid,city,logdate,hour,"nas_pmandmrotable","stop")

    PgMergeLogicTb.insert(taskid,city,logdate,hour,"nas_scmatrixtable","start")
    val nas_scmatrixtable = mroAnalysisStep.get_nas_scmatrixtable()
    nas_scmatrixtable.foreach(sqlStr=>{
      spark.sql(sqlStr)
    })

    HdfsUtilAnalysisData.getFileSystem().deleteOnExit(new Path(String.join("/",comp_merge,city_date_hour)))
    println(sm.format(new Date())+s"-----------------结束执行小时粒度$city $logdate $hour 业务逻辑程序----------------------------------------")

    PgMergeLogicTb.insert(taskid,city,logdate,hour,"nas_scmatrixtable","stop")
    PgMergeLogicTb.insert(taskid,city,logdate,hour,"callogic_hour","stop")

    val stopHourCount = PgMergeLogicTb.selectStopHourCount(city,logdate)
    if(stopHourCount == 24){
      println(sm.format(new Date())+s"-----------------开始执行天粒度$city $logdate 业务逻辑程序----------------------------------------")
      PgMergeLogicTb.insert(taskid,city,logdate,"day","nas_scmatrixtable_day","start")
      val nas_scmatrixtable_day = mroAnalysisStep.get_nas_scmatrixtable_day()
      nas_scmatrixtable_day.foreach(sqlStr=>{
        spark.sql(sqlStr)
      })

      PgMergeLogicTb.insert(taskid,city,logdate,"day","nas_scmatrixtable_day","stop")

      PgMergeLogicTb.insert(taskid,city,logdate,"day","nas_pmandmrotable_day","start")
      val nas_pmandmrotable_day = mroAnalysisStep.get_nas_pmandmrotable_day()
      nas_pmandmrotable_day.foreach(sqlStr=>{
        spark.sql(sqlStr)
      })
      PgMergeLogicTb.insert(taskid,city,logdate,"day","nas_pmandmrotable_day","stop")
      println(sm.format(new Date())+s"-----------------结束执行天粒度$city $logdate 业务逻辑程序----------------------------------------")

    }


  }
}
