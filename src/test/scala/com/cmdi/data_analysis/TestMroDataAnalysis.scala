package com.cmdi.data_analysis

import java.io.{File, FileReader}
import java.util.Properties

import com.cmdi.data_analysis.submit.MroDataAnalysis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import scala.collection.mutable
import scala.xml.XML

object TestMroDataAnalysis {


    private val prop: Properties = new Properties()

    def main(args: Array[String]): Unit = {
      //    Logger.getLogger("org").setLevel(Level.ERROR)
      // 1. 获取自定义的配置信息
      prop.load(new FileReader(".\\conf\\mrodataanalysis-conf.properties"))

      // 2. 获取 Hadoop配置
      val hadoopConfMap = getHadoopConf().toMap

      if (prop.getProperty("hadoop_security_authentication", "simple")
        .toLowerCase == "kerberos") {
        System.setProperty("java.security.krb5.conf",
          prop.getProperty("krb5_conf", null))
        //System.setProperty("javax.security.auth.useSubjectCredsOnly","true")
        System.setProperty("sun.security.krb5.debug","true")
        accessHadoop()
      }



      var hadoopConf: Configuration  = new Configuration()
      if (hadoopConfMap != null) {
        // 获取自定义的 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
        // 测试用
        hadoopConfMap.foreach(item => {
          if (item._1.startsWith("spark.hadoop.")) {
            hadoopConf.set(item._1.substring(13), item._2)
          }
        })
      }

      val str1=new Array[String](1)
       str1(0)=".\\conf\\mrodataanalysis-conf.properties"
      // 3. 调用测试主程序
    // TestSettingE.executeDataAnalysis(str1,hadoopConfMap)
      MroDataAnalysis.executeDataAnalysis(str1,hadoopConfMap)
      /*  MergeSeqFilesCoalesce.executeMerge(
         // Array("/zhangyan/mro/property/mromerge-conf.properties"),
          ///test/test_lkz_other/prop/lkz-test-mromerge-conf.properties
          //Array("/test/test_lkz_other/prop/lkz-test-mromerge-conf-v2.properties"),
          Array("/test/test_lkz_other/prop/lkz-test-mromerge-conf-v4.properties"),
          hadoopConfMap)*/
      /* MergeSeqFilesRepartitionOneHour.executeMerge(Array("/test/test_lkz_other/prop/lkz-test-mromerge-conf-v5.properties","error","shanghai","20181211","00"),
         hadoopConfMap )*/
      // MergeSeqFilesCoalesceSerialLine.executeMerge(Array("/test/test_lkz_other/prop/lkz-test-mromerge-conf-v5.properties"),hadoopConfMap)
      /*MergeSeqFilesCoalesceOneHour.executeMerge(Array("/test/test_lkz_other/prop/lkz-test-mromerge-conf-v5.properties","error","shanghai","20181211","00"),
        hadoopConfMap )*/
    }

    /**
      * 使合并数据的程序可以访问 启用了Kerberos的 CDH Hadoop集群
      */
    private def accessHadoop(): Unit ={
      val hive_user = prop.getProperty("hive_user")
      val hive_keytab = prop.getProperty("hive_keytab")
      val hadoopConf = new Configuration()
      hadoopConf.addResource(
        new File(".\\conf\\core-site.xml")
          .toURI.toURL)
      hadoopConf.addResource(
        new File(".\\conf\\hdfs-site.xml")
          .toURI.toURL)

      /*    if (prop.getProperty("hadoop_security_authentication", "simple")
            .toLowerCase == "kerberos") {
            System.setProperty("java.security.krb5.conf",
              prop.getProperty("krb5_conf", null))
          }*/
      try {

        UserGroupInformation.setConfiguration(hadoopConf)
       UserGroupInformation.loginUserFromKeytab(hive_user, hive_keytab)
        //val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(hive_user, hive_keytab)
        //UserGroupInformation.reset()
      } catch {
        case exception: Exception =>
          exception.printStackTrace()
          System.exit(1)
      }
    }

    /**
      * 将Hadoop *-site.xml解析为HashMap返回
      * 本地测试用，提交到CDH集群后就不用在代码中进行加载了
      * @return
      */
    private def getHadoopConf(): mutable.Map[String, String] = {
      // 获取 hive-site.xml core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 的内容
      val hiveConfMap = parseXMLToMap(
        ".\\conf\\hive-site.xml")
      var hadoopConfMap = mutable.HashMap.empty[String, String]
      hadoopConfMap ++= parseXMLToMap(
        ".\\conf\\core-site.xml")
      hadoopConfMap ++= parseXMLToMap(
        ".\\conf\\hdfs-site.xml")
      hadoopConfMap ++= parseXMLToMap(
        ".\\conf\\mapred-site.xml")
      hadoopConfMap ++= parseXMLToMap(
        ".\\conf\\yarn-site.xml")
      //    hadoopConfMap.foreach(x=>println(x._1 + " -> " + x._2))
      hadoopConfMap ++= hiveConfMap
      hadoopConfMap
    }

    /**
      * 将Hadoop *-site.xml的配置内容解析为HashMap
      * 本地测试用，提交到CDH集群后就不用在代码中进行加载了
      * @param xmlFilePath
      * @return
      */
    private def parseXMLToMap(xmlFilePath: String): Map[String, String] = {
      val confMap = new mutable.HashMap[String, String]
      val someXml = XML.loadFile(xmlFilePath)

      // 对于 core-site.xml、hdfs-site.xml、mapred-site.xml、yarn-site.xml
      // 需要在配置项的名字前加上 "spark.hadoop."
      val filename = xmlFilePath.substring(xmlFilePath.lastIndexOf(File.separator)+1)
      if (filename=="core-site.xml" || filename=="hdfs-site.xml"
        || filename=="mapred-site.xml" || filename=="yarn-site.xml" ) {
        (someXml \\ "property").foreach(item=>{
          confMap.put( "spark.hadoop." + (item \ "name").text, (item\"value").text )
        })
      } else {
        (someXml \\ "property").foreach(item=>{
          confMap.put( (item \ "name").text, (item\"value").text )
        })
      }

      confMap.toMap
    }
  }

