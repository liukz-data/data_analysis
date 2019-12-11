package com.cmdi.data_analysis.util

import java.io.{FileNotFoundException, IOException}
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


/**
  * hdfs的FileSystem的一个工具类
  */
object HdfsUtilAnalysisData {

  var fs:FileSystem = _
  var hadoopConf:Configuration = _


  /**
    * 设置FileSystem对象实例
    * @param hadoopConf hadoop配置文件对象
    */
  def setFileSystem(hadoopConf:Configuration):Unit={
    HdfsUtilAnalysisData.hadoopConf = hadoopConf
    fs = FileSystem.get(hadoopConf)
  }
  def getFileSystem():FileSystem = {
    fs
  }

  /**
    * 此方法用来获得city_date_hour文件名以及读取其内部数据到properties对象里面
    * @param hdfsLinkPath hdfs上city_date_hour的父路径
    * @return Array[(String,Properties)]
    */
   def getLinkFileNames(hdfsLinkPath: String): Array[(String,Properties)] = {

      val filesStatus = fs.listStatus(new Path(hdfsLinkPath))
      val len = filesStatus.length

      if (len == 0) {
        null
      } else {
        var arrCompSeqFile = new Array[(String,Properties)](len)
        for (i <- 0 until len) {
          val filePath = filesStatus(i).getPath
          val fileName = filePath.getName
          val in = fs.open(filePath,1024)
          val linkFilePro = new Properties()
          linkFilePro.load(in)
          arrCompSeqFile(i) =(fileName,linkFilePro)
        }
        arrCompSeqFile.sortBy(_._1)
      }

  }
}
