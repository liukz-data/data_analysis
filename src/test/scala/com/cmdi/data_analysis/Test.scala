package com.cmdi.data_analysis

import com.cmdi.data_analysis.step.MroAnalysisStep
import com.cmdi.data_analysis.util.PgMergeLogicTb

object Test {

  def main(args: Array[String]): Unit = {
    PgMergeLogicTb.init("10.254.222.226", 5432, "zhangyan", "#Mrdas17P19!", "mlogdb")
   // PgMergeLogicTb.insert(1,"test","test","test","test","test")
    println(PgMergeLogicTb.selectStopHourCount("shanghai","20181211"))
    //PgMergeLogicTb.end()
  }
}
