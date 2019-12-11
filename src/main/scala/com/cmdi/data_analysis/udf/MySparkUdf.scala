package com.cmdi.data_analysis.udf

import java.lang.Math._

/**
  * 一个帮助类，这里面记载mro业务逻辑所需的spark udf函数
  */
object MySparkUdf {


  /**
    * 计算距离的函数，参数为主小区经纬度和服务小区经纬度
    */
  val calDistanceUdf=(sclng:Double,sclat:Double,nclng:Double,nclat:Double)=>{

    val PI: Double = 3.14159265358979323
    val TWO_P_DIS_LIMIT: Double = 0.1
    val RadFactor: Double =  PI / 180
    //    val RevRadFactor = 180/PI
    val eR: Double = 6378.245
    val BiasE = abs(sclng - nclng)
    val BiasN = abs(sclat - nclat)
    var distance = 0.0
    //当经纬度都小于门限值时，采用两点间直线距离
    if ((BiasE < TWO_P_DIS_LIMIT) && (BiasN < TWO_P_DIS_LIMIT)) {
      val t2 = sqrt(BiasN * BiasN + BiasE * BiasE) * RadFactor
      distance = t2 * eR
    }
    else { //当经纬度都大于门限值时，采用大圆距离
      val a0 = (90 - nclat) * RadFactor
      val b0 = (90 - sclat) * RadFactor
      val C0 = abs(sclng - nclng) * RadFactor
      val t1 = cos(a0) * cos(b0) + sin(a0) * sin(b0) * cos(C0)
      val t2 = acos(t1)
      distance = t2 * eR
    }
    distance
  }
}
