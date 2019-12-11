package com.cmdi.data_analysis.step

import scala.collection.mutable.ArrayBuffer


class MroAnalysisStep(city:String= "shanghai",logdate:String= "20181211",hour:String= "00") {


  //为后面计算系数和邻区做准备
  def getBaseStep1():Array[String]={
    val drop_base_step1_001 = "drop table if exists mro.temp_mro_base_step1_001_convert_to_true_value"
    val create_base_step1_001 = s"""create table mro.temp_mro_base_step1_001_convert_to_true_value stored as orc tblproperties("orc.compress"="SNAPPY") as select eci,cgi,if(ltescearfcn=-111,null,ltescearfcn) ltescearfcn,if(ltescpci=-111,null,ltescpci) ltescpci,           if(ltescrsrp=-111,null,ltescrsrp-141) ltescrsrp,           if(ltencearfcn01=-111,null,ltencearfcn01) ltencearfcn01,           if(ltencpci01=-111,null,ltencpci01) ltencpci01,           if(ltencrsrp01=-111,null,ltencrsrp01-141) ltencrsrp01,           if(ltencearfcn02=-111,null,ltencearfcn02) ltencearfcn02,           if(ltencpci02=-111,null,ltencpci02) ltencpci02,           if(ltencrsrp02=-111,null,ltencrsrp02-141) ltencrsrp02,           if(ltencearfcn03=-111,null,ltencearfcn03) ltencearfcn03,           if(ltencpci03=-111,null,ltencpci03) ltencpci03,           if(ltencrsrp03=-111,null,ltencrsrp03-141) ltencrsrp03,           if(ltencearfcn04=-111,null,ltencearfcn04) ltencearfcn04,           if(ltencpci04=-111,null,ltencpci04) ltencpci04,           if(ltencrsrp04=-111,null,ltencrsrp04-141) ltencrsrp04,           if(ltencearfcn05=-111,null,ltencearfcn05) ltencearfcn05,           if(ltencpci05=-111,null,ltencpci05) ltencpci05,           if(ltencrsrp05=-111,null,ltencrsrp05-141) ltencrsrp05,           if(ltencearfcn06=-111,null,ltencearfcn06) ltencearfcn06,           if(ltencpci06=-111,null,ltencpci06) ltencpci06,           if(ltencrsrp06=-111,null,ltencrsrp06-141) ltencrsrp06,           if(ltencearfcn07=-111,null,ltencearfcn07) ltencearfcn07,           if(ltencpci07=-111,null,ltencpci07) ltencpci07,           if(ltencrsrp07=-111,null,ltencrsrp07-141) ltencrsrp07,           if(ltencearfcn08=-111,null,ltencearfcn08) ltencearfcn08,           if(ltencpci08=-111,null,ltencpci08) ltencpci08,           if(ltencrsrp08=-111,null,ltencrsrp08-141) ltencrsrp08,           city,           logdate,           hour        from mro.mro_dt_rec        where city='$city' and logdate='$logdate' and hour='$hour'"""
    val drop_base_step1_002 = "drop table if exists mro.temp_mro_base_step1_002_convert_to_true_value"
    val create_base_step1_002 = """create table mro.temp_mro_base_step1_002_convert_to_true_value stored as orc tblproperties("orc.compress"="SNAPPY") as select     city,     logdate,     hour,     eci,     cgi,     ltescrsrp,     ltescearfcn,     (overcovercounts_before_01+overcovercounts_before_02+overcovercounts_before_03+overcovercounts_before_04+overcovercounts_before_05+overcovercounts_before_06+overcovercounts_before_07+overcovercounts_before_08) overcovercounts_before,     (freovercovercounts_before_01+freovercovercounts_before_02+freovercovercounts_before_03+freovercovercounts_before_04+freovercovercounts_before_05+freovercovercounts_before_06+freovercovercounts_before_07) freovercovercounts_before from (     select         city,         logdate,         hour,         eci,         cgi,         ltescrsrp,         ltescearfcn,         if(-6<=ltencrsrp01-ltescrsrp and -110<=ltescrsrp,1,0) overcovercounts_before_01,         if(-6<=ltencrsrp01-ltescrsrp and -110<=ltescrsrp,if(abs(ltencearfcn01-ltescearfcn)<=100,1,0),0) freovercovercounts_before_01,         if(-6<=ltencrsrp02-ltescrsrp and -110<=ltescrsrp,1,0) overcovercounts_before_02,         if(-6<=ltencrsrp02-ltescrsrp and -110<=ltescrsrp,if(abs(ltencearfcn02-ltescearfcn)<=100,1,0),0) freovercovercounts_before_02,         if(-6<=ltencrsrp03-ltescrsrp and -110<=ltescrsrp,1,0) overcovercounts_before_03,         if(-6<=ltencrsrp03-ltescrsrp and -110<=ltescrsrp,if(abs(ltencearfcn03-ltescearfcn)<=100,1,0),0) freovercovercounts_before_03,         if(-6<=ltencrsrp04-ltescrsrp and -110<=ltescrsrp,1,0) overcovercounts_before_04,         if(-6<=ltencrsrp04-ltescrsrp and -110<=ltescrsrp,if(abs(ltencearfcn04-ltescearfcn)<=100,1,0),0) freovercovercounts_before_04,         if(-6<=ltencrsrp05-ltescrsrp and -110<=ltescrsrp,1,0) overcovercounts_before_05,         if(-6<=ltencrsrp05-ltescrsrp and -110<=ltescrsrp,if(abs(ltencearfcn05-ltescearfcn)<=100,1,0),0) freovercovercounts_before_05,         if(-6<=ltencrsrp06-ltescrsrp and -110<=ltescrsrp,1,0) overcovercounts_before_06,         if(-6<=ltencrsrp06-ltescrsrp and -110<=ltescrsrp,if(abs(ltencearfcn06-ltescearfcn)<=100,1,0),0) freovercovercounts_before_06,         if(-6<=ltencrsrp07-ltescrsrp and -110<=ltescrsrp,1,0) overcovercounts_before_07,         if(-6<=ltencrsrp07-ltescrsrp and -110<=ltescrsrp,if(abs(ltencearfcn07-ltescearfcn)<=100,1,0),0) freovercovercounts_before_07,         if(-6<=ltencrsrp08-ltescrsrp and -110<=ltescrsrp,1,0) overcovercounts_before_08,         if(-6<=ltencrsrp08-ltescrsrp and -110<=ltescrsrp,if(abs(ltencearfcn08-ltescearfcn)<=100,1,0),0) freovercovercounts_before_08     from mro.temp_mro_base_step1_001_convert_to_true_value ) a"""

    val drop_base_step1_003 = "drop table if exists mro.temp_mro_base_step1_003_convert_to_column"
   val create_base_step1_003="""create table mro.temp_mro_base_step1_003_convert_to_column stored as orc tblproperties("orc.compress"="SNAPPY") as select    eci,    ltescrsrp ltescrsrp,    ltencearfcn01 ltencearfcn,    ltencpci01 ltencpci,    ltencrsrp01 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value where -6<ltencrsrp01-ltescrsrp union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn02 ltencearfcn,    ltencpci02 ltencpci,    ltencrsrp02 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value where -6<ltencrsrp02-ltescrsrp union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn03 ltencearfcn,    ltencpci03 ltencpci,    ltencrsrp03 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value where -6<ltencrsrp03-ltescrsrp union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn04 ltencearfcn,    ltencpci04 ltencpci,    ltencrsrp04 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value where -6<ltencrsrp04-ltescrsrp union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn05 ltencearfcn,    ltencpci05 ltencpci,    ltencrsrp05 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value where -6<ltencrsrp05-ltescrsrp union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn06 ltencearfcn,    ltencpci06 ltencpci,    ltencrsrp06 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value where -6<ltencrsrp06-ltescrsrp union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn07 ltencearfcn,    ltencpci07 ltencpci,    ltencrsrp07 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value where -6<ltencrsrp07-ltescrsrp union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn08 ltencearfcn,    ltencpci08 ltencpci,    ltencrsrp08 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value where -6<ltencrsrp08-ltescrsrp"""
    // val create_base_step1_003="""create table mro.temp_mro_base_step1_003_convert_to_column stored as orc tblproperties("orc.compress"="SNAPPY") as select    eci,    ltescrsrp ltescrsrp,    ltencearfcn01 ltencearfcn,    ltencpci01 ltencpci,    ltencrsrp01 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value  union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn02 ltencearfcn,    ltencpci02 ltencpci,    ltencrsrp02 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value  union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn03 ltencearfcn,    ltencpci03 ltencpci,    ltencrsrp03 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value  union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn04 ltencearfcn,    ltencpci04 ltencpci,    ltencrsrp04 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value  union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn05 ltencearfcn,    ltencpci05 ltencpci,    ltencrsrp05 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value  union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn06 ltencearfcn,    ltencpci06 ltencpci,    ltencrsrp06 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value  union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn07 ltencearfcn,    ltencpci07 ltencpci,    ltencrsrp07 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value  union all select    eci,    ltescrsrp ltescrsrp,    ltencearfcn08 ltencearfcn,    ltencpci08 ltencpci,    ltencrsrp08 ltencrsrp,    city,    logdate,    hour from mro.temp_mro_base_step1_001_convert_to_true_value """
    val sqlArr = new Array[String](6)
    sqlArr(0) = drop_base_step1_001
    sqlArr(1) = create_base_step1_001
    sqlArr(2) = drop_base_step1_002
    sqlArr(3) = create_base_step1_002
    sqlArr(4) = drop_base_step1_003
    sqlArr(5) = create_base_step1_003
    sqlArr
  }

  //计算相关系数
  def getBaseStep2():Array[String]={
    val drop_base_step2_004 = "drop table if exists mro.temp_mro_base_step2_004_cal_denominator"
    val create_base_step2_004 = """create table mro.temp_mro_base_step2_004_cal_denominator as select city,logdate,hour,eci,cgi,sum(if(-110<=ltescrsrp,1,0)) interobjcounts from mro.temp_mro_base_step1_001_convert_to_true_value group by city,logdate,hour,eci,cgi """
    val drop_base_step2_005 = "drop table if exists mro.temp_mro_base_step2_005_cal_numerator"
    val create_base_step2_005 = "create table mro.temp_mro_base_step2_005_cal_numerator as select city,logdate,hour,eci,ltencearfcn,ltencpci,sum(if(-6<=ltencrsrp-ltescrsrp and -110<=ltescrsrp,1,0)) cellsbyovercoverage_before from mro.temp_mro_base_step1_003_convert_to_column group by city,logdate,hour,eci,ltencearfcn,ltencpci"
    val insert_base_step2_006 = "insert into table mro.temp_mro_base_step2_006_cal_coefficient select a.city,a.logdate,a.hour,a.eci,b.cgi,a.ltencearfcn,a.ltencpci,a.cellsbyovercoverage_before,b.interobjcounts,if(b.interobjcounts!=0,cellsbyovercoverage_before/b.interobjcounts,0) coefficient_association from mro.temp_mro_base_step2_005_cal_numerator a left join mro.temp_mro_base_step2_004_cal_denominator b on a.eci=b.eci"
    val sqlArr = new Array[String](5)
    sqlArr(0) = drop_base_step2_004
    sqlArr(1) = create_base_step2_004
    sqlArr(2) = drop_base_step2_005
    sqlArr(3) = create_base_step2_005
    sqlArr(4) = insert_base_step2_006
    sqlArr
  }

  //计算邻区
  def getBaseStep3():Array[String]={
    val drop_base_step3_007 = "drop table if exists mro.temp_mro_base_step3_007_distinct_union"
    val create_base_step3_007 = s"create table mro.temp_mro_base_step3_007_distinct_union as select      city,     eci,     ltencearfcn,     ltencpci  from mro.temp_mro_base_step2_005_cal_numerator a  where city='$city' and logdate='$logdate' and hour='$hour' and ltencearfcn is not null and not exists(select 1 from mro.mro_cal_nearest_neighborhood b  where a.city=b.city and a.eci=b.sc_eci and a.ltencearfcn=b.ltencearfcn and a.ltencpci=b.ltencpci)"
    val drop_base_step3_008 = "drop table if exists mro.temp_mro_base_step3_008_cal_distance"
    val create_base_step3_008 = """create table mro.temp_mro_base_step3_008_cal_distance stored as orc tblproperties("orc.compress"="SNAPPY") as select b.city,a.eci sc_eci,b.ltencearfcn ltencearfcn,b.ltencpci ltencpci,a.longitude sc_longitude,a.latitude sc_latitude,c.eci nc_eci,c.cgi nc_cgi,c.longitude nc_longitude,c.latitude nc_latitude,caldistanceudf(a.longitude,a.latitude,c.longitude,c.latitude) distance from mro.v_eptable  a inner join mro.temp_mro_base_step3_007_distinct_union b on a.eci=b.eci left join mro.v_eptable c on b.ltencearfcn=c.earfcn and b.ltencpci=c.pci where a.longitude is not null and c.longitude is not null and a.eci!=c.eci"""
    val drop_base_step3_009 = "drop table if exists mro.temp_mro_base_step3_009_cal_distance_parititon_rank"
    val create_base_step3_009 ="""create table mro.temp_mro_base_step3_009_cal_distance_parititon_rank stored as orc tblproperties("orc.compress"="SNAPPY") as select     city,    sc_eci,    ltencearfcn,    ltencpci,    nc_eci,    nc_cgi,    distance,    row_number() over(partition by sc_eci,ltencearfcn,ltencpci order by distance asc) partition_rank from mro.temp_mro_base_step3_008_cal_distance where distance<=10"""
    val insert_mro_cal_nearest_neighborhood = "insert into table mro.mro_cal_nearest_neighborhood select     city,    sc_eci,    ltencearfcn,    ltencpci,    nc_eci,    nc_cgi,    distance  from mro.temp_mro_base_step3_009_cal_distance_parititon_rank  where partition_rank=1"
    val sqlArr = new Array[String](7)
    sqlArr(0) = drop_base_step3_007
    sqlArr(1) = create_base_step3_007
    sqlArr(2) = drop_base_step3_008
    sqlArr(3) = create_base_step3_008
    sqlArr(4) = drop_base_step3_009
    sqlArr(5) = create_base_step3_009
    sqlArr(6) = insert_mro_cal_nearest_neighborhood
    sqlArr
  }

  //报表
  def get_nas_pmandmrotable():Array[String]={

    val insert_base_step4_010 = "insert into table mro.temp_mro_base_step4_010_count_sampling_points select      city,     logdate,     hour,     a.eci,     a.cgi,     sum(1) rightobj,     a.ltescearfcn,     sum(if(-110<=a.ltescrsrp,1,0)) interobjcounts,     sum(if(a.overcovercounts_before>=3,1,0)) overcovercounts,     sum(if(a.freovercovercounts_before>=3,1,0)) freovercovercounts,     sum(if(-140<a.ltescrsrp and a.ltescrsrp<=-110,1,0)) intervalone,     sum(if(-110<a.ltescrsrp and a.ltescrsrp<=-100,1,0)) intervaltwo,     sum(if(-100<a.ltescrsrp and a.ltescrsrp<=-90,1,0)) intervalthree,     sum(if(-90<a.ltescrsrp and a.ltescrsrp<=-80,1,0)) intervalfour,     sum(if(-80<a.ltescrsrp,1,0)) intervalfive from mro.temp_mro_base_step1_002_convert_to_true_value a group by      city,     logdate,     hour,     a.eci,     a.cgi,     a.ltescearfcn"
    val drop_base_step4_011 = "drop table if exists mro.temp_mro_base_step4_011_cal_rsrpvalue"
    val create_base_step4_011 ="create table mro.temp_mro_base_step4_011_cal_rsrpvalue as select          eci,     cgi,     avg(ltescrsrp) rsrpvalue  from mro.temp_mro_base_step1_002_convert_to_true_value group by      eci,     cgi"
    val drop_base_step4_012 = "drop table if exists mro.temp_mro_base_step4_012_cal_pmandmrotable_before"
    val create_base_step4_012 = s"create table mro.temp_mro_base_step4_012_cal_pmandmrotable_before as select     a.city,      a.logdate,     a.hour,     a.eci,     a.cgi,     a.rightobj,     a.interobjcounts,     a.overcovercounts,     a.freovercovercounts,     a.intervalone,     a.intervaltwo,     a.intervalthree,     a.intervalfour,     a.intervalfive,     sum(if(b.coefficient_association>=0.03,1,0)) cellsbyovercoverage,     sum(if(b.coefficient_association>=0.03 and abs(b.ltencearfcn-a.ltescearfcn)<=100,1,0)) influencecellsbyovercoverage from mro.temp_mro_base_step4_010_count_sampling_points a left join (select*from mro.temp_mro_base_step2_006_cal_coefficient where city='$city' and logdate='$logdate' and hour='$hour') b on a.eci=b.eci where a.city='$city' and a.logdate='$logdate' and a.hour='$hour' group by      a.city,     a.logdate,     a.hour,     a.eci,     a.cgi,     a.rightobj,     a.interobjcounts,     a.overcovercounts,     a.freovercovercounts,     a.intervalone,     a.intervaltwo,     a.intervalthree,     a.intervalfour,     a.intervalfive "
    val insert_nas_pmandmrotable = "insert into table mro.mro_nas_pmandmrotable select     city,     logdate,     hour,     a.cgi,     decode(binary(c.cellname),'utf-8') cellname,     c.longitude,     c.latitude,     c.earfcn,     if(38850<=c.earfcn and c.earfcn<39050,'E1',if(39050<=c.earfcn and c.earfcn<39250,'E2',if(39250<=c.earfcn and c.earfcn<39350,'E3',if((37800<=c.earfcn and c.earfcn<38000) or (40440<=c.earfcn and c.earfcn<40640),'D1',if((38000<=c.earfcn and c.earfcn<38200) or (40640<=c.earfcn and c.earfcn<40840),'D2',if((38200<=c.earfcn and c.earfcn<38250) or (40840<=c.earfcn and c.earfcn<41040),'D3',if(38300<=c.earfcn and c.earfcn<38500,'F1',if(38500<=c.earfcn and c.earfcn<38600,'F2','未知')))))))) frequence,     c.pci,     a.eci,     b.rsrpvalue,     a.rightobj,     a.interobjcounts,     a.overcovercounts,     a.freovercovercounts,     a.intervalone,     a.intervaltwo,     a.intervalthree,     a.intervalfour,     a.intervalfive,     if(a.rightobj!=0,a.interobjcounts/rightobj,0) mrcovratio,     if(a.interobjcounts!=0,a.freovercovercounts/a.interobjcounts,0) overlapcoverage,     a.cellsbyovercoverage,     a.influencecellsbyovercoverage from mro.temp_mro_base_step4_012_cal_pmandmrotable_before a left join mro.temp_mro_base_step4_011_cal_rsrpvalue b on a.eci=b.eci left join mro.v_eptable c on a.eci=c.eci"
    val sqlArr = new Array[String](6)
    sqlArr(0) = insert_base_step4_010
    sqlArr(1) = drop_base_step4_011
    sqlArr(2) = create_base_step4_011
    sqlArr(3) = drop_base_step4_012
    sqlArr(4) = create_base_step4_012
    sqlArr(5) = insert_nas_pmandmrotable
    sqlArr
  }

  //报表
  def get_nas_scmatrixtable():Array[String]={
    val insert_nas_scmatrixtable = s"insert into table mro.mro_nas_scmatrixtable select     a.city,     a.logdate,     a.hour,     a.cgi interedcellcgi,     b.nc_cgi intersourcecellcgi,     if(a.coefficient_association>0.001,a.coefficient_association,0) interindex from mro.temp_mro_base_step2_006_cal_coefficient a left join mro.mro_cal_nearest_neighborhood b on a.city=b.city and a.eci=b.sc_eci and a.ltencearfcn=b.ltencearfcn and a.ltencpci=b.ltencpci where a.city='$city' and a.logdate='$logdate' and a.hour='$hour'"
    val sqlArr = new Array[String](1)
    sqlArr(0) = insert_nas_scmatrixtable
    sqlArr
  }

  //天粒度报表
  def get_nas_scmatrixtable_day():Array[String]={
    val drop_base_step5_013 = "drop table if exists mro.temp_mro_base_step5_013_hour_to_day_cal_numerator_denominator"
    val create_base_step5_013 = s"create table mro.temp_mro_base_step5_013_hour_to_day_cal_numerator_denominator as select     a.city,     a.logdate,     eci,     cgi,     ltencearfcn,     ltencpci,     sum(cellsbyovercoverage_before) cellsbyovercoverage_before,     sum(interobjcounts) interobjcounts from mro.temp_mro_base_step2_006_cal_coefficient a where a.city='$city' and a.logdate='$logdate' group by     a.city,     a.logdate,     eci,     cgi,     ltencearfcn,     ltencpci"
    val drop_base_step5_014 = "drop table if exists mro.temp_mro_base_step5_014_hour_to_day_cal_coefficient"
    val create_base_step5_014 = "create table mro.temp_mro_base_step5_014_hour_to_day_cal_coefficient as select     city,     logdate,     eci,     cgi,     ltencearfcn,     ltencpci,     cellsbyovercoverage_before,     interobjcounts,     if(interobjcounts!=0,cellsbyovercoverage_before/interobjcounts,0) coefficient_association from mro.temp_mro_base_step5_013_hour_to_day_cal_numerator_denominator"
    val insert_nas_scmatrixtable_day = "insert into table mro.mro_nas_scmatrixtable_day select     a.city,     a.logdate,     a.cgi interedcellcgi,     b.nc_cgi intersourcecellcgi,     if(a.coefficient_association>0.001,a.coefficient_association,0) interindex from mro.temp_mro_base_step5_014_hour_to_day_cal_coefficient a left join mro.mro_cal_nearest_neighborhood b on a.city=b.city and a.eci=b.sc_eci and a.ltencearfcn=b.ltencearfcn and a.ltencpci=b.ltencpci"
    val sqlArr = new Array[String](5)
    sqlArr(0) = drop_base_step5_013
    sqlArr(1) = create_base_step5_013
    sqlArr(2) = drop_base_step5_014
    sqlArr(3) = create_base_step5_014
    sqlArr(4) = insert_nas_scmatrixtable_day
    sqlArr
  }

  //天粒度报表
  def get_nas_pmandmrotable_day():Array[String]={
    val drop_base_step6_015 = "drop table if exists mro.temp_mro_base_step6_015_hour_to_day_count_sampling_points"
    val create_base_step6_015 = s"create table mro.temp_mro_base_step6_015_hour_to_day_count_sampling_points as select      city,     logdate,     eci,     cgi,     sum(rightobj) rightobj,     ltescearfcn,     sum(interobjcounts) interobjcounts,     sum(overcovercounts) overcovercounts,     sum(freovercovercounts) freovercovercounts,     sum(intervalone) intervalone,     sum(intervaltwo) intervaltwo,     sum(intervalthree) intervalthree,     sum(intervalfour) intervalfour,     sum(intervalfive) intervalfive from mro.temp_mro_base_step4_010_count_sampling_points where city='$city' and logdate='$logdate' group by      city,     logdate,     eci,     cgi,     ltescearfcn"
    val drop_base_step6_016 ="drop table if exists mro.temp_mro_base_step6_016_hour_to_day_cal_rsrpvalue"
    val create_base_step6_016 = s"create table mro.temp_mro_base_step6_016_hour_to_day_cal_rsrpvalue as select          eci,     cgi,     avg(if(ltescrsrp=-111,null,ltescrsrp-141)) rsrpvalue  from mro.mro_dt_rec where city='$city' and logdate='$logdate' group by      eci,     cgi    "
    val drop_base_step6_017 = "drop table if exists mro.temp_mro_base_step6_017_hour_to_day_to_cal_influencecellsbyovercoverage"
    val create_base_step6_017 = s"create table mro.temp_mro_base_step6_017_hour_to_day_to_cal_influencecellsbyovercoverage as select     a.city,      a.logdate,     a.eci,     a.cgi,     a.rightobj,     a.interobjcounts,     a.overcovercounts,     a.freovercovercounts,     a.intervalone,     a.intervaltwo,     a.intervalthree,     a.intervalfour,     a.intervalfive,     sum(if(b.coefficient_association>=0.03,1,0)) cellsbyovercoverage,     sum(if(b.coefficient_association>=0.03 and abs(b.ltencearfcn-a.ltescearfcn)<=100,1,0)) influencecellsbyovercoverage from mro.temp_mro_base_step6_015_hour_to_day_count_sampling_points a left join mro.temp_mro_base_step5_014_hour_to_day_cal_coefficient b on a.eci=b.eci where a.city='$city' and a.logdate='$logdate' and b.city='$city' and b.logdate='$logdate' group by      a.city,     a.logdate,     a.eci,     a.cgi,     a.rightobj,     a.interobjcounts,     a.overcovercounts,     a.freovercovercounts,     a.intervalone,     a.intervaltwo,     a.intervalthree,     a.intervalfour,     a.intervalfive"
    val insert_nas_pmandmrotable_day = "insert into table mro.mro_nas_pmandmrotable_day select     city,     logdate,     a.cgi,     decode(binary(c.cellname),'utf-8') cellname,     c.longitude,     c.latitude,     c.earfcn,     if(38850<=c.earfcn and c.earfcn<39050,'E1',if(39050<=c.earfcn and c.earfcn<39250,'E2',if(39250<=c.earfcn and c.earfcn<39350,'E3',if((37800<=c.earfcn and c.earfcn<38000) or (40440<=c.earfcn and c.earfcn<40640),'D1',if((38000<=c.earfcn and c.earfcn<38200) or (40640<=c.earfcn and c.earfcn<40840),'D2',if((38200<=c.earfcn and c.earfcn<38250) or (40840<=c.earfcn and c.earfcn<41040),'D3',if(38300<=c.earfcn and c.earfcn<38500,'F1',if(38500<=c.earfcn and c.earfcn<38600,'F2','未知')))))))) frequence,     c.pci,     a.eci,     b.rsrpvalue,     a.rightobj,     a.interobjcounts,     a.overcovercounts,     a.freovercovercounts,     a.intervalone,     a.intervaltwo,     a.intervalthree,     a.intervalfour,     a.intervalfive,     if(a.rightobj!=0,a.interobjcounts/rightobj,0) mrcovratio,     if(a.interobjcounts!=0,a.freovercovercounts/a.interobjcounts,0) overlapcoverage,     a.cellsbyovercoverage,     a.influencecellsbyovercoverage from mro.temp_mro_base_step6_017_hour_to_day_to_cal_influencecellsbyovercoverage a left join mro.temp_mro_base_step6_016_hour_to_day_cal_rsrpvalue b on a.eci=b.eci left join mro.v_eptable c on a.eci=c.eci"
    val sqlArr = new Array[String](7)
    sqlArr(0) = drop_base_step6_015
    sqlArr(1) = create_base_step6_015
    sqlArr(2) = drop_base_step6_016
    sqlArr(3) = create_base_step6_016
    sqlArr(4) = drop_base_step6_017
    sqlArr(5) = create_base_step6_017
    sqlArr(6) = insert_nas_pmandmrotable_day
    sqlArr
  }

  def getAllOrderHourSql():ArrayBuffer[String]={
    val sqlHour = new ArrayBuffer[String]
    sqlHour ++= getBaseStep1()
    sqlHour ++= getBaseStep2()
    sqlHour ++= getBaseStep3()
    sqlHour ++= get_nas_pmandmrotable()
    sqlHour ++= get_nas_scmatrixtable()
    sqlHour
  }

  def getAllOrderDaySql():ArrayBuffer[String]={
    val sqlDay = new ArrayBuffer[String]
    sqlDay ++= get_nas_scmatrixtable_day()
    sqlDay ++= get_nas_pmandmrotable_day()
    sqlDay
  }


}
