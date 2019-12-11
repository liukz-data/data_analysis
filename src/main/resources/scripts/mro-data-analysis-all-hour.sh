#!/bin/bash

############
#从comp_merge读取city_date_hour文件，执行所有小时粒度和天粒度的业务逻辑解析程序
#autor:刘科志
#create_time：2019/11/28
#运行命令：
#sh mro-data-analysis-all-hour.sh
##################

export LANG=en_US.UTF-8

cd `dirname $0`

currpath=`pwd`
mro_data_analyssis_conf=${currpath}/../conf/mrodataanalysis-conf.properties

#日志路径
source $mro_data_analyssis_conf

if [[ -z ${log_dir} ]];then
   log_dir=${currpath}/../logs
fi
if [[ ! -d ${log_dir} ]];then
   mkdir $log_dir
fi

deal_date=`date +'%Y%m%d'`
log_file=${log_dir}/mrodataanalysis_${deal_date}.log

echo "日志路径："$log_file
echo "---------------------------`date +'%F %T'` 开始执行业务逻辑程序--------------------------------------------------------------------">>${log_file}

city_date_hour_files=`hdfs dfs -ls -C ${comp_merge}`
if [[ -z $city_date_hour_files ]];then
   echo "`date +'%F %T'` ${comp_merge} 文件夹无文件程序退出">>${log_file}
   exit 0
fi

spark-submit \
             --principal ${hive_user} --keytab ${hive_keytab} \
             --master yarn --deploy-mode client \
             --num-executors 8 \
             --conf spark.executor.memory=15g \
             --conf spark.executor.cores=18 \
             --conf spark.driver.memory=10g \
             --conf spark.sql.shuffle.partitions=72 \
             --conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=128000000 \
             --conf spark.dynamicAllocation.enabled=false \
             --conf spark.yarn.executor.memoryOverhead=2g \
             --conf spark.network.timeout=300s \
             --queue root.dev \
             --conf spark.blacklist.enabled="false" \
             --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseG1GC -XX:ParallelGCThreads=10 -XX:ConcGCThreads=4 -XX:InitiatingHeapOccupancyPercent=50" \
             --jars ../libs/postgresql-42.2.5.jar \
             --class com.cmdi.data_analysis.submit.MroDataAnalysis \
             ../libs/data_analysis.jar ../conf/mrodataanalysis-conf.properties 1>>${log_file} 2>&1

echo "---------------------------`date +'%F %T'` 结束执行业务逻辑程序--------------------------------------------------------------------">>${log_file}
