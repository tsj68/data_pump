#!/bin/bash

:<<'COMMENT'
功能：
     通用hive同步导入doris
程序设置：
     doris_host  host     本程序代码中维护(开头部分)
     http_port   
     query_port  
     user
     password
     $SPARK_HOME      环境变量
     doris_import.py      和本shell放在相同的目录
程序调用：
     source doris_import.sh   spark参数   程序参数
spark参数：
     --*                    spark参数  支持所有spark参数
程序参数：
     -source  source_sql    *,建议外层用双引号，里层用单引号；否则处理好转义
     -target  table         *
     -partition   
输出：
     success/error  成功记录数/错误信息
COMMENT

doris_host="192.168.1.46"
http_port=8030
query_port=9030
user="root"
password="stupid123321"

set -f

params_spark=""
params_app=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --*)
            params_spark="$params_spark $1 $2"
            shift
            ;;
        -*)
            params_app="$params_app $1 \"$2\""
            shift
            ;;
    esac
    shift
done

script_path=$(dirname "$(readlink -f "$0")")

auth="-doris_host $doris_host -http_port $http_port -query_port $query_port -user $user -password $password"

command="$SPARK_HOME/bin/spark-submit $params_spark $script_path/doris_import.py  $params_app $auth"

#echo $command
eval $command 

