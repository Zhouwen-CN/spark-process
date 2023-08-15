#!/bin/bash

# 假定传入参数table_insert-default-student2,20230705

set -e

function check_empty(){
    if [[ -z $2 ]]; then
        echo "check parameter empty error, $1 = $2 ..."
        exit 1
    else
        echo "print parameter, $1 = $2"
    fi
}

function search_file() {
  result=`find $1 -name "$2"`
  number=`find $1 -name "$2" | wc -l`
  if [ $number -ne 1 ]; then
      echo "search config file error, must be get 1 but get $number ..."
      return 0
  fi
  echo $result
}

# 解析脚本传入参数
args=(${1//,/ })

if [[ ${#args[*]} -lt 2 ]]; then
    echo "check parameter num < 2 error, need task_name and task_time ..."
    exit 1
fi


# 当前路径
cur_dir=$(cd $(dirname $0);pwd)
check_empty cur_dir $cur_dir

# 任务名称
task_name=${args[0]}
check_empty task_name $task_name

# 任务时间
task_time=${args[1]}
check_empty task_time $task_time

# 临时参数
project_dir="${cur_dir}/../.."
center_dir="${project_dir}/center"
config_dir="${center_dir}/config"
core_jar_file="spark-process-1.0-jar-with-dependencies.jar"
config_file="${task_name}.json"
user_config_file=`search_file ${config_dir} ${config_file}`

# spark提交参数封装
submit_tool="spark-submit"
master_config="--master yarn --deploy-mode client"
file_config="--files ${user_config_file}"
main_jar="${center_dir}/${core_jar_file}"

# java参数封装
jar_param=()
jar_param[${#jar_param[@]}]="${user_config_file}"
jar_param[${#jar_param[@]}]="task_name=${task_name}"
jar_param[${#jar_param[@]}]="task_time=${task_time}"
for ((i = 0; i < ${#args[*] - 1}; i++)) do
  if [ ${i} -ge 3 ]; then
    jar_param[${#jar_param[@]}]="${args[i]}";
  fi
done;
echo "print parameter, jar_param = ${jar_param[*]}"

# 提交任务
submit_cmd="${submit_tool} ${master_config} ${file_config} ${main_jar} ${jar_param[*]}"
echo "submit_cmd=$submit_cmd"

$submit_cmd