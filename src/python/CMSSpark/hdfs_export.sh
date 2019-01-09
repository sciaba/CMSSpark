#! /bin/bash

tag=$1
path=/cms/users/asciaba/$tag
year=$2
month=$3

hdfs dfs -ls $path/access_year=$year/month=$month | awk '/access/ {print $8}' | sort -n > tmp

while read line; do
    file=`hdfs dfs -ls $line | awk '/part/{print $8}'`
    hdfs dfs -copyToLocal $file tmpcsv
    cat tmpcsv >> ${tag}_${year}_${month}.csv
    rm -f tmpcsv
done < tmp
#rm -f tmp

