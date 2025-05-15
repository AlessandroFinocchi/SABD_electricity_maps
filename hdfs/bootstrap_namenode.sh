#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo sh $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start

hdfs namenode -format
$HADOOP_PREFIX/sbin/start-dfs.sh

echo "I'm a namenode"

hdfs dfs -mkdir -p /data
hdfs dfs -chown -R spark:spark /data
hdfs dfs -put /usr/local/test_file.txt /data/test_file.txt

# Launch bash console  
/bin/bash
