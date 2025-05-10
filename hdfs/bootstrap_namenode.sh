#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo sh $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start

hdfs namenode -format
$HADOOP_PREFIX/sbin/start-dfs.sh

echo "I'm a namenode"

# Launch bash console  
/bin/bash
