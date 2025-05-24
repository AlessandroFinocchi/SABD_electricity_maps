#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo sh $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start

hdfs namenode -format
$HADOOP_PREFIX/sbin/start-dfs.sh
hdfs dfs -mkdir -p /data
hdfs dfs -chown -R spark:spark /data            # 1) define spark owner/group
hdfs dfs -chmod -R 775 /data                    # 2) owner and group full-permissions, other read-execute permissions
hdfs dfs -setfacl -R -m user:root:rwx /data     # 3) set ACL rwx permissions for webUI user
hdfs dfs -setfacl -R -m user:nifi:rwx /data     # 4) set ACL rwx permissions for nifi user
hdfs dfs -setfacl -R -m user:dr.who:rwx /data   # 5) set ACL rwx permissions for webUI user

echo "I'm a namenode"

# Launch bash console  
/bin/bash
