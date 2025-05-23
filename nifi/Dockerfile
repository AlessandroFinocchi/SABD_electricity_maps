FROM apache/nifi:2.4.0

RUN mkdir $NIFI_HOME/conf/hadoop

COPY hdfs_conf/core-site.xml $NIFI_HOME/conf/hadoop/core-site.xml
COPY hdfs_conf/hdfs-site.xml $NIFI_HOME/conf/hadoop/hdfs-site.xml

COPY --chown=nifi:nifi lib/nifi-hadoop-nar-2.4.0.nar $NIFI_HOME/lib/nifi-hadoop-nar-2.4.0.nar
COPY --chown=nifi:nifi lib/nifi-parquet-nar-2.4.0.nar $NIFI_HOME/lib/nifi-parquet-nar-2.4.0.nar
COPY --chown=nifi:nifi lib/nifi-hadoop-libraries-nar-2.4.0.nar $NIFI_HOME/lib/nifi-hadoop-libraries-nar-2.4.0.nar
COPY --chown=nifi:nifi flow.json.gz $NIFI_HOME/conf/flow.json.gz