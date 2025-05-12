FROM apache/nifi:latest

RUN curl https://repo1.maven.org/maven2/org/apache/nifi/nifi-hadoop-nar/2.4.0/nifi-hadoop-nar-2.4.0.nar -o $NIFI_HOME/lib/nifi-hadoop-nar-2.4.0.nar

RUN curl https://repo1.maven.org/maven2/org/apache/nifi/nifi-hadoop-libraries-nar/2.4.0/nifi-hadoop-libraries-nar-2.4.0.nar -o $NIFI_HOME/lib/nifi-hadoop-libraries-nar-2.4.0.nar