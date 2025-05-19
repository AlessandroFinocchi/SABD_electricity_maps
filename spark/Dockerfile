FROM apache/spark:3.5.0

USER root
RUN pip install nipyapi requests
RUN mkdir /opt/spark/scripts\
    && chmod 777 /opt/spark/scripts

#USER spark