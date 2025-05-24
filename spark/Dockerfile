FROM apache/spark:3.5.0

USER root
RUN pip install nipyapi requests
RUN mkdir /opt/spark/src \
    && chmod 777 /opt/spark/src