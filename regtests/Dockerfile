FROM apache/spark:3.5.1-python3
ARG POLARIS_HOST=polaris
ENV POLARIS_HOST=$POLARIS_HOST
ENV SPARK_HOME=/opt/spark

USER root
RUN apt update
RUN apt-get install -y diffutils wget curl python3.8-venv
RUN mkdir -p /home/spark &&  \
    chown -R spark /home/spark && \
    mkdir -p /tmp/polaris-regtests && \
    chown -R spark /tmp/polaris-regtests
RUN mkdir /opt/spark/conf && chmod -R 777 /opt/spark/conf

USER spark
ENV PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# Copy and run setup.sh separately so that test sources can change, but the setup script run is still cached
WORKDIR /home/spark/regtests
COPY ./setup.sh /home/spark/regtests/setup.sh
COPY ./pyspark-setup.sh /home/spark/regtests/pyspark-setup.sh
COPY ./client/python /home/spark/regtests/client/python

RUN ./setup.sh

COPY --chown=spark . /home/spark/regtests

CMD ["./run.sh"]