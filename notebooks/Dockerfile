FROM jupyter/all-spark-notebook:spark-3.5.0

COPY --chown=jovyan regtests/client /home/jovyan/client
WORKDIR /home/jovyan/
RUN pip install poetry==1.5.0 && \
    cd client/python && \
    python3 -m poetry install && \
    pip install -e .

WORKDIR /home/jovyan/