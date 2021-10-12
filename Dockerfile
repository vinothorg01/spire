FROM databricksruntime/python-conda:9.x AS builder

ENV CONDA_ENV=/databricks/conda/envs/dcs-minimal
ENV PATH=$CONDA_ENV/bin:$PATH

RUN apt-get update && apt-get install git -y

LABEL org.opencontainers.image.source https://github.com/condenast/spire

COPY . /app/
WORKDIR /app
ENV PYTHONPATH=/app

RUN pip install \
    include/kalos \
    include/datasci-common/python \
    && pip install .

ENV SPARK_HOME=$CONDA_ENV/lib/python3.8/site-packages/pyspark
ENV MLFLOW_TRACKING_URI=databricks

FROM builder

ENV MLFLOW_TRACKING_URI ""

CMD tail -f /dev/null
