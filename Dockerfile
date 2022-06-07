FROM databricksruntime/standard:7.x

# updates and install ubuntu packages
RUN apt-get update \
    && apt-get install -y \
        build-essential \
        python3.8-dev \
    && apt-get clean \
    && apt-get install -y locales && locale-gen en_US.UTF-8

# RUN virtualenv -p python3.8 --system-site-packages /databricks/python3

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV MLFLOW_TRACKING_URI databricks

# timezone env
ENV TZ UTC

# add to github repo
LABEL org.opencontainers.image.source https://github.com/vinothorg01/spire
# create symlink for python 3.8 and pip3.8
RUN ln -s /databricks/python3/bin/python3.8 /usr/bin/python
RUN ln -s /databricks/python3/bin/pip3.8 /usr/bin/pip

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt --no-cache-dir

COPY . /tmp/falcon

RUN pip install /tmp/falcon

# clean up
RUN rm -rf /var/lib/apt/lists/* /tmp/*.yml /var/tmp/*
