FROM conda/miniconda3

WORKDIR /app
COPY . /app
COPY .cdsapirc /root/
COPY ./etl_env.yaml /app/etl_env.yaml

RUN apt update -y
RUN conda update -n base -c defaults conda
RUN conda env create -f /app/etl_env.yaml
ENV PYTHONUNBUFFERED=1

CMD conda run -n adcerva_etl --no-capture-output python3 -u transfer-between-buckets.py transfer-properties.yaml