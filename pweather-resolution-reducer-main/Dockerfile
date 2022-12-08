FROM conda/miniconda3

WORKDIR /app
COPY . /app
COPY ./conda_env.yaml /app/conda_env.yaml

RUN apt update -y
RUN conda update -n base -c defaults conda
RUN conda env create -f /app/conda_env.yaml
ENV PYTHONUNBUFFERED=1

CMD conda run -n pweather-resolution-reducer --no-capture-output python3 -u __main__.py