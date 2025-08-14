FROM python:3.11-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /home/jovyan
COPY notebooks /home/jovyan/notebooks

ARG SERVICE_PORT=8888
ENV SERVICE_PORT=${SERVICE_PORT}

CMD ["sh","-c", \
     "jupyter notebook --ip=0.0.0.0 --port=${SERVICE_PORT} \
      --no-browser --NotebookApp.token='' --NotebookApp.password='' \
      --notebook-dir=/home/jovyan/notebooks --allow-root"]