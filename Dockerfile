FROM bitnami/spark:latest

WORKDIR /usr/app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY main.py .

CMD ["spark-submit", "--master", "spark://spark-master:7077", "main.py"]
