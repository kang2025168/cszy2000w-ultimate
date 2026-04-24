FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir flask flask-cors pymysql
COPY engine.py api.py backtest.html .
EXPOSE 5051
CMD ["python", "api.py"]
