FROM docker.io/library/python:3.14-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ services/
COPY shared/ shared/

CMD ["python", "-m", "services.edge.main", "--single"]
