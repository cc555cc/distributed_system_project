FROM docker.io/library/python:3.14-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ services/
COPY shared/ shared/
# Central serves the passenger and operator pages itself (services/central/routers/auth.py).
COPY frontend/ frontend/

CMD ["python", "-m", "services.edge.main", "--single"]
