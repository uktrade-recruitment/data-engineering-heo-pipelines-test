FROM python:3.9-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    cron \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data/input data/output logs

RUN echo "station_id,station_name,timestamp,temperature,humidity,wind_speed,precipitation\n\
STN001,London,2025-01-01 12:00:00,68.0,75.5,10.3,0.5\n\
STN002,Manchester,2025-01-01 12:00:00,59.0,80.0,5.5,1.2\n\
STN003,Darlington,2025-01-01 12:00:00,57.2,82.0,6.0,0.8\n\
STN004,Belfast,2025-01-01 12:00:00,55.4,85.0,8.5,1.0\n\
STN005,Edinburgh,2025-01-01 12:00:00,53.6,80.5,7.2,0.7\n\
STN006,Cardiff,2025-01-01 12:00:00,60.8,78.0,9.0,0.9\n\
STN007,Birmingham,2025-01-01 12:00:00,59.0,76.5,6.8,0.6" > data/input/weather_2025-01-01.csv

RUN chmod -R 777 logs

RUN echo "0 3 * * * cd /app && python -m src.main >> logs/cron.log 2>&1" > /etc/cron.d/weather-cron && \
    chmod 0644 /etc/cron.d/weather-cron && \
    crontab /etc/cron.d/weather-cron

RUN echo '#!/bin/bash\necho "Running weather pipeline test..."\npython -m src.main\necho "\nRunning tests..."\npython -m unittest discover\necho "\nTest complete! You can now explore the code and fix the issues."\nexec "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
