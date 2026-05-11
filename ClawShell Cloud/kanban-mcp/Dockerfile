FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ /app/src/
ENV PYTHONPATH=/app
ENV KANBAN_DATA_PATH=/data
VOLUME /data
CMD ["python", "-m", "src.kanban"]
EXPOSE 8446
