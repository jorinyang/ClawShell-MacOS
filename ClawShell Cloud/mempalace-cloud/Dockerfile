FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir mempalace
ENV PALACE_PATH=/data/palace
ENV PYTHONPATH=/app
VOLUME /data
EXPOSE 8444
CMD ["python", "-m", "mempalace.mcp_server"]
