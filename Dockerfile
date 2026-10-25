FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY supplier supplier
RUN pip install --no-cache-dir .
RUN mkdir -p /app/data && chown -R 65532:65532 /app/data
USER 65532
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:' + __import__('os').environ.get('PORT', '8090') + '/health', timeout=4)"
CMD ["sh", "-c", "exec uvicorn supplier.app:app --host 0.0.0.0 --port ${PORT:-8090}"]
