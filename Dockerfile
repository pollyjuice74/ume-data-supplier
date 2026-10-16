FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY supplier supplier
RUN pip install --no-cache-dir .
USER 65532
EXPOSE 8090
CMD ["uvicorn", "supplier.app:app", "--host", "0.0.0.0", "--port", "8090"]

