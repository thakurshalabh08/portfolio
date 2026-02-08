FROM python:3.11-slim

WORKDIR /app

COPY AIScientist/backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY AIScientist /app/AIScientist

EXPOSE 8000

CMD ["uvicorn", "AIScientist.backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
