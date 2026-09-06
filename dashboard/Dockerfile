FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the dashboard code
COPY . .

# Wait for nodes to be ready, then start the server
EXPOSE 8006

CMD ["python", "server.py"]
