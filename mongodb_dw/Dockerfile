FROM python:3.9

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Install netcat (for the startup check)
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Make startup script executable
RUN chmod +x startup.sh

# Expose Flask API port
EXPOSE 5000

CMD ["./startup.sh"]
