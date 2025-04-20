FROM python:3.9

WORKDIR /app

# Copy and install
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

# Install netcat for port check
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Copy app
COPY . .

# Make script executable
RUN chmod +x startup.sh

EXPOSE 6060

CMD ["./startup.sh"]
