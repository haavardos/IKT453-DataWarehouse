This project was done in the course IKT453-G 25V at University of Agder
## Group members

- Yusef Said (Supabase)
- Håvard Østebø (MongoDB)
- Erik Bakken (Neo4j)

# MovieLens Data Warehouse Project (IKT453)

This project creates and compares three different data warehouse solutions using three different technologies:
- Supabase (PostgreSQL – relational)
- MongoDB (document-based NoSQL)
- Neo4j (graph-based NoSQL)

It also supports:
- Simple Kafka-based data streaming into all three systems
- A unified Dash front-end for running queries and visualizing data
- Full Docker setup for local deployment

## Folder Structure

project/
    - data/                  # MovieLens 100K dataset (CSV files)
    - mongo_dw/              # MongoDB loader, API, consumer
    - supabase_dw/           # Supabase loader, API, consumer
    - neo4j/                 # Neo4j loader, API, consumer
    - datastreaming/         # Kafka producer/consumer
    - frontend/              # Dash front-end
    - docker-compose.yml     # Full stack configuration
    

## How to Run

1. Clone the repository
2. Start the full stack with Docker:
   docker-compose up --build
3. Access the Dash front-end:
   http://localhost:8000

4. (Optional) Run Kafka producer manually in terminal, such as:
   python datastreaming/producer_neo4j.py

## Features

- Scripts for CSV data loading into each system
- REST APIs for each backend (Flask-based)
- Kafka consumers for streaming real-time movies
- Dash app with support for:
  - /movies/{id}
  - /top-rated
  - /most-popular
  - ...and more

## Evaluation Metrics

We compared the systems based on:
- Query performance
- Storage usage
- Aggregation support
- Front-end integration
- Data loading complexity
- Flexibility and scalability

See the full report for charts, discussion, and conclusion.



