Real-Time Ride-Hailing Data Pipeline
This project implements a scalable, end-to-end, real-time streaming ETL pipeline. It simulates ride-hailing trip data, processes it in real-time using modern data engineering tools, and stores aggregated metrics in a database for live dashboarding and analysis.

The entire environment is containerized using Docker, ensuring easy setup and reproducibility. The pipeline is fully orchestrated with Prefect, allowing for scheduled runs, monitoring, and automated management of dataflows.

Tech Stack & Core Concepts
Category	Technology	Purpose
Data Ingestion	Python, kafka-python	A script to generate realistic mock data and publish it to a Kafka topic.
Data Streaming	Apache Kafka	Acts as a distributed, durable message broker to buffer and serve real-time data.
Stream Processing	Apache Spark Structured Streaming	The core processing engine for consuming data from Kafka, performing transformations, and calculating windowed aggregations.
Orchestration	Prefect 2	Manages the data pipeline, including scheduling, dependency management, automatic retries, and providing a UI for observability.
Data Storage	PostgreSQL	A reliable relational database used to store the final, aggregated metrics for analytical queries.
Infrastructure	Docker & Docker Compose	Creates a fully containerized, isolated, and reproducible environment for all services.
BI & Reporting	Power BI (or any BI Tool)	Connects to PostgreSQL to display live KPIs and business metrics.
Architecture
The data flows through the system in the following sequence. This architecture is decoupled and scalable, meaning each component can be scaled independently to handle increased load.

Data Producer (Python) → Apache Kafka → Spark Structured Streaming → PostgreSQL → Power BI

<br>
<p align="center">
<img src="docs/architecture-diagram.png" width="800">
<em>(Recommendation: Create a simple diagram like this and save it in a `docs/` folder)</em>
</p>
<br>
Key Features
Real-Time Data Processing: Leverages Spark Structured Streaming to process data with low latency as it arrives.
Containerized & Reproducible: The entire infrastructure (Kafka, Zookeeper, Spark, Postgres) can be launched with a single Docker Compose command, guaranteeing a consistent environment.
Fully Orchestrated Pipeline: Uses Prefect to define, schedule, and monitor the data producer and the Spark processing job, demonstrating production-ready workflow management.
Idempotent Database Writes: The Spark job uses an INSERT ... ON CONFLICT DO UPDATE (upsert) pattern to write to PostgreSQL. This ensures that data can be re-processed without creating duplicates, a critical feature for reliable data pipelines.
Windowed Aggregations: Calculates metrics like total_trips and average_fare over 1-minute tumbling windows, a common requirement in real-time analytics.
Configuration as Code: The entire infrastructure and orchestration are defined in code (docker-compose.yml, deploy.py), following modern DevOps and DataOps best practices.
How to Run the Pipeline
Follow these steps to set up and run the entire pipeline on your local machine.

Prerequisites
Docker Desktop: Ensure Docker is installed and running.
Python 3.11: This project is built and tested with Python 3.11.
Git: For cloning the repository.
Step 1: Clone the Repository
code
Bash
git clone <your-repo-url>
cd real-time-ride-hailing-pipeline
Step 2: Set Up the Python Environment
It is highly recommended to use a virtual environment.

code
Bash
# Create a virtual environment using Python 3.11
python3.11 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Install all required dependencies
pip install -r requirements.txt
Step 3: Launch the Infrastructure (Docker)
In a dedicated terminal window, start all the necessary services (Kafka, Zookeeper, PostgreSQL).

code
Bash
docker-compose -f infra/docker-compose.yml up
Leave this terminal running. It manages your infrastructure.

Step 4: Launch the Orchestrator (Prefect)
Start the Prefect Server: In a second terminal, start the Prefect UI and backend.
code
Bash
# Activate the environment first
source .venv/bin/activate
prefect server start
Leave this running. You can access the UI at http://127.0.0.1:4200.
Start the Prefect Agent: In a third terminal, start the agent that will run your flows.
code
Bash
# Activate the environment first
source .venv/bin/activate

# Create the work queue if it doesn't exist
prefect work-queue create streaming-pipeline

# Start the agent
prefect agent start -q streaming-pipeline
Leave this running. It will pick up and execute your jobs.
Step 5: Deploy and Run the Flows
Apply Deployments: In a fourth terminal, register the producer and Spark flows with Prefect.
code
Bash
# Activate the environment first
source .venv/bin/activate

# Run the deployment script as a module
python -m prefect.deploy
Run from the UI:
Open your browser and navigate to the Prefect UI: http://127.0.0.1:4200
Go to the Deployments page.
First, click the Run button for the "Streaming Data Producer" deployment.
Next, click the Run button for the "Spark Streaming Job" deployment.
Step 6: Verify the Output
You can see the final, aggregated data in the PostgreSQL database.

Connect to the Postgres container:
code
Bash
docker exec -it postgres psql -U user -d rides
Query the metrics table:
code
SQL
SELECT * FROM city_metrics ORDER BY total_trips DESC;
You should see a table with live data being continuously updated.
code
Code
city     | total_trips |   average_fare    |    last_updated
--------------+-------------+-------------------+---------------------
 New York     |          18 | 73.77             | 2025-08-12 21:01:00
 Los Angeles  |          15 | 85.74             | 2025-08-12 21:01:00
 ...
Connecting Power BI
Open Power BI Desktop and select Get Data -> PostgreSQL database.
Server: localhost
Database: rides
Enter credentials: Username: user, Password: password.
In the Navigator, select the city_metrics table.
For Connectivity settings, choose DirectQuery to get a live view of the data.
Click Load and build your visualizations.
Repository Structure
code
Code
├── docs/
│   └── architecture-diagram.png  # Project architecture diagram
├── infra/
│   ├── docker-compose.yml        # Docker Compose for infrastructure
│   └── init.sql                  # Initial SQL to create Postgres tables
├── prefect/
│   └── deploy.py                 # Prefect deployment definitions
├── producer/
│   └── run_producer.py           # Kafka producer script (Prefect flow)
├── spark_jobs/
│   └── streaming_job.py          # Spark Structured Streaming job (Prefect flow)
├── .gitignore
├── README.md                     # This file
└── requirements.txt              # Python dependencies
Future Improvements
Add Unit and Integration Tests: Implement pytest to test data generation and transformation logic.
Schema Management: Introduce a Schema Registry (e.g., Confluent Schema Registry) to manage and enforce data schemas between the producer and consumer.
Cloud Deployment: Adapt the project to run on a cloud provider like AWS (using MSK for Kafka, EMR for Spark, and RDS for PostgreSQL).
CI/CD: Set up a GitHub Actions workflow to automatically test and deploy changes.