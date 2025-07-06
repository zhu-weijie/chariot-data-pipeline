# Chariot: A Modular, Multi-Target Data Pipeline

**"Chariot" is a demonstration of a professional data engineering project. This concurrent, pluggable pipeline reliably transfers data from a source database (MySQL) to multiple, disparate destinations (PostgreSQL and Neo4j) in a fault-tolerant manner.**

---

## About The Project

This is not just a simple script; it's a small framework built on professional principles.

### Key Architectural Features

*   **Fully Containerized:** The entire environment, including the four services (Python App, MySQL, PostgreSQL, Neo4j), is managed by Docker and Docker Compose. The only prerequisite is Docker.
*   **Pluggable & Modular:** The design is built on abstract `Extractor` and `Loader` interfaces. This allows new data sources or destinations to be added with minimal effort by simply creating a new class that adheres to the interface.
*   **Idempotent & Fault-Tolerant:** The core pipeline uses a **high-water mark** strategy. If the process is interrupted at any point, it can be safely restarted and will seamlessly resume from the last successfully processed record without duplicating or skipping data.
*   **Concurrent Execution:** The pipeline conductor uses a thread pool to run multiple data loading operations in parallel, maximizing efficiency when loading to multiple destinations.
*   **Configuration Driven:** All sensitive information (credentials) and parameters (batch sizes) are managed via a `.env` file and a typed Pydantic settings model, completely separating configuration from code.
*   **Robust Testing & Validation:** The project includes a full `pytest` suite with both unit and integration tests, as well as a separate data integrity audit script to verify correctness post-transfer.

### Architecture Diagram

The data flows from a single source to multiple destinations, orchestrated by the Python application.

```
┌──────────────────┐      ┌────────────────────┐      ┌────────────────────┐
│                  │      │                    │      │                    │
│   MySQL DB       ├──────►   PostgreSQL DB    │      │   Neo4j DB         │
│  (Source)        │      │   (Destination)    │      │   (Destination)    │
│                  │      │                    │      │                    │
└─────────▲────────┘      └─────────▲──────────┘      └─────────▲──────────┘
          │                         │                           │
          │ Extracts                │ Loads                     │ Loads
          │                         │                           │
┌─────────┴─────────────────────────┴───────────────────────────┴──────────┐
│                                                                          │
│                  Chariot Python Application (Conductor)                  │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Built With

*   **Backend:** Python 3.12
*   **Orchestration:** Docker, Docker Compose
*   **Databases:**
    *   MySQL 8.0 (Source)
    *   PostgreSQL 16 (Destination)
    *   Neo4j 5 (Destination)
*   **Key Python Libraries:**
    *   `pydantic` for settings management
    *   `structlog` for structured logging
    *   `mysql-connector-python`, `psycopg2`, `neo4j` for database drivers
    *   `pytest` for testing

---

## Getting Started

To get a local copy up and running, follow these simple steps.

### Prerequisites

*   Docker: [https://www.docker.com/get-started](https://www.docker.com/get-started)

### How to Run

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/zhu-weijie/chariot-data-pipeline.git
    cd chariot-data-pipeline
    ```

2.  **Create the Environment File:**
    Rename the example environment file to `.env`. The default values are already configured to work with Docker Compose.
    ```sh
    mv .env.example .env
    ```

3.  **Build and Start the Services:**
    This command will build the Python image and start all four database and application containers in the background. It will automatically populate the MySQL database with the MovieLens dataset.
    ```sh
    docker compose up --build -d
    ```
    *Wait about 30 seconds for all database services to become healthy.*

---

## Usage

You can run the main ETL pipeline, the testing suite, or the data audit script using `docker compose run`.

### 1. Run the Full ETL Pipeline

This command executes the main conductor, which will concurrently transfer all data from MySQL to both PostgreSQL and Neo4j.

```sh
docker compose run --rm python_app python main.py
```

### 2. Run the Data Integrity Audit

This script runs after the ETL is complete. It samples data from all three databases to verify that the transfer was correct.

```sh
docker compose run --rm python_app python audit.py
```
You should see an `✅ Audit PASSED` message at the end.

### 3. Run the Test Suite

This command runs all unit and integration tests using `pytest`.

```sh
docker compose run --rm python_app pytest
```
You should see `2 passed`.

### 4. Shut Down the Environment

When you are finished, this command will stop and remove all containers and networks. To also remove the database data volumes, add the `-v` flag.

```sh
docker compose down
```
