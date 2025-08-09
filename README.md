# Chariot: A Modular, Multi-Target, Parallel Data Pipeline

**"Chariot" is a demonstration of a professional data engineering project. This pipeline reliably transfers and aggregates data from a source database (MySQL) to multiple, disparate destinations (PostgreSQL and Neo4j) using a concurrent and parallel-processing framework.**

---

## About The Project

This is not just a simple script; it's a small framework built on professional principles. It performs two main operations in sequence:

1.  **Concurrent Data Transfer:** A multi-threaded pipeline reads raw data from MySQL and loads it into both PostgreSQL and Neo4j *concurrently*.
2.  **Parallel Data Aggregation:** A multi-process pipeline reads the raw data from PostgreSQL, performs a CPU-intensive aggregation, and loads the results back into a summary table.

### Key Architectural Features

*   **Fully Containerized:** The entire environment, including all services (Python App, MySQL, PostgreSQL, Neo4j), is managed by Docker and Docker Compose. The only prerequisite is Docker.
*   **Pluggable & Modular:** The design is built on abstract `Extractor` and `Loader` interfaces. This allows new data sources or destinations to be added with minimal effort.
*   **Idempotent & Fault-Tolerant:**
    *   The raw data transfer uses a **high-water mark** strategy (supporting both single and composite keys) to be safely restartable.
    *   The advanced aggregation pipeline uses a **job control table** to manage state, allowing it to be resumed if interrupted.
*   **Optimized for Performance:**
    *   **Concurrency for I/O:** The initial data transfer uses a `ThreadPoolExecutor` to run I/O-bound tasks concurrently, loading to PostgreSQL and Neo4j at the same time.
    *   **Parallelism for CPU:** The ratings aggregation pipeline uses a `multiprocessing.Pool` to distribute the CPU-bound calculation work across all available CPU cores for true parallel execution.
*   **Configuration Driven:** All sensitive information (credentials) and parameters (batch sizes) are managed via a `.env` file and a typed Pydantic settings model.
*   **Robust Testing & Validation:** The project includes a full `pytest` suite and a separate, comprehensive **data integrity audit script** that validates the raw data transfer and the results of the final aggregation.

### Architecture Diagram

```
                                          ┌────────────────────┐
                                          |                    |
                                    ┌─────►   PostgreSQL DB    │
                                    │     │  (Relational Sink) │
                                    │     └──────────▲─────────┘
                                    │                │
┌──────────────────┐      ┌─────────┴──┐             │ (Aggregates)
│                  │      │  Concurrent│             │
│   MySQL DB       ├──────► (I/O-Bound)├─────────────┤
│  (Source)        │      │  Data Sync │             │
│                  │      └─────────┬──┘             │
└──────────────────┘                │                │
                                    │     ┌──────────▼─────────┐
                                    │     │   Parallel         │
                                    └─────►   (CPU-Bound)      │
                                          │   Aggregation      │
                                          └──────────┬─────────┘
                                                     │
                                          ┌──────────▼─────────┐
                                          │                    │
                                          │   Neo4j DB         │
                                          │   (Graph Sink)     │
                                          └────────────────────┘
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
    *   `pandas` & `numpy` for high-performance aggregation
    *   `multiprocessing` for parallel processing
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
    Rename the example environment file to `.env`.
    ```sh
    cp .env.example .env
    ```

3.  **Build and Start the Services:**
    This command will build the Python image and start all database and application containers in the background. It will automatically populate the MySQL database with the MovieLens dataset.
    ```sh
    docker compose up --build -d
    ```
    *Wait about 30 seconds for all database services to become healthy.*

---

## Usage

You can run the main ETL pipeline, the testing suite, or the data audit script using `docker compose`.

### 1. Run the Full ETL & Aggregation Pipeline

This single command executes the main conductor, which will run all stages of the pipeline in the correct order.

```sh
docker compose run --rm python_app python main.py
```

### 2. Run the Data Integrity Audit

This script runs *after* the main pipeline is complete. It samples data from all three databases to verify that the initial transfer and the final aggregation were both correct.

```sh
docker compose run --rm python_app python audit.py
```
You should see an `✅ Audit PASSED` message at the end.

### 3. Run the Test Suite

This command runs all unit and integration tests using `pytest`.

```sh
docker compose run --rm python_app pytest
```
You should see all tests passing.

### 4. Shut Down the Environment

When you are finished, this command will stop and remove all containers and networks. To also remove the database data volumes, add the `-v` flag.

```sh
docker compose down -v
```

## Design Diagrams

### Relational Model

```mermaid
erDiagram
    movies {
        int movie_id PK "Primary Key"
        varchar title "Movie Title"
        text[] genres "Array of genre strings"
    }

    ratings {
        int user_id PK "Part of Composite PK"
        int movie_id PK, FK "Part of Composite PK, Foreign Key"
        decimal rating "User's rating"
        bigint timestamp "Unix timestamp of rating"
    }

    aggregation_batches {
        int batch_id PK "Primary Key for the job"
        int start_movie_id "Start of movie range"
        int end_movie_id "End of movie range"
        varchar status "'pending', 'processing', 'complete', 'failed'"
    }
    
    ratings_summary {
        int movie_id PK, FK "Foreign Key to movies table"
        decimal average_rating "Calculated average rating"
        int rating_count "Total number of ratings"
    }

    movies ||--|{ ratings : "has"
    movies ||--|| ratings_summary : "is summarized by"
```

### Graph Model

```mermaid
graph TD
    subgraph "Node Types & Properties"
        U[("User <br/> {userId}")];
        M[("Movie <br/> {movieId, title}")];
        G[("Genre <br/> {name}")];
    end

    subgraph "Example Relationships"
        U -- "RATED <br/> {rating: 4.0, timestamp: ...}" --> M;
        M -- "IN_GENRE" --> G;
    end
```

### Class Diagram

```mermaid
classDiagram
    class Extractor {
        <<Interface>>
        +read_batch(batch_size, high_water_mark) list
        +get_next_high_water_mark(batch) any
    }

    class Loader {
        <<Interface>>
        +get_high_water_mark() any
        +write_batch(batch) None
    }

    Extractor <|-- MySQLExtractor
    Extractor <|-- MySQLRatingsExtractor
    
    Loader <|-- PostgresLoader
    Loader <|-- PostgresRatingsLoader
    Loader <|-- Neo4jLoader
    Loader <|-- Neo4jRatingsLoader

    class PipelineConductor {
        -extractor: Extractor
        -loaders: list~Loader~
        +run_concurrently()
    }

    PipelineConductor o-- "1" Extractor
    PipelineConductor o-- "1..*" Loader

    class AggregationDispatcher {
        +run_parallel_aggregation()
    }

    class RatingsAggregator {
        +process_batch(batch_id)
    }

    AggregationDispatcher ..> RatingsAggregator : uses
```

### C4 Component Diagram

```mermaid
C4Component
    title Component Diagram for Chariot ETL System

    System_Boundary(chariot_system, "Chariot Data Pipeline") {
        Container(python_app, "ETL Application", "Python 3.12, Docker", "The main containerized application that runs all ETL logic.")
    }
    
    Container_Boundary(python_app_boundary, "ETL Application") {
        Component(conductor, "Conductor", "Python Class", "Orchestrates I/O-bound pipelines using threads.")
        Component(dispatcher, "Dispatcher", "Python Script", "Orchestrates CPU-bound pipelines using multiprocessing.")
        Component(aggregator, "Aggregator", "Python Class", "Performs CPU-heavy calculations using Pandas.")
        Component(extractors, "Extractors", "Python Module", "Handles reading data from the source database.")
        Component(loaders, "Loaders", "Python Module", "Handles writing data to destination databases.")
    }
    
    SystemDb_Ext(mysql, "MySQL Database", "Source of raw movie and rating data.")
    SystemDb_Ext(postgres, "PostgreSQL Database", "Destination for raw data and aggregated summaries.")
    SystemDb_Ext(neo4j, "Neo4j Database", "Destination for graph model of data.")

    Rel(conductor, extractors, "Uses")
    Rel(conductor, loaders, "Uses")

    Rel(dispatcher, aggregator, "Dispatches work to")
    
    Rel(aggregator, postgres, "Reads raw data from and writes aggregated data to", "JDBC")

    Rel_Back(extractors, mysql, "Reads from", "JDBC")
    Rel_Back(loaders, postgres, "Writes to", "JDBC")
    Rel_Back(loaders, neo4j, "Writes to", "Bolt")
```

### Sequence Diagram

```mermaid
sequenceDiagram
    participant main as main.py
    participant conductor as PipelineConductor
    participant executor as ThreadPoolExecutor
    participant mysql as MySQLExtractor
    participant pg as PostgresLoader
    participant neo4j as Neo4jLoader

    main->>conductor: __init__(extractor, [pg_loader, neo4j_loader])
    main->>conductor: run_concurrently()
    activate conductor
    
    conductor->>executor: submit(_run_pipeline_for_loader, pg)
    conductor->>executor: submit(_run_pipeline_for_loader, neo4j)

    par Postgres Pipeline
        loop For each batch
            executor->>pg: get_high_water_mark()
            executor->>mysql: read_batch()
            executor->>pg: write_batch()
        end
    and Neo4j Pipeline
        loop For each batch
            executor->>neo4j: get_high_water_mark()
            executor->>mysql: read_batch()
            executor->>neo4j: write_batch()
        end
    end
    
    conductor->>main: Execution finished
    deactivate conductor
```

### Process Flowchart

```mermaid
graph TD
    A[Start] --> B(Stage 1: Concurrent Data Transfer);
    
    subgraph Stage 1
        B1[Transfer Movies to PG & Neo4j] --> B2[Transfer Ratings to PG & Neo4j];
    end

    B2 --> C(Stage 2: Parallel Aggregation);
    
    subgraph Stage 2
        C1[Pre-process: Create Batches] --> C2[Dispatch Batches to Worker Pool];
        C2 --> C3[Promote Staging Data to Final Table];
    end
    
    C3 --> G(Stage 3: Data Integrity Audit);
    
    subgraph Stage 3
        G1[Run Audit Script] --> G2{Audit Succeeded?};
    end

    G2 -- Yes --> H[✅ End: Success];
    G2 -- No --> I[❌ End: Failure];
```
