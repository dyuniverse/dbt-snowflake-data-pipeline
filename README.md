# Data Pipeline: dbt & Snowflake Orchestration with Apache Airflow (Cosmos)

A modern data pipeline project designed to build and automate data transformation processes on Snowflake using dbt Core, with seamless orchestration managed by Apache Airflow via Astronomer Cosmos.

## 📌 **Project Overview**

The project is structured to decouple the Data Transformation layer from the Orchestration layer:

* **Data Transformation (`dbt_project/`):** Contains the complete dbt project, including models (bronze, silver, gold), test cases, and macros to perform data transformations directly inside the Snowflake Data Warehouse.
* **Orchestration (`airflow_project/`):** Leverages Astronomer Cosmos to automatically parse the dbt project into specialized Airflow DAGs and Task Groups, enabling intuitive monitoring and lifecycle management for each dbt model via the Airflow UI.

## 🏗 Repository Structure

```text
dbt-snowflake-data-pipeline/
├── dbt_project/                # 🏢 dbt Project (Data Transformation Layer)
│   ├── analyses/               # Ad-hoc SQL analysis queries
│   ├── macros/                 # Custom SQL macros for reusable logic
│   ├── models/                 # Data model layers (staging, intermediate, marts)
│   ├── seeds/                  # Static raw CSV data files
│   ├── snapshots/              # Change data management logic (SCD Type 2)
│   ├── tests/                  # Custom data quality tests
│   ├── .gitignore              # Specific git ignore for dbt project
│   ├── dbt_project.yml         # Core dbt project configuration file
│   ├── package-lock.yml        # Exact dbt package version lock file
│   ├── packages.yml            # dbt package dependencies declaration
│   └── README.md               # Dedicated documentation for dbt project
│
├── airflow_project/            # ⚡ Airflow Project (Orchestration Layer via Astro CLI)
│   ├── .astro/                 # Internal configuration for Astronomer CLI
│   ├── dags/                   # Airflow DAG definitions & Cosmos Execution Config
│   ├── tests/                  # Unit tests for Airflow DAGs
│   │   └── dags/               # Specific test files for DAGs
│   ├── .dockerignore           # Excluded files list for Docker image builds
│   ├── .gitignore              # Specific git ignore for Airflow project
│   ├── docker-compose.override.yml # Local Docker environment override configuration
│   ├── Dockerfile              # Custom Docker Image & Virtual environment setup
│   ├── packages.txt            # OS-level packages for Docker container
│   ├── requirements.txt        # Python packages (astronomer-cosmos, Snowflake provider)
│   └── README.md               # Default guide documentation from Astro CLI
│
└── .gitignore                  # Root repository git ignore file
```

## 🗄 **Data Source**

The project utilizes the dataset **TPCH**, which comes pre-integrated in the Snowflake Data Warehouse:

* **Database:** `SNOWFLAKE_SAMPLE_DATA`
* **Schema:** `TPCH_SF1`
* **Target Tables:**
  * `ORDERS`: Contains high-level order metadata (order key, customer key, order date, status, total price, etc.).
  * `LINEITEM`: Contains line-level product details within each order (unit price, quantity, discount rate, tax, shipping status, etc.).
 
## 🛠 Tech Stack & Ecosystem

| Technology | Layer | Role & Technical Details |
| :--- | :--- | :--- |
| **Snowflake** | Data Warehouse | Serves as both Storage and Compute Engine. Executes distributed query processing directly pushed down from dbt SQL. |
| **dbt Core** | Transformation | Manages modular data transformation logic (Bronze → Silver → Gold), enforces data quality testing, and automatically generates Data Lineage. |
| **Apache Airflow** | Orchestration | Handles job scheduling, execution lifecycle tracking, workflow automation, and retry mechanisms upon failure. |
| **Astronomer Cosmos** | DAG Dynamic Parser | Automatically parses the dbt project into native Airflow DAGs / Task Groups, enabling granular dbt task visualization on the Airflow UI. |
| **Power BI** | Visualization | Connects directly to the Gold layer on Snowflake to visualize business KPIs and support ad-hoc analysis. |

## 🚀 Project Implementation & Execution Steps

Below are the technical details for each step implemented in the project:

### Step 1: Snowflake Setup & Infrastructure Configuration

Initialize storage infrastructure and Role-Based Access Control (RBAC) on Snowflake Data Warehouse:
* Use `ACCOUNTADMIN` privileges to create a virtual warehouse `ll_wh` with the smallest size (`XSMALL`) to optimize compute costs.
* Create a dedicated role `ll_role`, grant operational privileges on the Warehouse/Database, and assign it to user `nlbduy`.
* Initialize the main database `ll_db` and establish multi-layered schemas: `ll_bronze`, `ll_silver`, and `ll_gold`.

```sql
USE ROLE ACCOUNTADMIN;

-- 1. Create compute warehouse
CREATE WAREHOUSE IF NOT EXISTS ll_wh WITH WAREHOUSE_SIZE = 'XSMALL';

-- 2. Create target database
CREATE DATABASE IF NOT EXISTS ll_db;

-- 3. RBAC Role Setup & Grant Privileges
CREATE ROLE IF NOT EXISTS ll_role;
GRANT USAGE ON WAREHOUSE ll_wh TO ROLE ll_role;
GRANT ROLE ll_role TO USER nlbduy;
GRANT ALL ON DATABASE ll_db TO ROLE ll_role;

-- 4. Switch role to initialize layered schemas
USE ROLE ll_role;

CREATE SCHEMA IF NOT EXISTS ll_db.ll_default_schema;
CREATE SCHEMA IF NOT EXISTS ll_db.ll_bronze;   -- Staging Layer
CREATE SCHEMA IF NOT EXISTS ll_db.ll_silver;   -- Intermediate Layer
CREATE SCHEMA IF NOT EXISTS ll_db.ll_gold;     -- Marts Layer
```

**Credentials Setup**
Upon completing the Snowflake setup, consolidate the core parameters required for `profiles.yml` in dbt Core and the Airflow Connection (`snowflake_conn`):

* Account Identifier: Snowflake account identifier in the format `<account_locator>.<region_or_cloud_provider>` (used by dbt and Airflow to route connection requests).

* User

* Password
  
* Role
  
* Warehouse

* Database

* Schema

### Step 2: Setup dbt Project

#### Step 2.1: Virtual Environment Setup & Dependency Installation
Establish a local development environment for dbt Core on **Linux (Ubuntu via WSL)**.

* **Initialize & Activate Virtual Environment using `uv`:**
   Utilize `uv`, an ultra-fast package manager, as an alternative to traditional `pip` to initialize a virtual environment with Python 3.11:

   ```bash
   # Create a virtual environment with Python 3.11
   uv venv venv-dbt --python 3.11

   # Activate the virtual environment
   source venv-dbt/bin/activate
  ```
* **Install dbt Core and Snowflake Adapter**:
Use uv pip to install the precise version of dbt-core along with the Snowflake adapter:

  ```bash
  uv pip install dbt-core==1.11.14 dbt-snowflake
  ```

#### Step 2.2: Initialize dbt Project

In the root directory of the repository (`dbt-snowflake-data-pipeline`), initialize a new dbt project using the interactive command:

```bash
dbt init dbt_project
```

During execution, the dbt CLI will prompt for connection configuration details (prepared in Step 1):

* Project Name: Enter project name.

* Select Adapter: Select snowflake (enter the corresponding option number from the terminal prompt).

* Account: Enter the Account Identifier retrieved in Step 1 (format: <account_locator>.<region_or_cloud>).

* User: Enter username.

* Password: Enter Snowflake account password.

* Role: Enter the dedicated project role.

* Warehouse: Enter Virtual Warehouse name

* Database: Enter target Database name.

* Schema: Enter default Schema name.

* Threads: Enter the number of concurrent execution threads.

Upon completion, dbt automatically generates a standardized directory structure inside dbt_project/ (including `models/`, `macros/`, `snapshots/`, `tests/`, and the main configuration file `dbt_project.yml`, along with `profiles.yml` stored in the hidden directory `~/.dbt/`).

#### Step 2.3: Build dbt Components

Design and develop the complete data transformation logic within the `dbt_project/` directory:

* **Declare and Install Packages (`packages.yml`):**
  * Create a `packages.yml` file in the same root directory as `dbt_project.yml` to declare utility packages (e.g., `dbt_utils`).
  * Run `dbt deps` to download and manage package dependencies.

* **Models Directory Structure & Materialization:**
  * Structure the `models/` directory following the Medallion Architecture into dedicated subfolders: `bronze`, `silver`, and `gold`.
  * Configure materialization strategies (e.g., `view` for the Bronze/Silver layer, `table` for the Gold layers) directly inside `dbt_project.yml`.

* **Declare Data Sources (`sources.yml`):**
  * Define incoming raw data sources via a `sources.yml` file (located in the `bronze` / staging layer), pointing directly to the `ORDERS` and `LINEITEM` tables in Snowflake's `TPCH_SF1` schema.

* **Develop Models (`.sql` & `.yml`):**
  * Write SQL transformation statements for each model.
  * Accompany each `.sql` file with a corresponding `schema.yml` to define metadata, column documentation, and test assertions.

* **Build Custom Macros (`macros/`):**
  * Create Jinja SQL macros to encapsulate and reuse common calculation or transformation logic across the project.

* **Setup Data Quality Testing:**
  * Configure Generic Tests (such as `not_null`, `unique`, `relationships`) inside the `.yml` schema files.
  * Write Singular Tests (custom SQL queries) in the `tests/` directory to enforce advanced business logic constraints.

### Step 3: Setup Airflow Project (Orchestration via Astro CLI & Cosmos)

Establish the automated Orchestration Layer using Apache Airflow, leveraging Astronomer CLI for containerization and Astronomer Cosmos to parse the dbt project into Airflow DAGs.

#### Step 3.1: Initialize Airflow Project

Return to the root directory of the project and initialize the empty framework structure for Airflow.

```bash
# Return to the root directory and create the airflow_project folder
mkdir airflow_project
cd airflow_project/

# Initialize the Airflow project framework using Astro CLI
astro dev init
```

#### Step 3.2: Core Component Configuration

1. **Declare Python Dependencies (`requirements.txt`):**
   Specify external libraries required for Airflow to connect to Snowflake and parse the dbt project.
   * `astronomer-cosmos`: The bridge library enabling Airflow to dynamically render the dbt project into intuitive Task Groups aligned with the data lineage graph.
   * `apache-airflow-providers-snowflake`: The official provider enabling Airflow connectivity to Snowflake DWH.
     
2. **Environment Isolation via Dockerfile (`Dockerfile`):**
   Configure the `Dockerfile` to initialize a dedicated Virtual Environment (`dbt_venv`) inside the Docker container, completely preventing dependency collisions between `dbt-snowflake` and Airflow Core.

   ```dockerfile
   FROM quay.io/astronomer/astro-runtime:11.3.0

   # Create a dedicated virtual environment for dbt and install dbt-snowflake
   RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
       pip install --no-cache-dir dbt-snowflake && deactivate
   ```

3. **Local Docker Configuration Override (`docker-compose.override.yml`):**
Configure directory mounting to expose the external `dbt_project/` directory inside the Docker container, enabling Cosmos to directly read dbt source code and artifacts during execution.

    ```yaml
       services:
         scheduler:
           volumes:
             - ../dbt_project:/usr/local/airflow/dags/dbt/dbt_project
         webserver:
           volumes:
             - ../dbt_project:/usr/local/airflow/dags/dbt/dbt_project
    ```

4. **Build Orchestration DAG (`dags/dbt_dag.py`):**
   Utilize Astronomer Cosmos to parse the entire dbt project into an Airflow DAG, executing tasks within the isolated `dbt_venv` virtual environment.

   ```python
   import os
   from datetime import datetime
   from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
   from cosmos.profiles import SnowflakeUserPasswordProfileMapping

   profile_config = ProfileConfig(
       profile_name="default",
       target_name="dev",
       profile_mapping=SnowflakeUserPasswordProfileMapping(
           conn_id="snowflake_conn",  
           profile_args={"database": "ll_db", "schema": "ll_default_schema"},
       )
   )

   dbt_snowflake_dag = DbtDag(
       project_config=ProjectConfig("/usr/local/airflow/dags/dbt/dbt_project"),
       operator_args={"install_deps": True},
       profile_config=profile_config,
       execution_config=ExecutionConfig(
           dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
       ),
       schedule_interval="@daily",
       start_date=datetime(2026, 11, 1),
       catchup=False,
       dag_id="dbt_dag",
   )

    ```

#### Step 3.3: Start Containers & Configure Airflow Connection

1. **Start Airflow Services:** 
   Navigate to the `airflow_project/` directory and run the Astro CLI command to build and start all Docker containers (`scheduler`, `webserver`, `postgres`, `triggerer`).

   ```bash
   astro dev start
   ```
2. **Configure Snowflake Connection on the WebUI**:

* Access the Airflow Webserver interface at http://localhost:8080.
* Navigate to Admin ➔ Connections ➔ Create a new connection.
* Fill in the connection parameters (Connection id, Connection type, Account, Warehouse, Database, Role, Username, Password).

3. **Activate DAG Execution**:
Toggle ON the dbt_dag and trigger the DAG. Cosmos will automatically parse the dbt models into dedicated Task Groups and execute the entire data transformation pipeline on Snowflake.

### Step 4: Visualize Data with Power BI (Data Visualization)

* **Connect Power BI to Snowflake:**
  * Open Power BI Desktop ➔ Select **Get Data** ➔ **Snowflake**.
  * Input the Server parameter (`<account_identifier>.snowflakecomputing.com`) and Warehouse.
  * Choose the connection mode (**Import** or **DirectQuery** depending on real-time requirements).

* **Leverage the Gold Layer:**
  * Load only the processed, cleaned, and optimized tables from the Gold schema.

* **Build Reports & Dashboards:**
  * Establish the Data Model, create DAX measures, and design visual reporting dashboards to support business decision-making.
