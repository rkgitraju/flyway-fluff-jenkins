
# Flyway Fluff

## Overview

Flyway Fluff is an automated database migration and linting project that integrates Flyway, Maven, Python Migra, and SQLFluff to streamline PostgreSQL database schema management, validation, and deployment.

## Features

- **Flyway Integration**: Automated SQL migration versioning and execution
- **Maven Build System**: Dependency management and project compilation
- **Python Migra**: Automatic PostgreSQL schema diff generation
- **SQLFluff Linting**: SQL code style validation and formatting
- **Automated Workflow**: Generate, lint, and deploy SQL migrations automatically

## Prerequisites

- Java 11+
- Maven 3.6+
- Python 3.8+
- PostgreSQL 12+
- Git

## Installation & Setup

### 1. Clone Repository
```bash
git clone <repo-url>
cd flyway-fluff
```

### 2. Maven Dependencies
```bash
mvn clean install
```

### 3. Python Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Database
Edit `src/main/resources/flyway.properties`:
```properties
flyway.url=jdbc:postgresql://localhost:5432/yourdb
flyway.user=postgres
flyway.password=yourpassword
```

## Usage

### Run Migrations
```bash
mvn clea compile flyway:migrate -PprofileName
```

### Generate Schema Diff using migra
```bash
python working_python_auto.py
```

### Lint SQL Files
```bash
sqlfluff lint src/main/resources/db/migration/
```

### Format SQL
```bash
sqlfluff format src/main/resources/db/migration/ --fixed
```

## Custom DML Validation Rules

Flyway Fluff includes custom linting rules to validate DML (Data Manipulation Language) commands and ensure compliance with your organization's SQL standards.

### DML Command Validation

The custom rules validate:
- INSERT statement structure and column specifications
- UPDATE operations with WHERE clause requirements
- DELETE operations with safety constraints
- Transaction handling and rollback scenarios

### Configuration

Custom rules are defined in `.sqlfluff` configuration file. Enable DML validation rules:

```ini
[sqlfluff:rules]
dml_validation = true
require_where_clause = true
```

### Usage

Validate DML commands with custom rules:

```bash
sqlfluff lint src/main/resources/db/migration/db2 --config .sqlfluff
```