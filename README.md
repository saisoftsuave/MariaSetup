# FastAPI with MariaDB and SQLModel

A sample project using FastAPI, SQLModel, and MariaDB.

## Prerequisites

- Python 3.9+
- MariaDB
- Git

## Installation

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

2. **Create a virtual environment and activate it:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

3. **Install the dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

## Database Setup

1. **Log in to MariaDB as root:**

   ```bash
   mysql -u root -p
   ```

2. **Run the SQL commands in `db_config.sql` to create the database and user:**

   ```sql
   CREATE DATABASE excel_import CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'excel_user'@'localhost' IDENTIFIED BY 'secure_password_123';
   CREATE USER 'excel_user'@'%' IDENTIFIED BY 'secure_password_123';
   GRANT ALL PRIVILEGES ON excel_import.* TO 'excel_user'@'localhost';
   GRANT ALL PRIVILEGES ON excel_import.* TO 'excel_user'@'%';
   FLUSH PRIVILEGES;
   EXIT;
   ```

3. **Create a `.env` file in the root directory and add the following line:**

   ```
   DATABASE_URL=mysql+pymysql://excel_user:secure_password_123@localhost/excel_import
   ```

4. **Run the database migrations:**

   ```bash
   alembic upgrade head
   ```

## Running the Application

1. **Start the FastAPI application:**

   ```bash
   uvicorn app.main:app --reload
   ```

2. **The application will be available at `http://127.0.0.1:8000`**.

## API Endpoints

The following API endpoints are available:

- `GET /`: Welcome message.
- `GET /health`: Health check.
- `GET /docs`: API documentation.

The application also includes routers for the following:

- `/api/v1/sectors`
- `/api/v1/excel`
- `/api/v1/datasources`
- `/api/v1/tenants`
- `/api/v1/tenant-datasources`
- `/api/v1/tenant-datasource-column-mappings`
