from fastapi import FastAPI
from contextlib import asynccontextmanager

from .database.connection import create_db_and_tables
from .routers import sector, excel, datasource, tenant, tenant_datasources, tenant_datasource_column_mappings


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Create database tables
    create_db_and_tables()
    yield
    # Shutdown: cleanup if needed

app = FastAPI(
    title="FastAPI with MariaDB and SQLModel",
    description="A sample project using FastAPI, SQLModel, and MariaDB",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(sector.router, prefix="/api/v1")
app.include_router(excel.router, prefix="/api/v1")
app.include_router(datasource.router, prefix="/api/v1")
app.include_router(tenant.router, prefix="/api/v1")
app.include_router(tenant_datasources.router, prefix="/api/v1")
app.include_router(tenant_datasource_column_mappings.router, prefix="/api/v1")




@app.get("/")
def read_root():
    return {"message": "Welcome to FastAPI with MariaDB and SQLModel!"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}