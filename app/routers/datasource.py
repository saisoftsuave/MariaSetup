from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel

from app.database.connection import get_session
from app.models.excel_data import DataSource, TenantDataSource, Tenant, RawData


class DataSourceCreate(BaseModel):
    source_name: str
    source_type: str
    description: Optional[str] = None
    is_active: bool = True


class DataSourceResponse(BaseModel):
    source_id: UUID
    source_name: str
    source_type: str
    description: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime


router = APIRouter(prefix="/api/v1/datasources", tags=["Data Sources"])


# GLOBAL DATA SOURCE MANAGEMENT (Admin APIs)

@router.post("/", response_model=DataSourceResponse)
def create_data_source(
        data_source: DataSourceCreate,
        session: Session = Depends(get_session)
):
    """Create a new global data source (Admin only)"""
    try:
        # Check if data source name already exists
        stmt = select(DataSource).where(DataSource.source_name == data_source.source_name)
        existing = session.execute(stmt)
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Data source name already exists")

        db_data_source = DataSource(**data_source.model_dump())
        session.add(db_data_source)
        session.commit()
        session.refresh(db_data_source)

        return db_data_source
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[DataSourceResponse])
def get_all_data_sources(
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        is_active: Optional[bool] = Query(None),
        source_type: Optional[str] = Query(None),
        session: Session = Depends(get_session)
):
    """Get all global data sources with optional filtering"""
    try:
        stmt = select(DataSource)

        # Apply filters
        if is_active is not None:
            stmt = stmt.where(DataSource.is_active == is_active)
        if source_type:
            stmt = stmt.where(DataSource.source_type == source_type)

        stmt = stmt.offset(skip).limit(limit)

        result = session.execute(stmt)
        data_sources = result.scalars().all()

        return data_sources
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{source_id}")
def delete_data_source(
        source_id: UUID,
        session: Session = Depends(get_session)
):
    """Delete a global data source (Admin only)"""
    try:
        stmt = select(DataSource).where(DataSource.source_id == source_id)
        result = session.execute(stmt)
        db_data_source = result.scalar_one_or_none()

        if not db_data_source:
            raise HTTPException(status_code=404, detail="Data source not found")

        # Check if any tenants are using this data source
        tenant_usage_stmt = select(TenantDataSource).where(TenantDataSource.source_id == source_id)
        tenant_usage = session.execute(tenant_usage_stmt)
        if tenant_usage.scalar_one_or_none():
            raise HTTPException(
                status_code=400,
                detail="Cannot delete data source. It is being used by one or more tenants."
            )

        session.delete(db_data_source)
        session.commit()

        return JSONResponse({"message": "Data source deleted successfully"})
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))