from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List
from uuid import UUID

from sqlmodel import Session

from app.core.schemas import TenantDataSourceRead, TenantDataSourceUpdate
from app.database.connection import get_session
from app.models.excel_data import Tenant, TenantDataSource, DataSource

router = APIRouter(prefix="/tenant-datasources", tags=["Tenant-DataSources"])


# ✅ Assign a DataSource to a Tenant
@router.post("/{tenant_id}/assign/{source_id}", response_model=TenantDataSourceRead)
async def assign_data_source(
    tenant_id: UUID,
    source_id: UUID,
    session: Session = Depends(get_session),
):
    # Validate tenant
    tenant_result = session.execute(select(Tenant).where(Tenant.tenant_id == tenant_id))
    if not tenant_result.scalars().first():
        raise HTTPException(status_code=404, detail="Tenant not found")

    # Validate data source
    source_result = session.execute(select(DataSource).where(DataSource.source_id == source_id))
    if not source_result.scalars().first():
        raise HTTPException(status_code=404, detail="Data source not found")

    # Ensure not duplicate
    existing = session.execute(
        select(TenantDataSource).where(
            TenantDataSource.tenant_id == tenant_id,
            TenantDataSource.source_id == source_id,
        )
    )
    if existing.scalars().first():
        raise HTTPException(status_code=400, detail="Already assigned")

    tenant_ds = TenantDataSource(tenant_id=tenant_id, source_id=source_id)
    session.add(tenant_ds)
    session.commit()
    session.refresh(tenant_ds)
    return tenant_ds


# ✅ Get all DataSources for a Tenant
@router.get("/{tenant_id}", response_model=List[TenantDataSourceRead])
async def get_tenant_data_sources(tenant_id: UUID, session: Session = Depends(get_session)):
    result = session.execute(
        select(TenantDataSource).where(TenantDataSource.tenant_id == tenant_id)
    )
    return result.scalars().all()


# ✅ Update (enable/disable/configuration)
@router.put("/{tenant_id}/update/{source_id}", response_model=TenantDataSourceRead)
async def update_tenant_data_source(
    tenant_id: UUID,
    source_id: UUID,
    payload: TenantDataSourceUpdate,
    session: Session = Depends(get_session),
):
    result = session.execute(
        select(TenantDataSource).where(
            TenantDataSource.tenant_id == tenant_id,
            TenantDataSource.source_id == source_id,
        )
    )
    tenant_ds = result.scalars().first()
    if not tenant_ds:
        raise HTTPException(status_code=404, detail="Relation not found")

    if payload.is_enabled is not None:
        tenant_ds.is_enabled = payload.is_enabled
    if payload.configuration is not None:
        tenant_ds.configuration = payload.configuration

    session.add(tenant_ds)
    session.commit()
    session.refresh(tenant_ds)
    return tenant_ds


# ✅ Remove (Unassign) DataSource
@router.delete("/{tenant_id}/remove/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_data_source(
    tenant_id: UUID, source_id: UUID, session: Session = Depends(get_session)
):
    result = session.execute(
        select(TenantDataSource).where(
            TenantDataSource.tenant_id == tenant_id,
            TenantDataSource.source_id == source_id,
        )
    )
    tenant_ds = result.scalars().first()
    if not tenant_ds:
        raise HTTPException(status_code=404, detail="Relation not found")

    session.delete(tenant_ds)
    session.commit()
    return {"detail": "Unassigned successfully"}
