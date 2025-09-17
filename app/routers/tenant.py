from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.future import select
from typing import List
from uuid import UUID

from sqlmodel import Session

from app.core.schemas import TenantRead, TenantUpdate, TenantCreate
from app.database.connection import get_session
from app.models.excel_data import Tenant

router = APIRouter(prefix="/tenants", tags=["Tenants"])


# ✅ Create Tenant
@router.post("/", response_model=TenantRead, status_code=status.HTTP_201_CREATED)
async def create_tenant(payload: TenantCreate, session: Session = Depends(get_session)):
    result = session.execute(select(Tenant).where(Tenant.tenant_name == payload.tenant_name))
    existing = result.scalars().first()
    if existing:
        raise HTTPException(status_code=400, detail="Tenant name already exists")

    tenant = Tenant(tenant_name=payload.tenant_name, is_active=payload.is_active)
    session.add(tenant)
    session.commit()
    session.refresh(tenant)
    return tenant


# ✅ Get All Tenants
@router.get("/", response_model=List[TenantRead])
async def get_tenants(session: Session = Depends(get_session)):
    result = session.execute(select(Tenant))
    return result.scalars().all()


# ✅ Get Tenant by ID
@router.get("/{tenant_id}", response_model=TenantRead)
async def get_tenant(tenant_id: UUID, session: Session = Depends(get_session)):
    result = session.execute(select(Tenant).where(Tenant.tenant_id == tenant_id))
    tenant = result.scalars().first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return tenant


# ✅ Update Tenant
@router.put("/{tenant_id}", response_model=TenantRead)
async def update_tenant(
    tenant_id: UUID,
    payload: TenantUpdate,
    session: Session = Depends(get_session),
):
    result = session.execute(select(Tenant).where(Tenant.tenant_id == tenant_id))
    tenant = result.scalars().first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    if payload.tenant_name is not None:
        tenant.tenant_name = payload.tenant_name
    if payload.is_active is not None:
        tenant.is_active = payload.is_active

    session.add(tenant)
    session.commit()
    session.refresh(tenant)
    return tenant


# ✅ Delete Tenant
@router.delete("/{tenant_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tenant(tenant_id: UUID, session: Session = Depends(get_session)):
    result = session.execute(select(Tenant).where(Tenant.tenant_id == tenant_id))
    tenant = result.scalars().first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    session.delete(tenant)
    session.commit()
    return {"detail": "Tenant deleted successfully"}
