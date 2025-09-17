from pydantic import BaseModel
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime


# ===== TENANTS =====
class TenantBase(BaseModel):
    tenant_name: str
    is_active: Optional[bool] = True


class TenantCreate(TenantBase):
    pass


class TenantUpdate(BaseModel):
    tenant_name: Optional[str] = None
    is_active: Optional[bool] = None


class TenantRead(TenantBase):
    tenant_id: UUID
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        orm_mode = True


# ===== DATASOURCES =====
class DataSourceBase(BaseModel):
    source_name: str
    source_type: str
    description: Optional[str] = None
    is_active: Optional[bool] = True


class DataSourceRead(DataSourceBase):
    source_id: UUID
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        orm_mode = True


# ===== TENANT-DATASOURCES =====
class TenantDataSourceBase(BaseModel):
    is_enabled: Optional[bool] = True
    configuration: Optional[Dict[str, Any]] = None


class TenantDataSourceCreate(TenantDataSourceBase):
    tenant_id: UUID
    source_id: UUID


class TenantDataSourceUpdate(TenantDataSourceBase):
    pass


class TenantDataSourceRead(TenantDataSourceBase):
    id: UUID
    tenant_id: UUID
    source_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
