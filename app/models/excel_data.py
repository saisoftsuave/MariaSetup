import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import UUID, uuid4

from sqlmodel import SQLModel, Field, Relationship, JSON, Column


# TENANTS MODEL
class Tenant(SQLModel, table=True):
    __tablename__ = "tenants"
    tenant_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    tenant_name: str = Field(unique=True, max_length=255)
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(default=True)

    # Relationships
    tenant_data_sources: List["TenantDataSource"] = Relationship(back_populates="tenant")
    raw_data: List["RawData"] = Relationship(back_populates="tenant")
    mapped_values: List["MappedValue"] = Relationship(back_populates="tenant")


# Global Data Sources (constant for all tenants)
class DataSource(SQLModel, table=True):
    __tablename__ = "data_sources"

    source_id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)
    source_name: str = Field(max_length=255, unique=True)  # Made unique since it's global
    source_type: str = Field(max_length=100)
    description: Optional[str] = Field(default=None, max_length=500)
    is_active: bool = Field(default=True)  # Admin can enable/disable data sources
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    # Relationships
    tenant_data_sources: List["TenantDataSource"] = Relationship(back_populates="data_source")
    raw_data: List["RawData"] = Relationship(back_populates="data_source")


# Junction table for many-to-many relationship between tenants and data sources
class TenantDataSource(SQLModel, table=True):
    __tablename__ = "tenant_data_sources"

    id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)
    tenant_id: UUID = Field(foreign_key="tenants.tenant_id", ondelete="CASCADE")
    source_id: UUID = Field(foreign_key="data_sources.source_id", ondelete="CASCADE")

    # Additional configuration per tenant-datasource combination
    is_enabled: bool = Field(default=True)  # Tenant can enable/disable specific data sources
    configuration: Optional[Dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )  # Tenant-specific configuration for this data source

    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    # Relationships
    tenant: Optional[Tenant] = Relationship(back_populates="tenant_data_sources")
    data_source: Optional[DataSource] = Relationship(back_populates="tenant_data_sources")

    # Ensure unique combination of tenant and data source
    class Config:
        table_args = ({"UniqueConstraint": ("tenant_id", "source_id")},)


class RawData(SQLModel, table=True):
    __tablename__ = "raw_data"

    data_id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)
    tenant_id: UUID = Field(foreign_key="tenants.tenant_id", ondelete="CASCADE")
    source_id: UUID = Field(foreign_key="data_sources.source_id", ondelete="CASCADE")

    data_payload: Dict[str, Any] = Field(sa_column=Column(JSON))
    extracted_data: Optional[Dict[str, Any]] = Field(
        sa_column=Column(JSON), default=None
    )

    # Metadata about the data
    data_hash: Optional[str] = Field(default=None, max_length=64)  # For deduplication
    processing_status: Optional[str] = Field(default="pending", max_length=50)  # pending, processed, failed

    created_timestamp: Optional[datetime] = Field(default_factory=datetime.utcnow)
    processed_timestamp: Optional[datetime] = Field(default=None)

    # Relationships
    tenant: Optional[Tenant] = Relationship(back_populates="raw_data")
    data_source: Optional[DataSource] = Relationship(back_populates="raw_data")
    mapped_values: List["MappedValue"] = Relationship(back_populates="raw_data")


class MappedValue(SQLModel, table=True):
    __tablename__ = "mapped_values"

    mapped_id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)
    tenant_id: UUID = Field(foreign_key="tenants.tenant_id", ondelete="CASCADE")
    data_id: UUID = Field(foreign_key="raw_data.data_id", ondelete="CASCADE")

    field_name: str = Field(max_length=255)  # Name of the field in RawData
    mapped_value: str = Field(max_length=255)  # Value mapped by FE
    data_type: Optional[str] = Field(default="string", max_length=50)

    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    # Relationships
    raw_data: Optional[RawData] = Relationship(back_populates="mapped_values")
    tenant: Optional[Tenant] = Relationship(back_populates="mapped_values")


class Sector(SQLModel, table=True):
    __tablename__ = "sectors"

    sector_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    sector_name: str = Field(max_length=255, unique=True)

    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    # Relationships
    default_fields: List["DefaultField"] = Relationship(back_populates="sector")


class DefaultField(SQLModel, table=True):
    __tablename__ = "default_fields"

    field_id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    sector_id: UUID = Field(foreign_key="sectors.sector_id", ondelete="CASCADE")

    field_name: str = Field(max_length=255)
    description: Optional[str] = Field(default=None, max_length=500)
    data_type: Optional[str] = Field(default="string", max_length=50)

    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)

    # Relationships
    sector: Optional[Sector] = Relationship(back_populates="default_fields")