from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from uuid import UUID
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from datetime import datetime
import json

from sqlmodel import Session

from app.database.connection import get_session
from app.models.excel_data import (
    RawData, MappedValue, Tenant, DataSource,
    TenantDataSource, Sector, DefaultField
)

router = APIRouter(prefix="/column-mapping", tags=["Column Mapping"])


# ✅ Pydantic Models for Request/Response
class ColumnMappingRequest(BaseModel):
    default_field_id: UUID  # The default field we're mapping
    excel_column: str  # Which Excel column it maps to
    custom_field_name: Optional[str] = None  # Allow custom naming
    data_type: Optional[str] = None  # Use default field's data type if not provided


class ColumnMappingResponse(BaseModel):
    default_field_id: UUID
    default_field_name: str
    excel_column: str
    mapped_field_name: str
    data_type: str
    sector_name: str


class TenantMappingSetup(BaseModel):
    tenant_id: UUID
    source_id: UUID
    sector_id: UUID
    mappings: List[ColumnMappingRequest]


# ✅ Get Available Default Fields for a Sector
@router.get("/sectors/{sector_id}/default-fields")
async def get_default_fields_for_sector(
        sector_id: UUID,
        session: Session = Depends(get_session)
):
    """Get all default fields available for a specific sector"""
    try:
        # Get sector with its default fields
        result = session.execute(
            select(Sector)
            .options(selectinload(Sector.default_fields))
            .where(Sector.sector_id == sector_id)
        )
        sector = result.scalars().first()

        if not sector:
            raise HTTPException(status_code=404, detail="Sector not found")

        return {
            "sector_id": sector.sector_id,
            "sector_name": sector.sector_name,
            "default_fields": [
                {
                    "field_id": field.field_id,
                    "field_name": field.field_name,
                    "description": field.description,
                    "data_type": field.data_type
                }
                for field in sector.default_fields
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Get Excel Columns + Available Default Fields for Mapping
@router.get("/setup/{tenant_id}/{source_id}/{data_id}")
async def get_mapping_setup(
        tenant_id: UUID,
        source_id: UUID,
        data_id: UUID,
        sector_id: UUID,  # Query parameter
        session: Session = Depends(get_session)
):
    """Get Excel columns and available default fields for mapping setup"""
    try:
        # Verify tenant has access to this data source
        tenant_ds_result = session.execute(
            select(TenantDataSource).where(
                TenantDataSource.tenant_id == tenant_id,
                TenantDataSource.source_id == source_id
            )
        )
        tenant_ds = tenant_ds_result.scalars().first()
        if not tenant_ds:
            raise HTTPException(status_code=403, detail="Tenant doesn't have access to this data source")

        # Get raw data
        raw_data_result = session.execute(
            select(RawData).where(RawData.data_id == data_id)
        )
        raw_data = raw_data_result.scalars().first()
        if not raw_data:
            raise HTTPException(status_code=404, detail="Raw data not found")

        # Extract Excel columns from raw data payload
        payload = raw_data.data_payload or {}
        excel_columns = set()
        for sheet, rows in payload.items():
            if isinstance(rows, list) and rows:
                for row in rows:
                    if isinstance(row, dict):
                        excel_columns.update(row.keys())

        excel_columns_list = sorted(list(excel_columns))

        # Get sector default fields
        sector_result = session.execute(
            select(Sector)
            .options(selectinload(Sector.default_fields))
            .where(Sector.sector_id == sector_id)
        )
        sector = sector_result.scalars().first()
        if not sector:
            raise HTTPException(status_code=404, detail="Sector not found")

        # Get existing mappings for this data
        existing_mappings_result = session.execute(
            select(MappedValue).where(MappedValue.data_id == data_id)
        )
        existing_mappings = existing_mappings_result.scalars().all()

        # Create a mapping of default_field_id to existing mapping
        existing_mapping_dict = {}
        for mv in existing_mappings:
            try:
                # field_name contains the default_field_id, mapped_value contains excel_column
                existing_mapping_dict[UUID(mv.field_name)] = mv.mapped_value
            except ValueError:
                # Skip invalid UUIDs
                continue

        return {
            "data_id": data_id,
            "tenant_id": tenant_id,
            "source_id": source_id,
            "sector": {
                "sector_id": sector.sector_id,
                "sector_name": sector.sector_name
            },
            "excel_columns": excel_columns_list,
            "default_fields_with_suggestions": [
                {
                    "field_id": field.field_id,
                    "field_name": field.field_name,
                    "description": field.description,
                    "data_type": field.data_type,
                    "current_mapping": existing_mapping_dict.get(field.field_id),
                    "suggested_excel_columns": [
                                                   col for col in excel_columns_list
                                                   if (field.field_name.lower().replace('_', ' ') in col.lower()
                                                       or col.lower().replace(' ', '_') in field.field_name.lower()
                                                       or any(
                                    word in col.lower() for word in field.field_name.lower().split('_')))
                                               ][:3]  # Top 3 suggestions
                }
                for field in sector.default_fields
            ],
            "existing_mappings_count": len(existing_mappings),
            "tenant_configuration": tenant_ds.configuration or {}
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ✅ Create Column Mappings with Default Fields
@router.post("/create/{tenant_id}/{source_id}/{data_id}")
async def create_column_mappings(
        tenant_id: UUID,
        source_id: UUID,
        data_id: UUID,
        sector_id: UUID,  # Query parameter
        mappings: List[ColumnMappingRequest],
        session: Session = Depends(get_session)
):
    """Create column mappings linking default fields to Excel columns"""
    try:
        # Verify raw data exists and belongs to tenant
        raw_data_result = session.execute(
            select(RawData).where(
                RawData.data_id == data_id,
                RawData.tenant_id == tenant_id
            )
        )
        raw_data = raw_data_result.scalars().first()
        if not raw_data:
            raise HTTPException(status_code=404, detail="Raw data not found")

        # Extract Excel columns for validation
        payload = raw_data.data_payload or {}
        excel_columns = set()
        for sheet, rows in payload.items():
            if isinstance(rows, list) and rows:
                for row in rows:
                    if isinstance(row, dict):
                        excel_columns.update(row.keys())

        # Get sector and default fields
        sector_result = session.execute(
            select(Sector)
            .options(selectinload(Sector.default_fields))
            .where(Sector.sector_id == sector_id)
        )
        sector = sector_result.scalars().first()
        if not sector:
            raise HTTPException(status_code=404, detail="Sector not found")

        # Create lookup for default fields
        default_fields_lookup = {df.field_id: df for df in sector.default_fields}

        # Clear existing mappings for this data
        existing_result = session.execute(
            select(MappedValue).where(MappedValue.data_id == data_id)
        )
        for existing in existing_result.scalars().all():
            session.delete(existing)

        # Create new mappings
        created_mappings = []
        for mapping_req in mappings:
            # Verify default field exists
            default_field = default_fields_lookup.get(mapping_req.default_field_id)
            if not default_field:
                raise HTTPException(
                    status_code=400,
                    detail=f"Default field {mapping_req.default_field_id} not found in sector"
                )

            # Verify Excel column exists in the data
            if mapping_req.excel_column not in excel_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Excel column '{mapping_req.excel_column}' not found in uploaded data"
                )

            # Use custom field name if provided, otherwise use default field name
            mapped_field_name = mapping_req.custom_field_name or default_field.field_name
            data_type = mapping_req.data_type or default_field.data_type

            mapped_value = MappedValue(
                tenant_id=tenant_id,
                data_id=data_id,
                field_name=str(mapping_req.default_field_id),  # Store default_field_id as key
                mapped_value=mapping_req.excel_column,  # Excel column it maps to
                data_type=data_type
            )

            session.add(mapped_value)
            created_mappings.append({
                "default_field_id": default_field.field_id,
                "default_field_name": default_field.field_name,
                "excel_column": mapping_req.excel_column,
                "mapped_field_name": mapped_field_name,
                "data_type": data_type,
                "sector_name": sector.sector_name
            })

        # Update tenant data source configuration to remember sector
        tenant_ds_result = session.execute(
            select(TenantDataSource).where(
                TenantDataSource.tenant_id == tenant_id,
                TenantDataSource.source_id == source_id
            )
        )
        tenant_ds = tenant_ds_result.scalars().first()
        if tenant_ds:
            current_config = tenant_ds.configuration or {}
            current_config["sector_id"] = str(sector_id)
            current_config["last_mapping_date"] = str(datetime.utcnow())
            tenant_ds.configuration = current_config

        session.commit()

        return {
            "message": f"Created {len(created_mappings)} column mappings",
            "sector": {
                "sector_id": sector.sector_id,
                "sector_name": sector.sector_name
            },
            "mappings": created_mappings
        }

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Get Mapped Data with Standardized Column Names
@router.get("/mapped-data/{data_id}")
async def get_mapped_data(
        data_id: UUID,
        session: Session = Depends(get_session)
):
    """Get the raw data with columns renamed according to default field mappings"""
    try:
        # Get raw data
        raw_data_result = session.execute(
            select(RawData).where(RawData.data_id == data_id)
        )
        raw_data = raw_data_result.scalars().first()
        if not raw_data:
            raise HTTPException(status_code=404, detail="Raw data not found")

        # Get mappings (field_name = default_field_id, mapped_value = excel_column)
        mappings_result = session.execute(
            select(MappedValue).where(MappedValue.data_id == data_id)
        )
        mappings = mappings_result.scalars().all()

        if not mappings:
            raise HTTPException(status_code=404, detail="No column mappings found for this data")

        # Get default fields to get proper field names
        default_field_ids = []
        for mv in mappings:
            try:
                default_field_ids.append(UUID(mv.field_name))
            except ValueError:
                continue

        default_fields_result = session.execute(
            select(DefaultField).where(DefaultField.field_id.in_(default_field_ids))
        )
        default_fields = {str(df.field_id): df for df in default_fields_result.scalars().all()}

        # Create mapping: excel_column -> standardized_field_name
        excel_to_standard = {}
        field_mappings_info = []

        for mv in mappings:
            excel_column = mv.mapped_value
            default_field = default_fields.get(mv.field_name)
            if default_field:
                standardized_name = default_field.field_name
                excel_to_standard[excel_column] = standardized_name
                field_mappings_info.append({
                    "default_field_id": default_field.field_id,
                    "default_field_name": default_field.field_name,
                    "excel_column": excel_column,
                    "data_type": mv.data_type
                })

        # Transform the data
        payload = raw_data.data_payload or {}
        mapped_payload = {}
        all_original_columns = set()

        for sheet_name, rows in payload.items():
            if isinstance(rows, list):
                mapped_rows = []
                for row in rows:
                    if isinstance(row, dict):
                        all_original_columns.update(row.keys())
                        mapped_row = {}
                        for excel_col, value in row.items():
                            # Use standardized field name if mapping exists, otherwise keep original
                            standardized_col = excel_to_standard.get(excel_col, excel_col)
                            mapped_row[standardized_col] = value
                        mapped_rows.append(mapped_row)
                mapped_payload[sheet_name] = mapped_rows

        return {
            "data_id": data_id,
            "original_columns": sorted(list(all_original_columns)),
            "standardized_columns": sorted(list(excel_to_standard.values())),
            "field_mappings": field_mappings_info,
            "mapped_data": mapped_payload,
            "total_sheets": len(mapped_payload),
            "total_rows": sum(len(rows) for rows in mapped_payload.values() if isinstance(rows, list))
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ✅ Get All Mappings for a Tenant/DataSource
@router.get("/tenant/{tenant_id}/source/{source_id}")
async def get_tenant_mappings(
        tenant_id: UUID,
        source_id: UUID,
        session: Session = Depends(get_session)
):
    """Get all column mappings for a tenant's data source"""
    try:
        # Get all raw data for this tenant/source
        raw_data_result = session.execute(
            select(RawData).where(
                RawData.tenant_id == tenant_id,
                RawData.source_id == source_id
            )
        )
        raw_data_list = raw_data_result.scalars().all()

        mappings_summary = []
        for rd in raw_data_list:
            # Get mappings for this data
            mappings_result = session.execute(
                select(MappedValue).where(MappedValue.data_id == rd.data_id)
            )
            mappings = mappings_result.scalars().all()

            if mappings:
                mappings_summary.append({
                    "data_id": rd.data_id,
                    "data_hash": rd.data_hash,
                    "created_timestamp": rd.created_timestamp,
                    "processing_status": rd.processing_status,
                    "mappings_count": len(mappings),
                    "column_mappings": [
                        {
                            "default_field_id": mv.field_name,  # This stores the default_field_id
                            "excel_column": mv.mapped_value,  # This stores the Excel column name
                            "data_type": mv.data_type
                        }
                        for mv in mappings
                    ]
                })

        return {
            "tenant_id": tenant_id,
            "source_id": source_id,
            "total_datasets": len(mappings_summary),
            "datasets_with_mappings": mappings_summary
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ✅ Delete Column Mappings
@router.delete("/delete/{data_id}")
async def delete_column_mappings(
        data_id: UUID,
        session: Session = Depends(get_session)
):
    """Delete all column mappings for a specific data set"""
    try:
        # Get existing mappings
        existing_result = session.execute(
            select(MappedValue).where(MappedValue.data_id == data_id)
        )
        existing_mappings = existing_result.scalars().all()

        if not existing_mappings:
            raise HTTPException(status_code=404, detail="No mappings found for this data")

        # Delete all mappings
        for mapping in existing_mappings:
            session.delete(mapping)

        session.commit()

        return {
            "message": f"Deleted {len(existing_mappings)} column mappings",
            "data_id": data_id
        }

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")