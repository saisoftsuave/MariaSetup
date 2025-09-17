from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.future import select
from uuid import UUID
from typing import List, Dict, Any
import pandas as pd
import numpy as np
import io
import hashlib
from datetime import datetime
import datetime as dt

from sqlmodel import Session

from app.database.connection import get_session
from app.models.excel_data import RawData, MappedValue

router = APIRouter(prefix="/raw-data", tags=["Raw Data"])


# ✅ Upload Excel as RawData
@router.post("/{tenant_id}/{source_id}")
async def upload_raw_data(
        tenant_id: UUID,
        source_id: UUID,
        file: UploadFile = File(...),
        session: Session = Depends(get_session),
):
    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="Only Excel files are supported")

    # Read Excel file into pandas - ADD AWAIT HERE
    content = await file.read()  # This was the missing 'await'
    excel_file = io.BytesIO(content)

    try:
        xls = pd.ExcelFile(excel_file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel file: {str(e)}")

    data_payload: Dict[str, Any] = {}
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
            # Convert timestamps and other non-JSON serializable types
            df = df.copy()

            # Convert datetime columns to ISO string format
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').where(df[col].notna(), None)
                elif pd.api.types.is_numeric_dtype(df[col]):
                    # Handle NaN values in numeric columns
                    df[col] = df[col].where(df[col].notna(), None)
                else:
                    # Handle other types and NaN/None values
                    df[col] = df[col].where(df[col].notna(), None)

            # Convert to records with JSON-safe types
            records = df.to_dict(orient="records")

            # Additional cleanup to ensure JSON compatibility
            def make_json_serializable(obj):
                """Convert any object to JSON-serializable format"""
                if pd.isna(obj) or obj is None:
                    return None
                elif isinstance(obj, (pd.Timestamp, pd.NaT.__class__)):
                    return obj.isoformat() if pd.notna(obj) else None
                elif isinstance(obj, (np.integer, np.floating)):
                    return obj.item()
                elif isinstance(obj, np.bool_):
                    return bool(obj)
                elif isinstance(obj, (dt.time,)):
                    return obj.strftime('%H:%M:%S')
                elif isinstance(obj, (dt.date, dt.datetime)):
                    return obj.isoformat()
                elif hasattr(obj, 'isoformat') and callable(getattr(obj, 'isoformat')):
                    try:
                        return obj.isoformat()
                    except:
                        return str(obj)
                elif isinstance(obj, (bytes, bytearray)):
                    return obj.decode('utf-8', errors='ignore')
                elif hasattr(obj, 'item') and callable(getattr(obj, 'item')):
                    # For numpy scalars
                    return obj.item()
                else:
                    return obj

            clean_records = []
            for record in records:
                clean_record = {key: make_json_serializable(value) for key, value in record.items()}
                clean_records.append(clean_record)

            data_payload[sheet] = clean_records

        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to process sheet '{sheet}': {str(e)}")

    # Hash for deduplication
    file_hash = hashlib.sha256(content).hexdigest()

    # Check for duplicate file hash (optional deduplication)
    existing = session.execute(
        select(RawData).where(
            RawData.tenant_id == tenant_id,
            RawData.source_id == source_id,
            RawData.data_hash == file_hash
        )
    ).scalars().first()

    if existing:
        raise HTTPException(status_code=409, detail="File already exists (duplicate hash)")

    raw_data = RawData(
        tenant_id=tenant_id,
        source_id=source_id,
        data_payload=data_payload,
        data_hash=file_hash,
        processing_status="pending",
        created_timestamp=datetime.utcnow(),
    )

    try:
        session.add(raw_data)
        session.commit()
        session.refresh(raw_data)
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return {
        "data_id": raw_data.data_id,
        "hash": raw_data.data_hash,
        "sheets_processed": list(data_payload.keys()),
        "total_rows": sum(len(rows) for rows in data_payload.values())
    }


# ✅ Get All RawData for Tenant
@router.get("/{tenant_id}")
async def get_raw_data_for_tenant(tenant_id: UUID, session: Session = Depends(get_session)):
    try:
        result = session.execute(select(RawData).where(RawData.tenant_id == tenant_id))
        return result.scalars().all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Get RawData by ID
@router.get("/item/{data_id}")
async def get_raw_data(data_id: UUID, session: Session = Depends(get_session)):
    try:
        result = session.execute(select(RawData).where(RawData.data_id == data_id))
        raw_data = result.scalars().first()
        if not raw_data:
            raise HTTPException(status_code=404, detail="RawData not found")
        return raw_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Extract Column Names
@router.get("/columns/{data_id}")
async def get_raw_data_columns(data_id: UUID, session: Session = Depends(get_session)):
    try:
        result = session.execute(select(RawData).where(RawData.data_id == data_id))
        raw_data = result.scalars().first()
        if not raw_data:
            raise HTTPException(status_code=404, detail="RawData not found")

        payload = raw_data.data_payload or {}
        columns = set()

        for sheet, rows in payload.items():
            if isinstance(rows, list) and rows:
                for row in rows:
                    if isinstance(row, dict):
                        columns.update(row.keys())

        return {
            "columns": sorted(list(columns)),
            "sheets": list(payload.keys())
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing columns: {str(e)}")


# ✅ Map Columns → MappedValue
@router.post("/map/{data_id}")
async def create_mappings(
        data_id: UUID,
        mappings: Dict[str, str],  # { "ExcelColumn": "mapped_field" }
        session: Session = Depends(get_session),
):
    try:
        result = session.execute(select(RawData).where(RawData.data_id == data_id))
        raw_data = result.scalars().first()
        if not raw_data:
            raise HTTPException(status_code=404, detail="RawData not found")

        # Clear existing mappings for this data_id (optional)
        session.execute(
            select(MappedValue).where(MappedValue.data_id == data_id)
        )
        existing_mappings = session.execute(
            select(MappedValue).where(MappedValue.data_id == data_id)
        ).scalars().all()

        for mapping in existing_mappings:
            session.delete(mapping)

        created = []
        for raw_field, mapped_field in mappings.items():
            mv = MappedValue(
                tenant_id=raw_data.tenant_id,
                data_id=data_id,
                field_name=raw_field,
                mapped_value=mapped_field,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            session.add(mv)
            created.append(mv)

        session.commit()
        return {
            "message": f"Created {len(created)} mappings",
            "mapped_values": [
                {
                    "field_name": mv.field_name,
                    "mapped_value": mv.mapped_value,
                    "mapped_id": mv.mapped_id
                }
                for mv in created
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Get mappings for a data record
@router.get("/mappings/{data_id}")
async def get_mappings(data_id: UUID, session: Session = Depends(get_session)):
    try:
        result = session.execute(
            select(MappedValue).where(MappedValue.data_id == data_id)
        )
        mappings = result.scalars().all()

        return {
            "data_id": data_id,
            "mappings": [
                {
                    "mapped_id": mv.mapped_id,
                    "field_name": mv.field_name,
                    "mapped_value": mv.mapped_value,
                    "data_type": mv.data_type
                }
                for mv in mappings
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")