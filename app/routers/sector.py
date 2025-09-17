from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from uuid import UUID
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from sqlmodel import Session

from app.database.connection import get_session
from app.models.excel_data import Sector, DefaultField

router = APIRouter(prefix="/sectors", tags=["Sectors & Default Fields"])


# ✅ Pydantic Models
class CreateSectorRequest(BaseModel):
    sector_name: str


class CreateDefaultFieldRequest(BaseModel):
    field_name: str
    description: Optional[str] = None
    data_type: Optional[str] = "string"


class SectorResponse(BaseModel):
    sector_id: UUID
    sector_name: str
    created_at: datetime
    default_fields_count: int


class DefaultFieldResponse(BaseModel):
    field_id: UUID
    field_name: str
    description: Optional[str]
    data_type: str
    created_at: datetime


# ✅ Get All Sectors
@router.get("/", response_model=List[SectorResponse])
async def get_all_sectors(session: Session = Depends(get_session)):
    """Get all sectors with default fields count"""
    try:
        result = session.execute(
            select(Sector).options(selectinload(Sector.default_fields))
        )
        sectors = result.scalars().all()

        return [
            SectorResponse(
                sector_id=sector.sector_id,
                sector_name=sector.sector_name,
                created_at=sector.created_at,
                default_fields_count=len(sector.default_fields)
            )
            for sector in sectors
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Create New Sector
@router.post("/", response_model=SectorResponse)
async def create_sector(
        sector_request: CreateSectorRequest,
        session: Session = Depends(get_session)
):
    """Create a new sector"""
    try:
        # Check if sector name already exists
        existing = session.execute(
            select(Sector).where(Sector.sector_name == sector_request.sector_name)
        ).scalars().first()

        if existing:
            raise HTTPException(
                status_code=409,
                detail=f"Sector '{sector_request.sector_name}' already exists"
            )

        sector = Sector(
            sector_name=sector_request.sector_name,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

        session.add(sector)
        session.commit()
        session.refresh(sector)

        return SectorResponse(
            sector_id=sector.sector_id,
            sector_name=sector.sector_name,
            created_at=sector.created_at,
            default_fields_count=0
        )

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Get Sector with Default Fields
@router.get("/{sector_id}")
async def get_sector_with_fields(
        sector_id: UUID,
        session: Session = Depends(get_session)
):
    """Get a specific sector with all its default fields"""
    try:
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
            "created_at": sector.created_at,
            "updated_at": sector.updated_at,
            "default_fields": [
                DefaultFieldResponse(
                    field_id=field.field_id,
                    field_name=field.field_name,
                    description=field.description,
                    data_type=field.data_type,
                    created_at=field.created_at
                )
                for field in sector.default_fields
            ]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Add Default Field to Sector
@router.post("/{sector_id}/fields", response_model=DefaultFieldResponse)
async def add_default_field_to_sector(
        sector_id: UUID,
        field_request: CreateDefaultFieldRequest,
        session: Session = Depends(get_session)
):
    """Add a new default field to a sector"""
    try:
        # Verify sector exists
        sector_result = session.execute(
            select(Sector).where(Sector.sector_id == sector_id)
        )
        sector = sector_result.scalars().first()
        if not sector:
            raise HTTPException(status_code=404, detail="Sector not found")

        # Check if field name already exists in this sector
        existing_field = session.execute(
            select(DefaultField).where(
                DefaultField.sector_id == sector_id,
                DefaultField.field_name == field_request.field_name
            )
        ).scalars().first()

        if existing_field:
            raise HTTPException(
                status_code=409,
                detail=f"Field '{field_request.field_name}' already exists in sector '{sector.sector_name}'"
            )

        default_field = DefaultField(
            sector_id=sector_id,
            field_name=field_request.field_name,
            description=field_request.description,
            data_type=field_request.data_type,
            created_at=datetime.utcnow()
        )

        session.add(default_field)
        session.commit()
        session.refresh(default_field)

        return DefaultFieldResponse(
            field_id=default_field.field_id,
            field_name=default_field.field_name,
            description=default_field.description,
            data_type=default_field.data_type,
            created_at=default_field.created_at
        )

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Update Default Field
@router.put("/fields/{field_id}", response_model=DefaultFieldResponse)
async def update_default_field(
        field_id: UUID,
        field_request: CreateDefaultFieldRequest,
        session: Session = Depends(get_session)
):
    """Update an existing default field"""
    try:
        result = session.execute(
            select(DefaultField).where(DefaultField.field_id == field_id)
        )
        field = result.scalars().first()

        if not field:
            raise HTTPException(status_code=404, detail="Default field not found")

        # Update fields
        field.field_name = field_request.field_name
        field.description = field_request.description
        field.data_type = field_request.data_type

        session.commit()
        session.refresh(field)

        return DefaultFieldResponse(
            field_id=field.field_id,
            field_name=field.field_name,
            description=field.description,
            data_type=field.data_type,
            created_at=field.created_at
        )

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Delete Default Field
@router.delete("/fields/{field_id}")
async def delete_default_field(
        field_id: UUID,
        session: Session = Depends(get_session)
):
    """Delete a default field"""
    try:
        result = session.execute(
            select(DefaultField).where(DefaultField.field_id == field_id)
        )
        field = result.scalars().first()

        if not field:
            raise HTTPException(status_code=404, detail="Default field not found")

        session.delete(field)
        session.commit()

        return {"message": f"Default field '{field.field_name}' deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ✅ Bulk Create Default Fields for Sector
@router.post("/{sector_id}/fields/bulk")
async def bulk_create_default_fields(
        sector_id: UUID,
        fields: List[CreateDefaultFieldRequest],
        session: Session = Depends(get_session)
):
    """Create multiple default fields for a sector at once"""
    try:
        # Verify sector exists
        sector_result = session.execute(
            select(Sector).where(Sector.sector_id == sector_id)
        )
        sector = sector_result.scalars().first()
        if not sector:
            raise HTTPException(status_code=404, detail="Sector not found")

        # Get existing field names to avoid duplicates
        existing_fields = session.execute(
            select(DefaultField.field_name).where(DefaultField.sector_id == sector_id)
        ).scalars().all()
        existing_names = set(existing_fields)

        created_fields = []
        skipped_fields = []

        for field_req in fields:
            if field_req.field_name in existing_names:
                skipped_fields.append(field_req.field_name)
                continue

            default_field = DefaultField(
                sector_id=sector_id,
                field_name=field_req.field_name,
                description=field_req.description,
                data_type=field_req.data_type,
                created_at=datetime.utcnow()
            )

            session.add(default_field)
            created_fields.append(default_field)
            existing_names.add(field_req.field_name)  # Prevent duplicates within the batch

        session.commit()

        # Refresh all created fields
        for field in created_fields:
            session.refresh(field)

        return {
            "sector_id": sector_id,
            "sector_name": sector.sector_name,
            "created_count": len(created_fields),
            "skipped_count": len(skipped_fields),
            "created_fields": [
                DefaultFieldResponse(
                    field_id=field.field_id,
                    field_name=field.field_name,
                    description=field.description,
                    data_type=field.data_type,
                    created_at=field.created_at
                )
                for field in created_fields
            ],
            "skipped_fields": skipped_fields
        }

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")