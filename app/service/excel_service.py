import hashlib
import logging
import re
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Any, Optional
from uuid import UUID, uuid4

import pandas as pd
from fastapi import UploadFile, HTTPException
from sqlmodel import Session, select

from ..models.excel_data import Tenant, DataSource, RawData

logger = logging.getLogger(__name__)


class ExcelService:
    def __init__(self, session: Session):
        self.session = session

    def clean_column_names(self, columns: List[str]) -> List[str]:
        """Clean column names for consistency"""
        cleaned = []
        for col in columns:
            if pd.isna(col):
                cleaned.append('unnamed_column')
            else:
                # Remove special characters and spaces
                clean_name = re.sub(r'[^\w\s]', '', str(col))
                clean_name = re.sub(r'\s+', '_', clean_name.strip())
                cleaned.append(clean_name.lower())
        return cleaned

    def generate_file_hash(self, file_content: bytes) -> str:
        """Generate MD5 hash for file content"""
        return hashlib.md5(file_content).hexdigest()

    def convert_dataframe_to_json(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert DataFrame to JSON-serializable format"""
        records = []
        for index, row in df.iterrows():
            record = {}
            for col, value in row.items():
                if pd.isna(value):
                    record[col] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    record[col] = value.isoformat()
                elif isinstance(value, (pd.Int64Dtype, pd.Float64Dtype)):
                    record[col] = float(value) if pd.notna(value) else None
                else:
                    record[col] = str(value)
            records.append(record)
        return records

    def get_or_create_tenant(self, tenant_name: str) -> Tenant:
        """Get existing tenant or create new one"""
        tenant = self.session.exec(
            select(Tenant).where(Tenant.tenant_name == tenant_name)
        ).first()

        if not tenant:
            tenant = Tenant(tenant_name=tenant_name)
            self.session.add(tenant)
            self.session.commit()
            self.session.refresh(tenant)

        return tenant

    def get_or_create_data_source(self, tenant_id: UUID, source_name: str, source_type: str = "EXCEL") -> DataSource:
        """Get existing data source or create new one"""
        data_source = self.session.exec(
            select(DataSource).where(
                DataSource.tenant_id == tenant_id,
                DataSource.source_name == source_name
            )
        ).first()

        if not data_source:
            data_source = DataSource(
                tenant_id=tenant_id,
                source_name=source_name,
                source_type=source_type
            )
            self.session.add(data_source)
            self.session.commit()
            self.session.refresh(data_source)

        return data_source

    def check_existing_file_data(self, tenant_id: UUID, file_hash: str) -> Optional[List[RawData]]:
        """Check if file data already exists based on hash"""
        existing_data = self.session.exec(
            select(RawData).where(
                RawData.tenant_id == tenant_id,
                RawData.data_payload.op('->>')('file_hash') == file_hash
            )
        ).all()

        return existing_data if existing_data else None

    def process_excel_file(self, file: UploadFile, tenant_name: str) -> Dict[str, Any]:
        """Process Excel file and store data in multi-tenant structure"""
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="File must be Excel format")

        try:
            # Read file content
            file_content = file.file.read()
            file_size = len(file_content)
            file_hash = self.generate_file_hash(file_content)

            # Get or create tenant
            tenant = self.get_or_create_tenant(tenant_name)

            # Check if file already exists for this tenant
            existing_data = self.check_existing_file_data(tenant.tenant_id, file_hash)
            if existing_data:
                return {
                    "status": "already_exists",
                    "message": "File already processed for this tenant",
                    "tenant_id": str(tenant.tenant_id),
                    "data_count": len(existing_data),
                    "uploaded_at": existing_data[0].created_timestamp.isoformat()
                }

            # Read Excel file
            excel_file = pd.ExcelFile(BytesIO(file_content))

            processed_sheets = []
            total_records_inserted = 0
            batch_id = uuid4()

            for sheet_name in excel_file.sheet_names:
                try:
                    # Read sheet
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)

                    # Skip empty sheets
                    if df.empty:
                        continue

                    # Clean column names
                    original_columns = df.columns.tolist()
                    df.columns = self.clean_column_names(original_columns)

                    # Remove completely empty rows
                    df = df.dropna(how='all')

                    if len(df) == 0:
                        continue

                    # Convert to JSON format
                    sheet_records = self.convert_dataframe_to_json(df)

                    if not sheet_records:
                        continue

                    # Create data source for this sheet
                    data_source = self.get_or_create_data_source(
                        tenant.tenant_id,
                        f"{file.filename}_{sheet_name}",
                        "EXCEL"
                    )

                    # Store each record as RawData
                    for record_index, record in enumerate(sheet_records):
                        # Add metadata to each record
                        enhanced_payload = {
                            "file_info": {
                                "original_filename": file.filename,
                                "file_hash": file_hash,
                                "file_size": file_size,
                                "sheet_name": sheet_name,
                                "record_index": record_index,
                                "total_records_in_sheet": len(sheet_records),
                                "column_mapping": {
                                    "original_columns": original_columns,
                                    "cleaned_columns": df.columns.tolist()
                                }
                            },
                            "data": record,
                            "batch_id": str(batch_id)
                        }

                        raw_data = RawData(
                            tenant_id=tenant.tenant_id,
                            source_id=data_source.source_id,
                            data_payload=enhanced_payload
                        )
                        self.session.add(raw_data)

                    total_records_inserted += len(sheet_records)
                    processed_sheets.append({
                        "sheet_name": sheet_name,
                        "records_count": len(sheet_records),
                        "columns": df.columns.tolist(),
                        "source_id": str(data_source.source_id)
                    })

                except Exception as e:
                    logger.warning(f"Error processing sheet '{sheet_name}': {str(e)}")
                    continue

            if not processed_sheets:
                raise HTTPException(status_code=400, detail="No valid data found in Excel file")

            # Commit all data
            self.session.commit()

            return {
                "status": "success",
                "tenant_id": str(tenant.tenant_id),
                "tenant_name": tenant.tenant_name,
                "file_name": file.filename,
                "file_size": file_size,
                "file_hash": file_hash,
                "batch_id": str(batch_id),
                "total_sheets_processed": len(processed_sheets),
                "total_records_inserted": total_records_inserted,
                "sheets_info": processed_sheets,
                "uploaded_at": datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.session.rollback()
            logger.error(f"Error processing Excel file: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    def get_tenant_files(self, tenant_name: str, skip: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Get all files processed for a tenant"""
        tenant = self.session.exec(
            select(Tenant).where(Tenant.tenant_name == tenant_name)
        ).first()

        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

        # Get unique files by batch_id
        raw_data = self.session.exec(
            select(RawData)
            .where(RawData.tenant_id == tenant.tenant_id)
            .offset(skip)
            .limit(limit * 10)  # Get more records to find unique files
            .order_by(RawData.created_timestamp.desc())
        ).all()

        unique_files = {}
        for data in raw_data:
            file_info = data.data_payload.get("file_info", {})
            batch_id = data.data_payload.get("batch_id")

            if batch_id not in unique_files:
                unique_files[batch_id] = {
                    "batch_id": batch_id,
                    "filename": file_info.get("original_filename"),
                    "file_hash": file_info.get("file_hash"),
                    "file_size": file_info.get("file_size"),
                    "uploaded_at": data.created_timestamp.isoformat(),
                    "sheets": set(),
                    "total_records": 0
                }

            sheet_name = file_info.get("sheet_name")
            if sheet_name:
                unique_files[batch_id]["sheets"].add(sheet_name)
            unique_files[batch_id]["total_records"] += 1

        # Convert sets to lists and limit results
        files_list = []
        for file_data in list(unique_files.values())[:limit]:
            file_data["sheets"] = list(file_data["sheets"])
            files_list.append(file_data)

        return {
            "tenant_name": tenant.tenant_name,
            "tenant_id": str(tenant.tenant_id),
            "total_files": len(files_list),
            "files": files_list
        }

    def get_file_data_by_batch(self, tenant_name: str, batch_id: str) -> Dict[str, Any]:
        """Get all data for a specific file batch"""
        tenant = self.session.exec(
            select(Tenant).where(Tenant.tenant_name == tenant_name)
        ).first()

        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

        raw_data = self.session.exec(
            select(RawData)
            .where(
                RawData.tenant_id == tenant.tenant_id,
                RawData.data_payload.op('->>')('batch_id') == batch_id
            )
            .order_by(RawData.created_timestamp.asc())
        ).all()

        if not raw_data:
            raise HTTPException(status_code=404, detail="File batch not found")

        # Group by sheets
        sheets_data = {}
        file_info = None

        for data in raw_data:
            payload = data.data_payload
            if not file_info:
                file_info = payload.get("file_info", {})

            sheet_name = payload.get("file_info", {}).get("sheet_name")
            record_data = payload.get("data", {})

            if sheet_name not in sheets_data:
                sheets_data[sheet_name] = []

            sheets_data[sheet_name].append({
                "data_id": str(data.data_id),
                "record": record_data,
                "created_at": data.created_timestamp.isoformat()
            })

        return {
            "tenant_name": tenant.tenant_name,
            "batch_id": batch_id,
            "file_info": file_info,
            "sheets": sheets_data,
            "total_records": len(raw_data)
        }

    def get_sheet_data(self, tenant_name: str, batch_id: str, sheet_name: str) -> List[Dict[str, Any]]:
        """Get data from specific sheet of a file batch"""
        tenant = self.session.exec(
            select(Tenant).where(Tenant.tenant_name == tenant_name)
        ).first()

        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

        raw_data = self.session.exec(
            select(RawData)
            .where(
                RawData.tenant_id == tenant.tenant_id,
                RawData.data_payload.op('->>')('batch_id') == batch_id,
                RawData.data_payload.op('#>>')("{file_info,sheet_name}") == sheet_name
            )
            .order_by(RawData.data_payload.op('#>>')("{file_info,record_index}").cast(int))
        ).all()

        if not raw_data:
            raise HTTPException(status_code=404, detail="Sheet data not found")

        return [
            {
                "data_id": str(data.data_id),
                "record": data.data_payload.get("data", {}),
                "record_index": data.data_payload.get("file_info", {}).get("record_index"),
                "created_at": data.created_timestamp.isoformat()
            }
            for data in raw_data
        ]

    def search_tenant_data(self, tenant_name: str, search_term: str, batch_id: Optional[str] = None) -> Dict[str, Any]:
        """Search data across tenant's files"""
        tenant = self.session.exec(
            select(Tenant).where(Tenant.tenant_name == tenant_name)
        ).first()

        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

        query = select(RawData).where(RawData.tenant_id == tenant.tenant_id)

        if batch_id:
            query = query.where(RawData.data_payload.op('->>')('batch_id') == batch_id)

        raw_data = self.session.exec(query).all()

        results = []
        for data in raw_data:
            payload = data.data_payload
            record_data = payload.get("data", {})

            # Check if search term exists in any column value
            match_found = False
            for value in record_data.values():
                if value and search_term.lower() in str(value).lower():
                    match_found = True
                    break

            if match_found:
                results.append({
                    "data_id": str(data.data_id),
                    "batch_id": payload.get("batch_id"),
                    "file_info": payload.get("file_info", {}),
                    "record": record_data,
                    "created_at": data.created_timestamp.isoformat()
                })

        return {
            "tenant_name": tenant.tenant_name,
            "search_term": search_term,
            "batch_id": batch_id,
            "total_matches": len(results),
            "results": results
        }

    def delete_file_batch(self, tenant_name: str, batch_id: str) -> Dict[str, str]:
        """Delete all data for a specific file batch"""
        tenant = self.session.exec(
            select(Tenant).where(Tenant.tenant_name == tenant_name)
        ).first()

        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

        raw_data = self.session.exec(
            select(RawData)
            .where(
                RawData.tenant_id == tenant.tenant_id,
                RawData.data_payload.op('->>')('batch_id') == batch_id
            )
        ).all()

        if not raw_data:
            raise HTTPException(status_code=404, detail="File batch not found")

        filename = raw_data[0].data_payload.get("file_info", {}).get("original_filename", "Unknown")

        for data in raw_data:
            self.session.delete(data)

        self.session.commit()

        return {
            "message": f"File batch '{filename}' (batch_id: {batch_id}) deleted successfully",
            "records_deleted": len(raw_data)
        }

    def get_tenant_statistics(self, tenant_name: str) -> Dict[str, Any]:
        """Get statistics for a tenant"""
        tenant = self.session.exec(
            select(Tenant).where(Tenant.tenant_name == tenant_name)
        ).first()

        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

        # Get all raw data for tenant
        raw_data = self.session.exec(
            select(RawData).where(RawData.tenant_id == tenant.tenant_id)
        ).all()

        # Get data sources count
        data_sources = self.session.exec(
            select(DataSource).where(DataSource.tenant_id == tenant.tenant_id)
        ).all()

        # Calculate statistics
        unique_batches = set()
        unique_files = set()
        total_sheets = set()

        for data in raw_data:
            payload = data.data_payload
            batch_id = payload.get("batch_id")
            file_hash = payload.get("file_info", {}).get("file_hash")
            sheet_name = payload.get("file_info", {}).get("sheet_name")

            if batch_id:
                unique_batches.add(batch_id)
            if file_hash:
                unique_files.add(file_hash)
            if sheet_name:
                total_sheets.add(sheet_name)

        return {
            "tenant_name": tenant.tenant_name,
            "tenant_id": str(tenant.tenant_id),
            "created_at": tenant.created_at.isoformat(),
            "statistics": {
                "total_files": len(unique_files),
                "total_batches": len(unique_batches),
                "total_sheets": len(total_sheets),
                "total_records": len(raw_data),
                "total_data_sources": len(data_sources)
            }
        }