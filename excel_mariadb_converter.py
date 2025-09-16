from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData, inspect
import re
from typing import Dict, List, Tuple, Any, Optional
import numpy as np
from io import BytesIO
import logging
from dataclasses import dataclass
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Excel to MariaDB Converter", version="1.0.0")


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    references_table: Optional[str] = None
    references_column: Optional[str] = None
    max_length: Optional[int] = None


@dataclass
class TableInfo:
    name: str
    columns: List[ColumnInfo]
    primary_key: Optional[str] = None


class ExcelAnalyzer:
    """Analyzes Excel files to extract structure and relationships"""

    def __init__(self):
        self.sheets_data = {}
        self.table_info = {}

    def read_excel_file(self, file_content: bytes) -> Dict[str, pd.DataFrame]:
        """Read all sheets from Excel file"""
        try:
            excel_file = pd.ExcelFile(BytesIO(file_content))
            sheets_data = {}

            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                # Clean column names
                df.columns = [self._clean_column_name(col) for col in df.columns]
                # Remove completely empty rows and columns
                df = df.dropna(how='all').dropna(axis=1, how='all')
                if not df.empty:
                    sheets_data[self._clean_table_name(sheet_name)] = df

            self.sheets_data = sheets_data
            return sheets_data

        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading Excel file: {str(e)}")

    def _clean_table_name(self, name: str) -> str:
        """Clean table name for SQL compatibility"""
        # Remove special characters and replace with underscore
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        # Ensure it starts with a letter
        if clean_name[0].isdigit():
            clean_name = 'table_' + clean_name
        return clean_name.lower()

    def _clean_column_name(self, name: str) -> str:
        """Clean column name for SQL compatibility"""
        if pd.isna(name):
            return 'unnamed_column'
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        if clean_name[0].isdigit():
            clean_name = 'col_' + clean_name
        return clean_name.lower()

    def _infer_data_type(self, series: pd.Series) -> Tuple[str, Optional[int]]:
        """Infer MariaDB data type from pandas series"""
        # Remove null values for analysis
        non_null_series = series.dropna()

        if len(non_null_series) == 0:
            return "TEXT", None

        # Check for numeric types
        if pd.api.types.is_integer_dtype(series):
            max_val = non_null_series.max()
            min_val = non_null_series.min()

            if min_val >= -128 and max_val <= 127:
                return "TINYINT", None
            elif min_val >= -32768 and max_val <= 32767:
                return "SMALLINT", None
            elif min_val >= -2147483648 and max_val <= 2147483647:
                return "INT", None
            else:
                return "BIGINT", None

        elif pd.api.types.is_float_dtype(series):
            return "DECIMAL(10,2)", None

        elif pd.api.types.is_datetime64_any_dtype(series):
            return "DATETIME", None

        elif pd.api.types.is_bool_dtype(series):
            return "BOOLEAN", None

        else:
            # String type - determine length
            max_length = non_null_series.astype(str).str.len().max()

            if max_length <= 255:
                return "VARCHAR", min(max_length + 50, 255)  # Add some buffer
            else:
                return "TEXT", None

    def _detect_primary_key(self, df: pd.DataFrame, table_name: str) -> Optional[str]:
        """Detect potential primary key column"""
        for col in df.columns:
            # Check if column has unique values and no nulls
            if df[col].nunique() == len(df) and df[col].notna().all():
                # Prefer columns with 'id' in name
                if 'id' in col.lower():
                    return col

        # If no obvious primary key, look for unique columns
        for col in df.columns:
            if df[col].nunique() == len(df) and df[col].notna().all():
                return col

        return None

    def _get_unique_columns(self, df: pd.DataFrame) -> List[str]:
        """Get list of columns that have unique values (potential reference targets)"""
        unique_cols = []
        for col in df.columns:
            if df[col].nunique() == len(df) and df[col].notna().all():
                unique_cols.append(col)
        return unique_cols

    def _detect_foreign_keys(self) -> Dict[str, List[Tuple[str, str, str]]]:
        """Detect potential foreign key relationships between tables"""
        relationships = defaultdict(list)
        tables = list(self.sheets_data.keys())

        # First, identify which columns in each table are unique (can be referenced)
        unique_columns = {}
        for table_name, df in self.sheets_data.items():
            unique_columns[table_name] = self._get_unique_columns(df)

        for i, table1 in enumerate(tables):
            for j, table2 in enumerate(tables):
                if i != j:  # Don't compare table with itself
                    df1 = self.sheets_data[table1]
                    df2 = self.sheets_data[table2]

                    for col1 in df1.columns:
                        # Only check against unique columns in the target table
                        for col2 in unique_columns[table2]:
                            if self._check_potential_relationship(df1[col1], df2[col2], col1, col2, table2):
                                relationships[table1].append((col1, table2, col2))

        return dict(relationships)

    def _check_potential_relationship(self, series1: pd.Series, series2: pd.Series,
                                      col1: str, col2: str, target_table: str) -> bool:
        """Check if two columns might have a foreign key relationship"""
        # Remove nulls
        s1_clean = series1.dropna()
        s2_clean = series2.dropna()

        if len(s1_clean) == 0 or len(s2_clean) == 0:
            return False

        # Skip if data types are completely incompatible
        if not self._are_types_compatible(s1_clean, s2_clean):
            return False

        try:
            # Convert to same type for comparison
            s1_str = s1_clean.astype(str)
            s2_str = s2_clean.astype(str)

            # Calculate overlap
            s1_unique = set(s1_str.unique())
            s2_unique = set(s2_str.unique())
            overlap = s1_unique & s2_unique

            # Check if this is a valid foreign key relationship
            # All values in s1 should exist in s2 for a proper foreign key
            all_values_exist = s1_unique.issubset(s2_unique)
            overlap_percentage = len(overlap) / len(s1_unique) if len(s1_unique) > 0 else 0

            # Consider it a relationship if:
            # 1. All values in source exist in target (proper foreign key) OR
            # 2. High overlap (>80%) AND column names suggest relationship
            if (all_values_exist or
                    (overlap_percentage > 0.8 and self._names_suggest_relationship(col1, col2, target_table))):
                return True

        except Exception as e:
            logger.warning(f"Error checking relationship {col1}->{col2}: {str(e)}")

        return False

    def _are_types_compatible(self, series1: pd.Series, series2: pd.Series) -> bool:
        """Check if two series have compatible types for foreign key relationship"""
        try:
            # Try to convert both to string and see if they're comparable
            s1_str = series1.astype(str)
            s2_str = series2.astype(str)
            return True
        except Exception:
            return False

    def _names_suggest_relationship(self, col1: str, col2: str, target_table: str) -> bool:
        """Check if column names suggest a relationship"""
        col1_lower = col1.lower()
        col2_lower = col2.lower()
        target_lower = target_table.lower()

        # Pattern matching for foreign keys
        patterns = [
            # Direct table name match with id suffix
            col1_lower == f"{target_lower}_id",
            col1_lower == f"{target_lower}id",

            # Column ends with _id and contains table name
            col1_lower.endswith('_id') and target_lower in col1_lower,

            # Target column is 'id' and source contains table name
            col2_lower == 'id' and target_lower in col1_lower,

            # Same column names (like course_id -> course_id)
            col1_lower == col2_lower and col1_lower.endswith('_id'),

            # Course name -> course name type relationships
            col1_lower == col2_lower and 'name' in col1_lower
        ]
        return any(patterns)

    def analyze_structure(self) -> Dict[str, TableInfo]:
        """Analyze Excel structure and create table information"""
        if not self.sheets_data:
            raise ValueError("No data to analyze. Please read Excel file first.")

        # Detect relationships
        relationships = self._detect_foreign_keys()

        table_info = {}

        for table_name, df in self.sheets_data.items():
            columns = []
            primary_key = self._detect_primary_key(df, table_name)

            for col_name in df.columns:
                data_type, max_length = self._infer_data_type(df[col_name])

                # Check if this column is a foreign key
                is_fk = False
                ref_table = None
                ref_column = None

                if table_name in relationships:
                    for fk_col, ref_tbl, ref_col in relationships[table_name]:
                        if fk_col == col_name:
                            is_fk = True
                            ref_table = ref_tbl
                            ref_column = ref_col
                            break

                column_info = ColumnInfo(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=bool(df[col_name].isna().any()),
                    is_primary_key=(col_name == primary_key),
                    is_foreign_key=is_fk,
                    references_table=ref_table,
                    references_column=ref_column,
                    max_length=max_length
                )
                columns.append(column_info)

            table_info[table_name] = TableInfo(
                name=table_name,
                columns=columns,
                primary_key=primary_key
            )

        self.table_info = table_info
        return table_info


class MariaDBManager:
    """Manages MariaDB database operations"""

    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.connection_string)

    def create_tables(self, table_info: Dict[str, TableInfo]) -> List[str]:
        """Create tables in MariaDB based on table information"""
        sql_statements = []

        try:
            with self.engine.connect() as conn:
                # Create tables without foreign keys first
                for table_name, info in table_info.items():
                    create_sql = self._generate_create_table_sql(info, include_foreign_keys=False)
                    sql_statements.append(create_sql)
                    conn.execute(text(create_sql))
                    logger.info(f"Created table: {table_name}")

                # Add unique constraints for columns that will be referenced by foreign keys
                unique_constraints = self._generate_unique_constraints(table_info)
                for constraint_sql in unique_constraints:
                    sql_statements.append(constraint_sql)
                    try:
                        conn.execute(text(constraint_sql))
                        logger.info(f"Added unique constraint")
                    except Exception as e:
                        logger.warning(f"Could not add unique constraint: {str(e)}")

                # Add foreign key constraints after all tables are created
                for table_name, info in table_info.items():
                    fk_statements = self._generate_foreign_key_sql(info)
                    for fk_sql in fk_statements:
                        sql_statements.append(fk_sql)
                        try:
                            conn.execute(text(fk_sql))
                            logger.info(f"Added foreign key constraint to {table_name}")
                        except Exception as e:
                            logger.warning(f"Could not add foreign key constraint: {str(e)}")

                conn.commit()

        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise

        return sql_statements

    def _generate_unique_constraints(self, table_info: Dict[str, TableInfo]) -> List[str]:
        """Generate unique constraints for columns that are referenced by foreign keys"""
        constraints = []
        referenced_columns = set()

        # Find all columns that are referenced by foreign keys
        for table_name, info in table_info.items():
            for col in info.columns:
                if col.is_foreign_key and col.references_table and col.references_column:
                    # Don't add unique constraint if it's already a primary key
                    ref_table_info = table_info.get(col.references_table)
                    if ref_table_info:
                        for ref_col in ref_table_info.columns:
                            if ref_col.name == col.references_column and not ref_col.is_primary_key:
                                key = f"{col.references_table}.{col.references_column}"
                                if key not in referenced_columns:
                                    referenced_columns.add(key)
                                    constraint_sql = f"ALTER TABLE `{col.references_table}` ADD UNIQUE KEY `unique_{col.references_table}_{col.references_column}` (`{col.references_column}`)"
                                    constraints.append(constraint_sql)

        return constraints

    def _generate_create_table_sql(self, table_info: TableInfo, include_foreign_keys: bool = True) -> str:
        """Generate CREATE TABLE SQL statement"""
        columns_sql = []

        for col in table_info.columns:
            col_sql = f"`{col.name}` {col.data_type}"

            if col.max_length and col.data_type == "VARCHAR":
                col_sql = f"`{col.name}` {col.data_type}({col.max_length})"

            if not col.is_nullable:
                col_sql += " NOT NULL"

            if col.is_primary_key:
                col_sql += " PRIMARY KEY"
                if col.data_type in ["INT", "BIGINT", "SMALLINT", "TINYINT"]:
                    col_sql += " AUTO_INCREMENT"

            columns_sql.append(col_sql)

        sql = f"CREATE TABLE IF NOT EXISTS `{table_info.name}` (\n"
        sql += ",\n".join(f"  {col}" for col in columns_sql)
        sql += "\n)"

        return sql

    def _generate_foreign_key_sql(self, table_info: TableInfo) -> List[str]:
        """Generate ALTER TABLE statements for foreign keys"""
        fk_statements = []

        for col in table_info.columns:
            if col.is_foreign_key and col.references_table and col.references_column:
                fk_name = f"fk_{table_info.name}_{col.name}"
                sql = f"""
                ALTER TABLE `{table_info.name}` 
                ADD CONSTRAINT `{fk_name}` 
                FOREIGN KEY (`{col.name}`) 
                REFERENCES `{col.references_table}`(`{col.references_column}`)
                ON DELETE SET NULL ON UPDATE CASCADE
                """
                fk_statements.append(sql.strip())

        return fk_statements

    def insert_data(self, sheets_data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Insert data into tables"""
        insert_counts = {}

        try:
            with self.engine.connect() as conn:
                for table_name, df in sheets_data.items():
                    # Clean data before insertion
                    df_clean = df.replace({np.nan: None})

                    # Insert data
                    df_clean.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
                    insert_counts[table_name] = len(df_clean)
                    logger.info(f"Inserted {len(df_clean)} rows into {table_name}")

                conn.commit()

        except Exception as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise

        return insert_counts


# FastAPI endpoints
@app.post("/upload-excel/")
async def upload_excel(
        file: UploadFile = File(...),
        db_host: str = "localhost",
        db_user: str = "root",
        db_password: str = "password",
        db_name: str = "excel_import",
        db_port: int = 3306
):
    """
    Upload Excel file and create MariaDB database with relationships
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")

    try:
        # Read file content
        file_content = await file.read()

        # Initialize analyzer
        analyzer = ExcelAnalyzer()

        # Read and analyze Excel file
        sheets_data = analyzer.read_excel_file(file_content)
        table_info = analyzer.analyze_structure()

        # Initialize MariaDB manager
        db_manager = MariaDBManager(db_host, db_user, db_password, db_name, db_port)

        # Create tables
        sql_statements = db_manager.create_tables(table_info)

        # Insert data
        insert_counts = db_manager.insert_data(sheets_data)

        # Prepare response
        response_data = {
            "status": "success",
            "message": "Excel file processed and database created successfully",
            "database_info": {
                "database_name": db_name,
                "tables_created": len(table_info),
                "total_rows_inserted": sum(insert_counts.values())
            },
            "tables": {}
        }

        # Add detailed table information
        for table_name, info in table_info.items():
            response_data["tables"][table_name] = {
                "columns": len(info.columns),
                "primary_key": info.primary_key,
                "rows_inserted": insert_counts.get(table_name, 0),
                "column_details": [
                    {
                        "name": col.name,
                        "type": col.data_type,
                        "nullable": col.is_nullable,
                        "primary_key": col.is_primary_key,
                        "foreign_key": col.is_foreign_key,
                        "references": f"{col.references_table}.{col.references_column}" if col.is_foreign_key else None
                    }
                    for col in info.columns
                ]
            }

        return JSONResponse(content=response_data)

    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/analyze-excel/")
async def analyze_excel(file: UploadFile = File(...)):
    """
    Analyze Excel file structure without creating database
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="File must be Excel format (.xlsx or .xls)")

    try:
        file_content = await file.read()
        analyzer = ExcelAnalyzer()

        sheets_data = analyzer.read_excel_file(file_content)
        table_info = analyzer.analyze_structure()

        response_data = {
            "status": "success",
            "analysis": {
                "sheets_found": len(sheets_data),
                "tables": {}
            }
        }

        for table_name, info in table_info.items():
            response_data["analysis"]["tables"][table_name] = {
                "row_count": len(sheets_data[table_name]),
                "column_count": len(info.columns),
                "primary_key": info.primary_key,
                "columns": [
                    {
                        "name": col.name,
                        "type": col.data_type,
                        "nullable": col.is_nullable,
                        "primary_key": col.is_primary_key,
                        "foreign_key": col.is_foreign_key,
                        "references": f"{col.references_table}.{col.references_column}" if col.is_foreign_key else None
                    }
                    for col in info.columns
                ]
            }

        return JSONResponse(content=response_data)

    except Exception as e:
        logger.error(f"Error analyzing Excel file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error analyzing file: {str(e)}")


@app.get("/")
async def root():
    return {
        "message": "Excel to MariaDB Converter API",
        "endpoints": {
            "POST /upload-excel/": "Upload Excel file and create MariaDB database",
            "POST /analyze-excel/": "Analyze Excel file structure without creating database",
            "GET /": "This help message"
        }
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)