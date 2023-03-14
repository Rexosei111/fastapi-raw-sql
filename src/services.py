from database import db_parameter_engine, db_transaction_engine, TbParameters
from sqlalchemy import text, select
from sqlalchemy.exc import SQLAlchemyError, NoResultFound, IntegrityError
from fastapi import HTTPException, status
from schemas import TbTableRead, TbParameterRead
import re


async def extract_table_name(statement: str):
    pattern = re.compile(r"from\s+(\w+)", re.IGNORECASE)
    match = pattern.search(statement)
    if match:
        table_name = match.group(1)
        return table_name
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Could not get table name"
        )


async def get_db_parameter(table_name: str):
    statement = select(TbParameters).where(TbParameters.tablename == table_name)
    async with db_parameter_engine.connect() as connection:
        try:
            results = await connection.execute(statement)  # type: ignore
        except NoResultFound:
            results = None
        except SQLAlchemyError as msg:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Something went wrong",
            )
    if results is None:
        return {}

    db_data = [
        {
            **TbParameterRead.parse_obj(
                dict(zip(TbParameterRead.__fields__.keys(), row))
            ).dict()
        }
        for row in results
    ]
    return db_data[0]


async def execute_sql_command(sql_statement: str):
    statement = text(sql_statement)
    async with db_transaction_engine.begin() as connection:  # type: ignore
        try:
            await connection.execute(statement)
        except IntegrityError:
            await connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Entry already exist"
            )
        except SQLAlchemyError:
            await connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Something went wrong",
            )
    return True


async def execute_select_sql_command(sql_statement: str):
    statement = text(sql_statement)
    async with db_transaction_engine.begin() as connection:
        try:
            results = await connection.execute(statement)  # type: ignore
        except SQLAlchemyError as msg:
            await connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Something went wrong",
            )
    table_name = await extract_table_name(sql_statement)
    db_parameter_data = await get_db_parameter(table_name)
    db_data = [
        {
            **TbTableRead.parse_obj(
                dict(zip(TbTableRead.__fields__.keys(), row))
            ).dict(),
            "tb_parameter": db_parameter_data,
        }
        for row in results
    ]
    return db_data
