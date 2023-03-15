from database import db_parameter_engine, db_transaction_engine, TbParameters
from sqlalchemy import text, select
from sqlalchemy.exc import SQLAlchemyError, NoResultFound, IntegrityError
from fastapi import HTTPException, status
from schemas import TbTableRead, TbParameterRead
import re

command_and_columns = {
    "select": "id_select",
    "update": "id_update",
    "insert": "id_insert",
    "delete": "id_delete",
    "drop": "id_drop",
    "truncate": "id_truncate",
    "alter": "id_alter",
    "token": "id_token",
}


async def extract_table_name(statement: str):
    pattern = re.compile(r"tb_table_(\d+)", re.IGNORECASE)
    command = statement.split(" ")[0]
    match = pattern.search(statement)
    if match:
        table_name = match.group(1)
        return f"tb_table_{table_name}", command
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
    table_name, command = await extract_table_name(sql_statement)
    if command.lower() == "select":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{command} is not allowed on this endpoint",
        )
    db_parameter_data = await get_db_parameter(table_name)
    column_value = db_parameter_data[command_and_columns[command.lower()]]
    if column_value == "no":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"You are not allowed to execute {command} command on this table",
        )
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
    table_name, command = await extract_table_name(sql_statement)
    if command.lower() != "select":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{command} is not allowed on this endpoint",
        )
    db_parameter_data = await get_db_parameter(table_name)  # type: ignore
    column_value = db_parameter_data[command_and_columns[command.lower()]]
    if column_value == "no":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"You are not allowed to execute {command} command on this table",
        )
    async with db_transaction_engine.begin() as connection:
        try:
            results = await connection.execute(statement)  # type: ignore
        except SQLAlchemyError as msg:
            await connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Something went wrong",
            )
    db_data = [dict(zip(results.keys(), row)) for row in results]

    return db_data
