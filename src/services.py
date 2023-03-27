from typing import Optional
from database import db_parameter_engine, db_transaction_engine, TbParameters
from sqlalchemy import text, select, inspect
from sqlalchemy.exc import SQLAlchemyError, NoResultFound, IntegrityError, NoSuchTableError, ProgrammingError
from fastapi import HTTPException, status
from schemas import TbParameterRead, LoginData
from utils import (
    create_access_tokens,
    encrypt_otp_with_md5,
    decrypt_access_token,
    command_and_columns,
    patterns
)

async def extract_table_name(statement: str):
    command = statement.split(" ")[0]
    pattern = patterns.get(command.lower())  # type: ignore
    match = pattern.search(statement)  # type: ignore
    if match:
        table_name = match.group(1)
        return f"{table_name}", command
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Could not get table name"
        )


async def get_db_parameter(table_name: str):
    statement = select(TbParameters).where(TbParameters.tablename == table_name)
    async with db_parameter_engine.connect() as connection:
        try:
            results = await connection.execute(statement)
            data_db = results.fetchone()  # type: ignore
            if data_db is None:
                raise HTTPException(404, detail=f"Table {table_name} not found")
        except SQLAlchemyError:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Something went wrong",
            )

    db_data = {
        **TbParameterRead.parse_obj(
            dict(zip(TbParameterRead.__fields__.keys(), data_db))  # type: ignore
        ).dict()
    }

    return db_data


async def execute_sql_command(
    sql_statement: str, authorization_token: Optional[str] = None
):
    statement = text(sql_statement)
    table_name, command = await extract_table_name(sql_statement)
    if command.lower() == "select":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{command} is not allowed on this endpoint",
        )
    db_parameter_data = await get_db_parameter(table_name)
    column_value = db_parameter_data[command_and_columns[command.lower()]]
    id_token = db_parameter_data[command_and_columns["token"]]
    if id_token == "yes":
        phone = await decrypt_access_token(authorization=authorization_token)
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


async def execute_select_sql_command(
    sql_statement: str, authorization_token: Optional[str] = None
):
    statement = text(sql_statement)
    table_name, command = await extract_table_name(sql_statement)
    if command.lower() != "select":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{command} is not allowed on this endpoint",
        )
    db_parameter_data = await get_db_parameter(table_name)  # type: ignore
    column_value = db_parameter_data[command_and_columns[command.lower()]]
    id_token = db_parameter_data[command_and_columns["token"]]
    if id_token == "yes":
        phone = await decrypt_access_token(authorization=authorization_token)
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
            if isinstance(msg, ProgrammingError):
                raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Something went wrong, probably table was not found.",
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Something went wrong.",
            )
    db_data = [dict(zip(results.keys(), row)) for row in results]

    return db_data


async def login_user(data: LoginData):
    statement = text(f"SELECT phone, otp FROM tb_user WHERE phone = '{data.phone}'")
    async with db_transaction_engine.begin() as connection:
        try:
            results = await connection.execute(statement)  # type: ignore
            db_user = results.fetchone()
        except NoResultFound:
            await connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"User not found"
            )
        except SQLAlchemyError as msg:
            await connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Something went wrong",
            )

    user = LoginData(**dict(zip(results.keys(), db_user)))  # type: ignore
    encrypted_otp = await encrypt_otp_with_md5(data.otp)
    if user.otp != encrypted_otp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=f"Incorrect otp"
        )
    access_token, expire_time = await create_access_tokens(user)
    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": expire_time,
    }

async def view_db_tables():
    async with db_transaction_engine.begin() as connection:
        try:
            
            tables = await connection.run_sync(
                lambda sync_conn: inspect(sync_conn).get_table_names()
            )
            return tables
        except SQLAlchemyError:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unable to retrive table names",
            )
        
async def view_table_columns(table_name: str):
    async with db_transaction_engine.begin() as connection:
        try:
            
            columns = await connection.run_sync(
                lambda sync_conn: inspect(sync_conn).get_columns(table_name)
            )
            db_columns = [{"name" : column.get("name"), "type": str(column.get("type"))} for column in columns]
            return db_columns
        except SQLAlchemyError as msg:
            if isinstance(msg, NoSuchTableError):
                raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Table not found",
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unable to retrive table columns",
            )