import datetime
from typing import Optional
from database import db_parameter_engine, db_transaction_engine, TbParameters
from sqlalchemy import text, select
from sqlalchemy.exc import SQLAlchemyError, NoResultFound, IntegrityError
from asyncpg.exceptions import UndefinedTableError
from fastapi import HTTPException, status
from schemas import TbParameterRead, LoginData
import re
from utils import (
    create_access_tokens,
    encrypt_otp_with_md5,
    decrypt_access_token,
    generate_random_data,
)
from docxtpl import DocxTemplate
from functools import reduce

patterns = {
    "select": re.compile(r"[Ss][Ee][Ll][Ee][Cc][Tt].+[Ff][Rr][Oo][Mm]\s+([\w.]+)", re.IGNORECASE),
    "update": re.compile(r"[Uu][Pp][Dd][Aa][Tt][Ee]\s+([\w.]+)", re.IGNORECASE),
    "insert": re.compile(r"[Ii][Nn][Tt][Oo]\s+([\w.]+)", re.IGNORECASE),
    "delete": re.compile(
        r"[Dd][Ee][Ll][Ee][Tt][Ee]\s+[Ff][Rr][Oo][Mm]\s+([\w.]+)", re.IGNORECASE
    ),
    "alter": re.compile(
        r"[Aa][Ll][Tt][Ee][Rr]\s+[Tt][Aa][Bb][Ll][Ee]\s+([\w.]+)", re.IGNORECASE
    ),
    "truncate": re.compile(
        r"[Tt][Rr][Uu][Nn][Cc][Aa][Tt][Ee]\s+[Tt][Aa][Bb][Ll][Ee]\s+([\w.]+)",
        re.IGNORECASE,
    ),
    "drop": re.compile(
        r"[Dd][Rr][Oo][Pp]\s+[Tt][Aa][Bb][Ll][Ee]\s+([\w.]+)", re.IGNORECASE
    ),
}

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
    command = statement.split(" ")[0]
    pattern = patterns.get(command.lower())  # type: ignore
    match = pattern.search(statement)  # type: ignore
    if match:
        table_name = match.group(1)
        print(table_name)
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
        except SQLAlchemyError as msg:
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
        except UndefinedTableError:
            await connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Table not found",
            )
        except SQLAlchemyError as msg:
            await connection.rollback()
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


async def generate_report():
    # statement = text(f"SELECT * FROM tb_table_02")
    # async with db_transaction_engine.begin() as connection:
    #     try:
    #         results = await connection.execute(statement)  # type: ignore
    #     except NoResultFound:
    #         await connection.rollback()
    #         raise HTTPException(
    #             status_code=status.HTTP_404_NOT_FOUND, detail=f"User not found"
    #         )
    #     except SQLAlchemyError as msg:
    #         await connection.rollback()
    #         raise HTTPException(
    #             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    #             detail="Something went wrong",
    #         )
    # db_data = [dict(zip(results.keys(), row)) for row in results]
    db_data = generate_random_data()
    total_price = reduce(lambda x, y: x + y["xprice"], db_data, 0)
    template = DocxTemplate("src/templates/template_1.docx")
    context = {
        "timestamp": datetime.datetime.now(),
        "database_name": "db_transaction",
        "results": db_data,
        "total_price": total_price,
    }
    try:
        template.render(context)
        template.save("src/reports/report_01.docx")
        return True
    except Exception as msg:
        print(msg)
        raise HTTPException(500, detail="Unable to generate report")
