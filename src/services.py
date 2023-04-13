import datetime
from functools import reduce
import os
from typing import Optional
from database import db_parameter_engine, db_transaction_engine, TbParameters
from sqlalchemy import text, select, inspect
from sqlalchemy.exc import (
    SQLAlchemyError,
    NoResultFound,
    IntegrityError,
    NoSuchTableError,
    ProgrammingError,
)
from fastapi import HTTPException, status
from schemas import ReqBody, TbParameterRead, LoginData
from docxtpl import DocxTemplate
from utils import (
    create_access_tokens,
    encrypt_otp_with_md5,
    decrypt_access_token,
    command_and_columns,
    patterns,
)
from fastapi.responses import FileResponse
from docxtpl import InlineImage
from docx.shared import Mm
import re


async def extract_table_name(statement: str):
    # command = statement.split(" ")[0]
    transformed_statement = statement.replace("\n", " ")
    match = re.match(r"^\w+", transformed_statement)
    command = match.group()
    pattern = patterns.get(command.lower())  # type: ignore
    if not pattern:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Could not get table name"
        )
    match = pattern.search(transformed_statement)  # type: ignore
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
            db_columns = [
                {"name": column.get("name"), "type": str(column.get("type"))}
                for column in columns
            ]
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


async def generate_report(data: ReqBody):
    req_data = data.dict(exclude_none=True, exclude_unset=True)
    base_template_path = os.path.join("src", "templates")
    base_report_path = os.path.join("src", "reports")
    template = None
    context = {
        "timestamp": datetime.datetime.now(),
        "database_name": "db_transaction",
    }
    if "sqltest" in req_data.keys():
        statement = text(req_data.get("sqltest"))  # type: ignore
        async with db_transaction_engine.begin() as connection:
            try:
                results = await connection.execute(statement)  # type: ignore
            except SQLAlchemyError as msg:
                await connection.rollback()
                if isinstance(msg, NoSuchTableError):
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Table not found",
                    )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Something went wrong",
                )
        db_data = [dict(zip(results.keys(), row)) for row in results]
        total_price = reduce(lambda x, y: x + y["xprice"], db_data, 0)
        total_value = reduce(lambda x, y: x + y.get("xint", 0), db_data, 0)
        template = DocxTemplate(f"{os.path.join(base_template_path, req_data.get('nametemplate') + '.docx')}")  # type: ignore
        context = {
            **context,
            "results": db_data,
            "total_price": total_price,
            "total_value": total_value,
            "qr_code": InlineImage(
                template,
                os.path.join("src", "qr_code_image.jpg"),
                width=Mm(60),
                height=Mm(60),
            ),
        }

    if "sqltestmaster" in req_data.keys():
        master_statement = text(req_data.get("sqltestmaster"))  # type: ignore
        detail_statement = text(req_data.get("sqltestdetail"))  # type: ignore
        async with db_transaction_engine.begin() as connection:
            try:
                results = await connection.execute(master_statement)  # type: ignore
                invoice = results.fetchone()
                if invoice is None:
                    raise HTTPException(404, detail="Invoice not found")
                db_invoice = dict(zip(results.keys(), invoice))
                results = await connection.execute(detail_statement)  # type: ignore

            except SQLAlchemyError as msg:
                await connection.rollback()
                if isinstance(msg, NoSuchTableError):
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Table not found",
                    )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Something went wrong",
                )
        # type: ignore
        db_data = [dict(zip(results.keys(), row)) for row in results]
        total_value = reduce(lambda x, y: x + y.get("subtotal", 0), db_data, 0)
        template = DocxTemplate(f"{os.path.join(base_template_path, req_data.get('nametemplate') + '.docx')}")  # type: ignore
        context = {
            **context,
            "id_invoice": db_invoice.get("id_invoice"),
            "namecustumer": db_invoice.get("namecustumer"),
            "details": db_data,
            "total": total_value,
        }

    try:
        if template is None:
            raise HTTPException(404, detail="unable to generate report")
        template.render(context)
        template.save(os.path.join(base_report_path, req_data.get("nameoutput") + "." + req_data.get("typefile")))  # type: ignore
        return True
    except Exception as msg:
        print(msg)
        raise HTTPException(500, detail="Unable to generate report")


async def download_report(report_name: str):
    report_base_path = os.path.join("src", "reports")
    if not os.path.exists(os.path.join(report_base_path, report_name)):
        raise HTTPException(404, detail="Report not found")
    media_types = {
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pdf": "application/pdf",
    }
    report_type = report_name.split(".")[1]
    return FileResponse(
        os.path.join(report_base_path, report_name),
        media_type=media_types.get(report_type, "docx"),
    )
