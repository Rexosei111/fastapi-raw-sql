from typing import Optional
from fastapi import FastAPI, Body, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from services import (
    execute_select_sql_command,
    execute_sql_command,
    login_user,
    view_db_tables,
    view_table_columns,
    generate_report,
)
from fastapi import Header
import uvicorn
from schemas import LoginData, ReqBody

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        content={"codestatus": exc.status_code, "detail": exc.detail},
        status_code=exc.status_code,
    )


@app.exception_handler(RequestValidationError)
async def value_error_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content=jsonable_encoder({"detail": exc.errors(), "status_code": 422}),
    )


@app.post("/api/v1/opensql")
async def execute_select_sql(
    text: str = Body(..., media_type="text/plain"),
    authorization: Optional[str] = Header(default=None),
):
    result = await execute_select_sql_command(
        sql_statement=text, authorization_token=authorization
    )
    return result


@app.post("/api/v1/exesql")
async def execute_sql(
    text: str = Body(..., media_type="text/plain"),
    authorization: Optional[str] = Header(default=None),
):
    await execute_sql_command(sql_statement=text, authorization_token=authorization)
    return {"codestatus": 200, "msg": "success"}


@app.post("/api/v1/login")
async def login(data: LoginData):
    return await login_user(data=data)


@app.get("/api/v1/tables")
async def view_tables():
    """
    Get the List of names of tables in the database
    """
    return await view_db_tables()


@app.get("/api/v1/tables/{table_name}")
async def view_db_table_columns(table_name: str):
    """
    Get the List of columns for a specific table
    """
    return await view_table_columns(table_name)


@app.post("/api/v1/reports")
async def generate_db_report(data: ReqBody):
    return await generate_report(data) #type: ignore


if __name__ == "__main__":
    uvicorn.run("main:app", host="localhost", port=5000, reload=True)  # type: ignore
