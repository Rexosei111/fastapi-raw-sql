from fastapi import FastAPI, Body, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from services import execute_select_sql_command, execute_sql_command
import uvicorn

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
async def execute_select_sql(text: str = Body(..., media_type="text/plain")):
    result = await execute_select_sql_command(sql_statement=text)
    return result


@app.post("/api/v1/exesql")
async def execute_sql(text: str = Body(..., media_type="text/plain")):
    await execute_sql_command(sql_statement=text)
    return {"codestatus": 200, "msg": "success"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="localhost", port=8000, reload=True)  # type: ignore
