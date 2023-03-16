from datetime import datetime, timedelta

from fastapi import HTTPException, status
from config import get_settings
from jose import jwt, JWTError
from typing import Union
from schemas import LoginData
import hashlib

settings = get_settings()


async def encrypt_otp_with_md5(otp: str):
    return hashlib.md5(otp.encode()).hexdigest()


async def create_access_tokens(
    data: LoginData, expires_delta: Union[timedelta, None] = None
):
    to_encode = {"phone": data.phone}
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.jwt_expire_time)
    to_encode.update({"exp": expire})  # type: ignore
    encoded_jwt = jwt.encode(
        to_encode, settings.jwt_secret, algorithm=settings.algorithm
    )
    return encoded_jwt, settings.jwt_expire_time


async def decrypt_access_token(authorization: Union[str, None]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if authorization is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="access token is required",
        )
    authorization = authorization.split(" ")[-1]
    try:
        payload = jwt.decode(
            authorization, settings.jwt_secret, algorithms=[settings.algorithm]
        )
        phone: str = payload.get("phone")  # type: ignore
        if phone is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return phone