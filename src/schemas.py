from pydantic import BaseModel
from typing import Optional, Union
from datetime import date, time, datetime
from enum import Enum


class EnumYesOrNo(str, Enum):
    no = "no"
    yes = "yes"


class TbParameterRead(BaseModel):
    id_parameter: Optional[int]
    databasename: Optional[str]
    tablename: Optional[str]
    id_select: EnumYesOrNo
    id_insert: EnumYesOrNo
    id_update: EnumYesOrNo
    id_delete: EnumYesOrNo
    id_truncate: EnumYesOrNo
    id_drop: EnumYesOrNo
    id_token: EnumYesOrNo


class TbTableRead(BaseModel):
    idc: Optional[Union[int, None]]
    xname: Optional[str]
    xaddress: Optional[str]
    xdate: Optional[date]
    xprice: Optional[float]
    xtime: Optional[time]
    xint: Optional[int]
    xtimestamp: Optional[datetime]


class LoginData(BaseModel):
    phone: str
    otp: str


class ReqBodyBase(BaseModel):
    nametemplate: str
    nameoutput: str
    typefile: Optional[str] = "docx"


class ReqBodySQLTest(ReqBodyBase):
    query: str


class ReqBodySQLMaster(ReqBodyBase):
    query: str
    detail_query: str


class ReqBody(ReqBodyBase):
    sqltest: Optional[str]
    sqltestmaster: Optional[str]
    sqltestdetail: Optional[str]
