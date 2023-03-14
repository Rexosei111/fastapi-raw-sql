from pydantic import BaseModel
from typing import Optional
from datetime import date, time, datetime
from enum import Enum


class EnumYesOrNo(str, Enum):
    no = "no"
    yes = "yes"


class TbParameterRead(BaseModel):
    id_parameter: int
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
    idc: int
    xname: Optional[str]
    xaddress: Optional[str]
    xdate: Optional[date]
    xprice: Optional[float]
    xtime: Optional[time]
    xint: Optional[int]
    xtimestamp: Optional[datetime]
