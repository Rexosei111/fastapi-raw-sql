from config import get_settings
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Integer, String, Column


class Base(DeclarativeBase):
    pass


class TbParameters(Base):
    __tablename__ = "tb_parameter"
    id_parameter = Column(Integer, primary_key=True)
    databasename = Column(String(100))
    tablename = Column(String(100))
    id_select = Column(String(3), default="no")
    id_insert = Column(String(3), default="no")
    id_update = Column(String(3), default="no")
    id_delete = Column(String(3), default="no")
    id_truncate = Column(String(3), default="no")
    id_drop = Column(String(3), default="no")
    id_token = Column(String(3), default="no")


settings = get_settings()
db_parameter_engine = create_async_engine(
    settings.db_parameter_url, future=True, echo=False
)
db_transaction_engine = create_async_engine(
    settings.db_transaction_url, future=True, echo=False
)
