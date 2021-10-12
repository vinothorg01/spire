from sqlalchemy.sql.sqltypes import BigInteger, Boolean, Date, String, Integer
from spire.framework.workflows.connectors import Base
from sqlalchemy import Column
import datetime


class DFPOrder(Base):
    __tablename__ = "dfp_orders"
    id = Column(Integer(), primary_key=True, autoincrement=True, unique=True)
    advertiser = Column(String(100), nullable=False)
    order_id = Column(BigInteger(), nullable=False, unique=True)
    start_date = Column(Date(), default=datetime.date.today(), nullable=False)
    last_processed_date = Column(Date(), default=datetime.date.today())
    active = Column(Boolean(), default=True, nullable=False)
