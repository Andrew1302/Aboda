from pydantic import BaseModel
from datetime import date

class PriceBase(BaseModel):
    date: date
    open_price: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int

class AssetBase(BaseModel):
    ticker: str

class PriceResponse(BaseModel):
    date: date
    volume: int

    class Config:
        from_attributes = True
