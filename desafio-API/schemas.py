from typing import List
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

class VolumeResponse(BaseModel):
    ticker: str
    date: date
    volume: int

class LowestClosingPriceResponse(BaseModel):
    ticker: str
    date: date
    close: float

class MeanPriceResponse(BaseModel):
    ticker: str
    date: date
    mean_price: float

class PaginatedResponse(BaseModel):
    data: List
    page: int
    total_pages: int

class DailyVariationResponse(BaseModel):
    date: date
    variation: float

class ConsolidatedResponse(BaseModel):
    date: date
    mean_price: float
    variation: float

class Config:
    from_attributes = True
