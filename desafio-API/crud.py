from sqlalchemy.orm import Session
from models import Asset, Price
from schemas import PriceBase

def get_or_create_asset(db: Session, ticker: str):
    asset = db.query(Asset).filter(Asset.ticker == ticker).first()
    if not asset:
        asset = Asset(ticker=ticker)
        db.add(asset)
        db.commit()
        db.refresh(asset)
    return asset

def create_prices(db: Session, ticker: str, prices: list[PriceBase]):
    asset = get_or_create_asset(db, ticker)
    for price_data in prices:
        price_base = PriceBase(**price_data)
        price = Price(asset_id=asset.id, **price_base.dict())
        db.add(price)
    db.commit()

def get_highest_volume(db: Session, ticker: str = None):
    query = db.query(Price)
    if ticker:
        query = query.join(Asset).filter(Asset.ticker == ticker)
    return query.order_by(Price.volume.desc()).first()