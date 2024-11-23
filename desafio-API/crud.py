from datetime import date
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

def update_prices(db: Session, ticker: str, prices: list[PriceBase]):
    asset = get_or_create_asset(db, ticker)
    insertions = 0
    updates = 0
    for price_data in prices:
        existing_price = db.query(Price).filter(Price.asset_id == asset.id, Price.date == price_data.date).first()
        if existing_price:
            for key, value in price_data.dict().items():
                setattr(existing_price, key, value)
            updates += 1
        else:
            price = Price(asset_id=asset.id, **price_data.dict())
            db.add(price)
            insertions += 1
    db.commit()
    return insertions, updates

def get_highest_volume(db: Session, ticker: str = None, start_date: date = None, end_date: date = None):
    query = db.query(Price.date, Price.volume, Asset.ticker).join(Asset, Price.asset_id == Asset.id)
    if ticker:
        query = query.filter(Asset.ticker == ticker)
    if start_date:
        query = query.filter(Price.date >= start_date)
    if end_date:
        query = query.filter(Price.date <= end_date)
    result = query.order_by(Price.volume.desc()).first()
    if result:
        return {
            "ticker": result.ticker,
            "date": result.date,
            "volume": result.volume,
        }
    return None

def get_lowest_closing_price(db: Session, ticker: str = None, start_date: date = None, end_date: date = None):
    query = db.query(Price.date, Price.close, Asset.ticker).join(Asset, Price.asset_id == Asset.id)
    if ticker:
        query = query.filter(Asset.ticker == ticker)
    if start_date:
        query = query.filter(Price.date >= start_date)
    if end_date:
        query = query.filter(Price.date <= end_date)
    result = query.order_by(Price.close).first()
    if result:
        return {
            "ticker": result.ticker,
            "date": result.date,
            "close": result.close,
        }
    return None

def get_mean_daily_price(db: Session, ticker: str, start_date: date = None, end_date: date = None):
    query = db.query(Price.date, Price.open_price, Price.close, Asset.ticker).join(Asset, Price.asset_id == Asset.id)
    query = query.filter(Asset.ticker == ticker)
    if start_date:
        query = query.filter(Price.date >= start_date)
    if end_date:
        query = query.filter(Price.date <= end_date)
    results = query.all()
    mean_prices = []
    for result in results:
        mean_price = (result.open_price + result.close) / 2
        mean_prices.append({
            "ticker": result.ticker,
            "date": result.date,
            "mean_price": mean_price,
        })
    return mean_prices

def get_assets(db: Session, ticker: str):
    query = db.query(Asset)
    if ticker:
        query = query.filter(Asset.ticker == ticker) 
    return query.all()

def delete_asset(db: Session, ticker: str):
    asset = db.query(Asset).filter(Asset.ticker == ticker).first()
    if asset:
        db.query(Price).filter(Price.asset_id == asset.id).delete()
        db.delete(asset)
        db.commit()
        return True
    return False

def get_daily_variation(db: Session, ticker: str, start_date: date = None, end_date: date = None):
    query = db.query(Price.date, Price.open_price, Price.close).join(Asset, Price.asset_id == Asset.id)
    query = query.filter(Asset.ticker == ticker)
    if start_date:
        query = query.filter(Price.date >= start_date)
    if end_date:
        query = query.filter(Price.date <= end_date)
    results = query.all()
    variations = []
    for result in results:
        variation = ((result.close - result.open_price) / result.open_price) * 100
        variations.append({
            "date": result.date,
            "variation": variation
        })
    return variations

def get_consolidated_data(db: Session, ticker: str, start_date: date = None, end_date: date = None):
    query = db.query(Price.date, Price.open_price, Price.close).join(Asset, Price.asset_id == Asset.id)
    query = query.filter(Asset.ticker == ticker)
    if start_date:
        query = query.filter(Price.date >= start_date)
    if end_date:
        query = query.filter(Price.date <= end_date)
    results = query.all()
    consolidated_data = []
    for result in results:
        mean_price = (result.open_price + result.close) / 2
        variation = ((result.close - result.open_price) / result.open_price) * 100
        consolidated_data.append({
            "date": result.date,
            "mean_price": mean_price,
            "variation": variation
        })
    return consolidated_data