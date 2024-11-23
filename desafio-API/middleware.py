from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session
from crud import get_assets
from database import get_db
from datetime import date

def check_start_end_date(start_date: date = None, end_date: date = None):
    if start_date and start_date > date.today():
        raise HTTPException(status_code=400, detail="start_date must be less than or equal to today")
    if end_date and end_date > date.today():
        raise HTTPException(status_code=400, detail="end_date must be less than or equal to today")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be less than or equal to end_date")

def check_ticker(ticker: str = None, db: Session = Depends(get_db)):
    if ticker and not get_assets(db, ticker):
        raise HTTPException(status_code=400, detail="Ticker isn't on the database")
    return True