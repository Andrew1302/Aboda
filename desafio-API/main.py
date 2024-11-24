from datetime import date
from fastapi import FastAPI, UploadFile, Depends, HTTPException, File, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from database import Base, engine, get_db
from middleware import check_start_end_date, check_ticker
from models import Asset, Price
from schemas import ConsolidatedResponse, DailyVariationResponse, PriceBase, PaginatedResponse
from crud import get_consolidated_data, update_prices, create_prices, get_highest_volume, get_lowest_closing_price
from crud import get_mean_daily_price, get_assets, delete_asset, get_daily_variation
from schemas import VolumeResponse, LowestClosingPriceResponse, MeanPriceResponse
import pandas as pd
from typing import List
from utils import paginate

TAG_MANAGE_DATA = "Manage Data"
TAG_STATISTICS = "Statistics"

app = FastAPI(
    title="ABODA CHALLENGE",
    description="This API was built as part of Aboda selection process."
    + "\n For the first use of this API, you must upload CSV files containing stock price data."
    + "\n \n You can write the ticker both in uppercase and lowercase and parameters like start_date, end_date, and ticker are validated in the server.",
    version="1.0.0",
    contact={
        "name": "Andrew Carvalho de Sá",
        "url": "https://www.linkedin.com/in/andrew-csa/",
        "email": "andrew@usp.br",
    },
    docs_url="/docs",
)

# Inicializar banco de dados
Base.metadata.create_all(bind=engine)

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

@app.post("/assets/", tags=[TAG_MANAGE_DATA])
async def upload_csv(files: List[UploadFile] = File(...), db: Session = Depends(get_db)):
    """
    Insert a list of CSV files into the database.
    Each file should contain the columns 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close' and 'Volume'.
    If you upload a file from an asset that is already in the database, the data will be duplicated, so consider using the PUT /assets endpoint instead.
    """
    for file in files:
        try:
            df = pd.read_csv(file.file)
            required_columns = {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"}
            if not required_columns.issubset(df.columns):
                raise HTTPException(status_code=400, detail=f"CSV file {file.filename} is missing required columns")
            
            ticker = file.filename.split('.')[0].upper()  # Assumindo que o nome do arquivo é o ticker
            prices = [
                {
                    "date": row["Date"],
                    "open_price": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "adj_close": row["Adj Close"],
                    "volume": row["Volume"],
                }
                for _, row in df.iterrows()
            ]
            create_prices(db, ticker, prices)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing file {file.filename}: {str(e)}")
    return {"message": "Data uploaded successfully"}

@app.put("/assets/", tags=[TAG_MANAGE_DATA])
async def upload_csv(files: List[UploadFile] = File(...), db: Session = Depends(get_db)):
    """
    Insert a list of CSV files into the database.
    If you upload a file from an asset that is already in the database, the data will be updated.
    \nReturns:
        The number of insertions and updates made.
    """
    total_insertions = 0
    total_updates = 0
    for file in files:
        try:
            df = pd.read_csv(file.file)
            required_columns = {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"}
            if not required_columns.issubset(df.columns):
                raise HTTPException(status_code=400, detail=f"CSV file {file.filename} is missing required columns")
            
            ticker = file.filename.split('.')[0].upper()  # Assumindo que o nome do arquivo é o ticker
            prices = [
                PriceBase(
                    date=row["Date"],
                    open_price=row["Open"],
                    high=row["High"],
                    low=row["Low"],
                    close=row["Close"],
                    adj_close=row["Adj Close"],
                    volume=row["Volume"],
                )
                for _, row in df.iterrows()
            ]
            insertions, updates = update_prices(db, ticker, prices)
            total_insertions += insertions
            total_updates += updates
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing file {file.filename}: {str(e)}")
    return {"message": f"Data uploaded successfully: {total_insertions} insertions, {total_updates} updates"}

@app.get("/assets/", tags=[TAG_MANAGE_DATA])
def assets(ticker: str = None, db: Session = Depends(get_db)):
    """
    Returns:
        List of all assets in the database.
    """
    if ticker:
        ticker = ticker.upper()
    assets = get_assets(db, ticker)
    if not assets:
        raise HTTPException(status_code=404, detail="No assets found")
    return [{"ticker": asset.ticker} for asset in assets]

@app.delete("/assets/{ticker}", tags=[TAG_MANAGE_DATA])
def delete_asset_route(ticker: str, db: Session = Depends(get_db)):
    """
    Delete the provided asset from the database, both from the Asset and Price tables.
    """
    ticker = ticker.upper()
    success = delete_asset(db, ticker)
    if not success:
        raise HTTPException(status_code=404, detail="Asset not found")
    return {"message": "Asset deleted successfully"}

@app.get("/highest-volume/", response_model=VolumeResponse, tags=[TAG_STATISTICS])
def highest_volume(ticker: str = None, start_date: date = None, end_date: date = None, db: Session = Depends(get_db)):
    """
    Get the highest negotiated volume for a given ticker.
    If any ticket is provided, the API will return the highest negotiated volume for all assets.
    You can optionally filter the results by a date range.

    Returns:
        The ticker, date, and volume of the highest negotiated volume.
    """
    if ticker:
        ticker = ticker.upper()
        check_ticker(ticker, db)
    check_start_end_date(start_date, end_date)
    price = get_highest_volume(db, ticker, start_date, end_date)
    if not price:
        raise HTTPException(status_code=404, detail="No data found")
    return price

@app.get("/lowest-closing-price/", response_model=LowestClosingPriceResponse, tags=[TAG_STATISTICS])
def lowest_closing_price(ticker: str = None, start_date: date = None, end_date: date = None, db: Session = Depends(get_db)):
    """
    Get the lowest-closing-price for a given ticker.
    If any ticket is provided, the API will return the lowest-closing-price for all assets.
    You can optionally filter the results by a date range.

    Returns:
        The ticker, date, and closing price of the lowest-closing-price.
    """
    if ticker:
        ticker = ticker.upper()
        check_ticker(ticker, db)
    check_start_end_date(start_date, end_date)
    price = get_lowest_closing_price(db, ticker, start_date, end_date)
    if not price:
        raise HTTPException(status_code=404, detail="No data found")
    return price

@app.get("/mean-daily-price/", response_model=PaginatedResponse, tags=[TAG_STATISTICS])
def mean_daily_price(ticker: str, start_date: date = None, end_date: date = None, page: int = Query(1, ge=1), page_size: int = Query(10, ge=1), db: Session = Depends(get_db)):
    """
    Get the daily mean price between the opening and closing prices for a given ticker.
    You can optionally filter the results by a date range.

    Returns:
        A list of daily mean prices with date and prices.
    """
    ticker = ticker.upper()
    check_ticker(ticker, db)
    check_start_end_date(start_date, end_date)
    prices = get_mean_daily_price(db, ticker, start_date, end_date)
    if not prices:
        raise HTTPException(status_code=404, detail="No data found")
    paginated_data, current_page, total_pages = paginate(prices, page, page_size)
    return {"data": paginated_data, "page": current_page, "total_pages": total_pages}

@app.get("/daily-variation/", response_model=PaginatedResponse, tags=[TAG_STATISTICS])
def daily_variation(ticker: str, start_date: date = None, end_date: date = None, page: int = Query(1, ge=1), page_size: int = Query(10, ge=1), db: Session = Depends(get_db)):
    """
    Get the daily percentage variation between the opening and closing prices for a given ticker.
    You can optionally filter the results by a date range.

    Returns:
        A list of daily variations with date and variation percentage.
    """
    ticker = ticker.upper()
    check_ticker(ticker, db)
    check_start_end_date(start_date, end_date)
    variations = get_daily_variation(db, ticker, start_date, end_date)
    if not variations:
        raise HTTPException(status_code=404, detail="No data found")
    paginated_data, current_page, total_pages = paginate(variations, page, page_size)
    return {"data": paginated_data, "page": current_page, "total_pages": total_pages}

@app.get("/consolidated-data/", response_model=PaginatedResponse, tags=[TAG_STATISTICS])
def consolidated_data(ticker: str, start_date: date = None, end_date: date = None, page: int = Query(1, ge=1), page_size: int = Query(10, ge=1), db: Session = Depends(get_db)):
    """
    Get consolidated data for a given ticker, including mean price and percentage variation.
    You can optionally filter the results by a date range.

    Returns:
        A list of consolidated data with date, mean price, and variation percentage.
    """
    ticker = ticker.upper()
    check_ticker(ticker, db)
    check_start_end_date(start_date, end_date)
    data = get_consolidated_data(db, ticker, start_date, end_date)
    if not data:
        raise HTTPException(status_code=404, detail="No data found")
    paginated_data, current_page, total_pages = paginate(data, page, page_size)
    return {"data": paginated_data, "page": current_page, "total_pages": total_pages}