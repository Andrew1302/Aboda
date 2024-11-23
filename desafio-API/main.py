from fastapi import FastAPI, UploadFile, Depends, HTTPException, File
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from database import Base, engine, get_db
from models import Asset, Price
from schemas import PriceBase
from crud import update_prices,create_prices, get_highest_volume, get_lowest_closing_price, get_mean_daily_price, get_assets, delete_asset
from schemas import VolumeResponse, CloseResponse, MeanPriceResponse
import pandas as pd
from typing import List 

TAG_MANAGE_DATA = "Manage Data"
TAG_STATISTICS = "Statistics"

app = FastAPI(
    title="ABODA CHALLENGE",
    description="This API was built as part of Aboda's selection process."
    + "\nFor the first use of this API, you must upload CSV files containing stock price data.",
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
    for file in files:  # Modificação: Iterar sobre cada arquivo na lista
        try:
            df = pd.read_csv(file.file)
            required_columns = {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"}
            if not required_columns.issubset(df.columns):
                raise HTTPException(status_code=400, detail=f"CSV file {file.filename} is missing required columns")
            
            ticker = file.filename.split('.')[0]  # Assumindo que o nome do arquivo é o ticker
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
    total_insertions = 0
    total_updates = 0
    for file in files:
        try:
            df = pd.read_csv(file.file)
            required_columns = {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"}
            if not required_columns.issubset(df.columns):
                raise HTTPException(status_code=400, detail=f"CSV file {file.filename} is missing required columns")
            
            ticker = file.filename.split('.')[0]  # Assumindo que o nome do arquivo é o ticker
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
    assets = get_assets(db, ticker)
    if not assets:
        raise HTTPException(status_code=404, detail="No assets found")
    return [{"ticker": asset.ticker} for asset in assets]

@app.delete("/assets/{ticker}", tags=[TAG_MANAGE_DATA])
def delete_asset_route(ticker: str, db: Session = Depends(get_db)):
    success = delete_asset(db, ticker)
    if not success:
        raise HTTPException(status_code=404, detail="Asset not found")
    return {"message": "Asset deleted successfully"}

@app.get("/highest-volume/", response_model=VolumeResponse, tags=[TAG_STATISTICS])
def highest_volume(ticker: str = None, db: Session = Depends(get_db)):
    price = get_highest_volume(db, ticker)
    if not price:
        raise HTTPException(status_code=404, detail="No data found")
    return price

@app.get("/lowest-closing-price/", response_model=CloseResponse, tags=[TAG_STATISTICS])
def lowest_closing_price(ticker: str = None, db: Session = Depends(get_db)):
    price = get_lowest_closing_price(db, ticker)
    if not price:
        raise HTTPException(status_code=404, detail="No data found")
    return price

@app.get("/mean-daily-price/", response_model=MeanPriceResponse, tags=[TAG_STATISTICS])
def mean_daily_price(ticker: str, date: str, db: Session = Depends(get_db)):
    price = get_mean_daily_price(db, ticker, date)
    if not price:
        raise HTTPException(status_code=404, detail="No data found")
    return price

