from fastapi import FastAPI, UploadFile, Depends, HTTPException, File
from sqlalchemy.orm import Session
from database import Base, engine, get_db
from models import Asset, Price
from crud import create_prices, get_highest_volume
from schemas import PriceResponse
import pandas as pd
from typing import List 

app = FastAPI(
    title="API para consulta de dados sobre stocks",
    description="Para o primeiro uso desta API, você deve fazer upload de arquivos CSV contendo dados de preços de ações."
     +"\t Os arquivos devem conter as colunas 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close' e 'Volume'.",
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

# Modificação: Alterar a assinatura da função para aceitar uma lista de arquivos
@app.post("/upload-csv/")
async def upload_csv(files: List[UploadFile] = File(...), db: Session = Depends(get_db)):
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

@app.get("/highest-volume/", response_model=PriceResponse)
def highest_volume(ticker: str = None, db: Session = Depends(get_db)):
    price = get_highest_volume(db, ticker)
    if not price:
        raise HTTPException(status_code=404, detail="No data found")
    return price