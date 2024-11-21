from database import Base, engine

# Importar os modelos
from models import Asset, Price

# Criar o banco de dados e as tabelas
def init():
    print("Criando as tabelas no banco SQLite...")
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init()
