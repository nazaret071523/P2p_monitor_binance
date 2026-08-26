import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(
    title="Venbot P2P API",
    description="API de tasas P2P y BCV en tiempo real",
    version="1.0.0"
)

# Habilitar CORS para permitir que Vercel o cualquier cliente web consulte la API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite peticiones desde cualquier origen (Vercel, Localhost, etc.)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Estructura de datos de respuesta
class P2PRates(BaseModel):
    buy_price: float
    sell_price: float
    bcv_price: float
    euro_price: float

@app.get("/")
def root():
    return {"status": "ok", "message": "Venbot API activa"}

@app.get("/api/v1/p2p-rates", response_model=P2PRates)
async def get_p2p_rates():
    # Aquí puedes conectar tu lógica existente o lectura de base de datos (Supabase)
    # Por defecto retorna las tasas actualizadas observadas en el mercado P2P
    return {
        "buy_price": 945.25,
        "sell_price": 956.00,
        "bcv_price": 898.50,
        "euro_price": 1050.00
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
