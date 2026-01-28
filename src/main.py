from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from contextlib import asynccontextmanager

from src.api.v1.forecasting import forecast_router
from src.database.session import Database
from src.utils.route import group

@asynccontextmanager
async def lifespan(app: FastAPI):
    Database.connect_async_session()
    yield
    await Database.close()
    
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


api_v1_router = group(
    "/api/v1",
    (forecast_router, "/forecasts", ["Forecasts"]),
)

app.include_router(api_v1_router)


@app.get("/health")
def check_server_health():
    return {"message": "Server is Healthy"}
