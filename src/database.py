import os
from sqlalchemy import create_engine, Column, Integer, String, Float, UniqueConstraint, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from .logger import logger

# --- Database Setup ---
# Ensure the data directory exists
os.makedirs('data', exist_ok=True)
DB_FILE = "data/trading_data.db"
DATABASE_URL = f"sqlite:///{DB_FILE}"

engine = create_engine(DATABASE_URL, echo=False) # Set echo=True to see generated SQL
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- ORM Model Definition ---
class Candle(Base):
    __tablename__ = "candles"

    id = Column(Integer, primary_key=True, index=True)
    market = Column(String, nullable=False, index=True)
    timestamp = Column(Integer, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint('market', 'timestamp', name='_market_timestamp_uc'),
    )

    def __repr__(self):
        return f"<Candle(market='{self.market}', timestamp={self.timestamp}, close={self.close})>"

# --- Database Interaction Functions ---

def setup_database():
    """Creates the database and the candles table if they don't exist."""
    Base.metadata.create_all(bind=engine)

def get_latest_candle_timestamp(market: str) -> int | None:
    """
    Retrieves the timestamp of the most recent candle for a given market.
    Returns the timestamp (in milliseconds) or None if no data exists for the market.
    """
    with SessionLocal() as session:
        # Query for the maximum timestamp for the given market, which is more efficient.
        latest_timestamp = session.query(func.max(Candle.timestamp)).filter(Candle.market == market).scalar()
    return latest_timestamp

def insert_candles(market: str, candles_data: list):
    """
    Inserts a list of candle data into the database, ignoring duplicates.
    Uses a bulk insert statement for efficiency.
    """
    if not candles_data:
        return

    # Prepare data for bulk insertion
    objects_to_insert = [
        {"market": market, "timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}
        for ts, o, h, l, c, v in candles_data
    ]

    # Create a statement that works like "INSERT OR IGNORE" for SQLite
    stmt = sqlite_insert(Candle).values(objects_to_insert)
    stmt = stmt.on_conflict_do_nothing(index_elements=['market', 'timestamp'])

    with SessionLocal() as session:
        result = session.execute(stmt)
        session.commit()
        logger.info(f"  -> Inserted {result.rowcount} new candles for {market}.")
