from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

def get_engine(use_pooler=True):
    """
    Create and return a SQLAlchemy engine for Supabase connection.
    
    Args:
        use_pooler (bool): If True, uses the Supabase pooler connection string
        
    Returns:
        engine: SQLAlchemy engine object
    """
    # Using the working pooler connection string
    DATABASE_URL = "postgresql://postgres.mdusbtytnzevfrrtnniz:Dkeow!9fdus*io@aws-1-us-east-1.pooler.supabase.com:6543/postgres"
    
    if use_pooler:
        # Disable SQLAlchemy client-side pooling for Transaction/Session Pooler
        engine = create_engine(DATABASE_URL, poolclass=NullPool)
    else:
        engine = create_engine(DATABASE_URL)
    
    return engine

def test_connection():
    """Test the database connection"""
    try:
        engine = get_engine()
        with engine.connect() as connection:
            print("Connection successful!")
            return True
    except Exception as e:
        print(f"Failed to connect: {e}")
        return False

# Test connection when running this file directly
if __name__ == "__main__":
    test_connection()
