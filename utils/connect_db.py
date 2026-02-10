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
    # Try to get DATABASE_URL from environment first (for Docker/production)
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    # Fallback to individual components if DATABASE_URL not set
    if not DATABASE_URL:
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'postgres')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_sslmode = os.getenv('DB_SSLMODE', 'require')
        
        if db_user and db_password:
            DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={db_sslmode}"
        else:
            # Final fallback to hardcoded for development
            print("Warning: Using hardcoded database connection. Set environment variables for production.")
    
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
