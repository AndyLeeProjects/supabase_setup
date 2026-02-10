# Master Data Management App

A simple Streamlit web application for managing master data in your data warehouse.

## Features

- ✅ **Add Clients** - Create new client organizations
- ✅ **Add Practices** - Create practices linked to clients  
- ✅ **Add Providers** - Create providers linked to practices
- ✅ **View Data** - Browse all master data with relationships
- ✅ **Form Validation** - Required fields and smart defaults
- ✅ **Real-time Database** - Direct integration with Supabase

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the app:**
   ```bash
   streamlit run master_data_app.py
   ```

3. **Open in browser:**
   - The app will automatically open at `http://localhost:8501`

## Usage Flow

1. **Add Client** first (required)
2. **Add Practice(s)** linked to the client
3. **Add Provider(s)** linked to practices
4. **View Data** to verify everything is correct

## Database Connection

The app uses the existing `connect_db.py` utility to connect to your Supabase database. Make sure your database connection is configured properly.

## Simple & Fast

No complex command-line interfaces - just fill out the forms and click submit!