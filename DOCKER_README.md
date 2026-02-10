# Docker Deployment Instructions

## Quick Start

1. **Copy environment file and configure:**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

2. **Build and run:**
   ```bash
   docker-compose up -d --build
   ```

3. **Access the application:**
   - Open your browser to `http://your-server-ip:8501`
   - Or locally: `http://localhost:8501`

## Environment Configuration

### For Supabase:
```env
DATABASE_URL=postgresql://postgres.your-project:your-password@aws-0-us-east-1.pooler.supabase.com:5432/postgres
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_anon_key
```

### For Regular PostgreSQL:
```env
DB_HOST=your-db-host
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
DB_SSLMODE=require
```

## Docker Commands

```bash
# Build and start services
docker-compose up -d --build

# View logs
docker-compose logs -f streamlit-app

# Stop services
docker-compose down

# Rebuild and restart
docker-compose down && docker-compose up -d --build

# Check service status
docker-compose ps

# Access container shell
docker-compose exec streamlit-app bash
```

## Production Considerations

1. **Remove development volumes** from docker-compose.yml:
   ```yaml
   # Remove these lines in production:
   volumes:
     - ./app:/app/app:ro
     - ./utils:/app/utils:ro
   ```

2. **Use secrets management** instead of .env files for sensitive data

3. **Set up reverse proxy** (nginx) for SSL termination:
   ```nginx
   server {
       listen 80;
       server_name your-domain.com;
       
       location / {
           proxy_pass http://localhost:8501;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
           
           # WebSocket support for Streamlit
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection "upgrade";
       }
   }
   ```

4. **Monitor resources** and adjust container limits if needed

## Troubleshooting

- **Database connection issues**: Check your .env file and network connectivity
- **Port conflicts**: Change the host port in docker-compose.yml (e.g., "8502:8501")
- **Permission issues**: Ensure the streamlit user has proper permissions
- **Health check failures**: Check logs with `docker-compose logs streamlit-app`