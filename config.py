from decouple import config

# In your FastAPI app
DB_HOST = config('DB_HOST', default='localhost')
DB_USER = config('DB_USER', default='root')
DB_PASSWORD = config('DB_PASSWORD')