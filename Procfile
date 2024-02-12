web: gunicorn server:app --workers 4 # Increase the number of workers (adjust based on available resources)
    -k uvicorn.workers.UvicornWorker # Use Uvicorn worker class
    --timeout 300 # Increase the worker timeout to 5 minutes (adjust as needed)
    --max-requests 1  # Increase the maximum number of requests per worker
