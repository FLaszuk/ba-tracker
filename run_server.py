"""Run this script to start the BA-Tracker API server."""
import uvicorn, os, sys

sys.path.insert(0, os.path.dirname(__file__))

if __name__ == "__main__":
    uvicorn.run("app.api:app", host="127.0.0.1", port=8000, reload=True)
