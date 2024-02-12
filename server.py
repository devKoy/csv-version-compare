from fastapi import FastAPI, File, UploadFile, HTTPException, Response, Request
from fastapi.responses import JSONResponse
import pandas as pd
import io, os
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
import uvicorn


origins = [
   "*"
]

ALLOWED_ORIGINS = '*' 
middleware = [
    Middleware(CORSMiddleware, allow_origins=origins,  allow_headers=["*"])
]

app = FastAPI(middleware=middleware)

# handle CORS preflight requests
@app.options('/*')
async def preflight_handler(request: Request, rest_of_path: str) -> Response:
    response = Response()
    response.headers['Access-Control-Allow-Origin'] = ALLOWED_ORIGINS
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = '*'
    return response

# set CORS headers
@app.middleware("http")
async def add_CORS_header(request: Request, call_next):
    response = await call_next(request)
    response.headers['Access-Control-Allow-Origin'] = ALLOWED_ORIGINS
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = '*'
    return response

def compare_csv_sheets(old_df, updated_df):
    try:
        old_df['OrderNo'] = old_df['OrderNo'].fillna(method='ffill')
        updated_df['OrderNo'] = updated_df['OrderNo'].fillna(method='ffill')

        result_df = pd.DataFrame(columns=list(updated_df.columns) + ['Update Type'])

        # Replace NaN values with a placeholder for key columns
        old_df[['OrderNo', 'StoreDescription']] = old_df[['OrderNo', 'StoreDescription']].fillna('NaN')
        updated_df[['OrderNo', 'StoreDescription']] = updated_df[['OrderNo', 'StoreDescription']].fillna('NaN')

        for index, old_row in old_df.iterrows():
            key_columns = ['OrderNo', 'StoreDescription']
            key_values = old_row[key_columns].values

            matching_rows = updated_df[
                (updated_df['OrderNo'] == key_values[0]) & (updated_df['StoreDescription'] == key_values[1])
            ]

            if matching_rows.empty:
                result_df = pd.concat([result_df, old_row.to_frame().T.assign(**{'Update Type': 'Removed'})], ignore_index=True)
            else:
                updated_row = matching_rows.iloc[0]
                if not old_row.equals(updated_row):
                    result_df = pd.concat([result_df, updated_row.to_frame().T.assign(**{'Update Type': 'Updated'})], ignore_index=True)

        for index, new_row in updated_df.iterrows():
            key_columns = ['OrderNo', 'StoreDescription']
            key_values = new_row[key_columns].values

            matching_rows = old_df[
                (old_df['OrderNo'] == key_values[0]) & (old_df['StoreDescription'] == key_values[1])
            ]

            if matching_rows.empty:
                result_df = pd.concat([result_df, new_row.to_frame().T.assign(**{'Update Type': 'New'})], ignore_index=True)

        result_df = result_df.reset_index(drop=True)

        # Convert DataFrame to JSON
        result_json = result_df.to_json(orient='records')

        return result_json

    except Exception as e:
        # Log the exception for debugging
        print(f"Exception during CSV comparison: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.post("/compare-csv/")
async def compare_csv(
    old_file: UploadFile = File(...),
    updated_file: UploadFile = File(...),
):
    try:
        # Read CSV files into memory
        old_data = await old_file.read()
        updated_data = await updated_file.read()

        # Parse CSV data into Pandas DataFrames
        old_df = pd.read_csv(io.StringIO(old_data.decode('utf-8')))
        updated_df = pd.read_csv(io.StringIO(updated_data.decode('utf-8')))

        # Perform CSV comparison
        result_json = compare_csv_sheets(old_df, updated_df)

        # Return the result as JSON
        return JSONResponse(content=result_json)

    except pd.errors.ParserError as pe:
        # Handle CSV parsing errors
        return HTTPException(status_code=400, detail=f"Error parsing CSV file: {str(pe)}")

    except Exception as e:
        # Handle other exceptions and log for debugging
        print(f"Unhandled exception: {str(e)}")
        return HTTPException(status_code=500, detail="Internal Server Error")

