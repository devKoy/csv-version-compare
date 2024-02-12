import pandas as pd
import time
import json
from fastapi import FastAPI, UploadFile, File, Request, Response
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


def count_total_rows(csv_file, batch_size):
    try:
        df = pd.read_csv(csv_file.file)
        total_rows = len(df)
        total_loops = (total_rows + batch_size - 1) // batch_size  # Calculate total loops

        batch_points = []
        start = 0
        for i in range(total_loops):
            end = min(start + batch_size, total_rows)
            batch_points.append({"start": start, "end": end})
            start = end + 1  # Next start point

        return {"total_rows": total_rows, "total_loops": total_loops, "batch_points": batch_points}
    except Exception as e:
        return {"error": str(e)}

def compare_csv_sheets(old_df, updated_df, start_row=0, end_row=None):
    try:
        start_time = time.time()  # Start time
        
        # Explicitly create a copy of the sliced DataFrame
        old_df = old_df.iloc[start_row:end_row].copy()
        updated_df = updated_df.iloc[start_row:end_row].copy()

        # Replace NaN values with a placeholder
        old_df.fillna("", inplace=True)
        updated_df.fillna("", inplace=True)

        result_df = pd.DataFrame(columns=list(updated_df.columns) + ['Update Type'])

        unchanged_rows_count = 0  # Counter for unchanged rows

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
                else:
                    unchanged_rows_count += 1  # Increment count for unchanged rows

        for index, new_row in updated_df.iterrows():
            key_columns = ['OrderNo', 'StoreDescription']
            key_values = new_row[key_columns].values

            matching_rows = old_df[
                (old_df['OrderNo'] == key_values[0]) & (old_df['StoreDescription'] == key_values[1])
            ]

            if matching_rows.empty:
                result_df = pd.concat([result_df, new_row.to_frame().T.assign(**{'Update Type': 'New'})], ignore_index=True)

        result_df = result_df.reset_index(drop=True)

        total_time = time.time() - start_time  # Calculate total time taken
        
        # Define column names for the JSON keys
        column_names = ['Vendor', 'UniversalNo', 'OrderNo', 'Location', 'OrderDate', 'ShipDate', 'CancelDate',
                        'PLU', 'VLU', 'Department', 'Class', 'League', 'Team', 'Description1', 'StoreDescription',
                        'Attribute1', 'Attribute2', 'Attribute3', 'QtyOrdered', 'QtyReceived', 'QtyRemaining',
                        'Textbox197', 'OrderAmount', 'RetailAmount', 'Textbox206']
        
        # Convert result_df to JSON array with custom column names
        result_json = result_df.rename(columns=dict(zip(result_df.columns, column_names))).to_dict(orient='records')

        return result_json, total_time, unchanged_rows_count, start_row, end_row, len(old_df), len(updated_df)
    
    except Exception as e:
        # Log the exception for debugging
        print(f"Exception during CSV comparison: {str(e)}")
        raise


@app.post("/count_total_rows")
async def count_total_rows_endpoint(csv_file: UploadFile = File(...), batch_size: int = 1000):
    return count_total_rows(csv_file, batch_size)



@app.post("/compare")
async def compare_csv_files(start_row: int = 0, end_row: int = None, old_csv: UploadFile = File(...), updated_csv: UploadFile = File(...)):
    try:
        # Load CSV files
        old_df = pd.read_csv(old_csv.file)
        updated_df = pd.read_csv(updated_csv.file)

        # Perform CSV comparison
        result_json, total_time, unchanged_rows_count, start_row, end_row, old_row_count, updated_row_count = compare_csv_sheets(old_df, updated_df, start_row, end_row)

        response_data = {
            "total_time": total_time,
            "unchanged_rows_count": unchanged_rows_count,
            "start_row": start_row,
            "end_row": end_row,
            "old_row_count": len(old_df),
            "updated_row_count": len(updated_df),
            "result": result_json,
        }

        return response_data
    except Exception as e:
        return {"error": f"Error during comparison: {str(e)}"}

# This block is used to run the FastAPI server when this script is executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
