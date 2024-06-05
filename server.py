import pandas as pd
import numpy as np
import time
import json
from fastapi import FastAPI, UploadFile, File, Request, Response
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
import uvicorn
from typing import Optional

# Middleware configuration for CORS
middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
]

app = FastAPI(middleware=middleware)

def calculate_qty_due(file: UploadFile, order_no: Optional[str] = None):
    try:
        # Read the uploaded Excel file
        df = pd.read_excel(file.file, sheet_name='Details', header=0)

        # Convert 'QtyOrdered' and 'QtyReceived' columns to integers
        df['QtyOrdered'] = pd.to_numeric(df['QtyOrdered'], errors='coerce').fillna(0).astype(int)
        df['QtyReceived'] = pd.to_numeric(df['QtyReceived'], errors='coerce').fillna(0).astype(int)

        if order_no:
            # Filter DataFrame by OrderNo if provided
            df = df[df['OrderNo'] == order_no]

        # Group items by 'OrderNo' and calculate the sum of 'QtyOrdered' and 'QtyReceived'
        grouped_df = df.groupby('OrderNo').agg({'QtyOrdered': 'sum', 'QtyReceived': 'sum'}).reset_index()

        # Calculate the quantity due for each order
        grouped_df['QtyDue'] = grouped_df['QtyOrdered'] - grouped_df['QtyReceived']

        # Convert the result to a dictionary
        result_dict = grouped_df.to_dict(orient='records')

        return result_dict
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def count_total_rows(file: UploadFile, batch_size: int):
    try:
        df = pd.read_excel(file.file, sheet_name='Details', header=0)
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
        raise HTTPException(status_code=500, detail=str(e))

def compare_csv_sheets(old_file: UploadFile, updated_file: UploadFile, start_row: int = 0, end_row: Optional[int] = None):
    try:
        start_time = time.time()  # Start time
        
        # Read the uploaded Excel files
        old_df = pd.read_excel(old_file.file, sheet_name='Details', header=1)
        updated_df = pd.read_excel(updated_file.file, sheet_name='Details', header=1)
        
        # Explicitly create a copy of the sliced DataFrame
        old_df = old_df.iloc[start_row:end_row].copy()
        updated_df = updated_df.iloc[start_row:end_row].copy()

        # Replace NaN values with a placeholder
        old_df.fillna("", inplace=True)
        updated_df.fillna("", inplace=True)

        # Mapping old column names to new column names
        column_mapping = {
            'Order #': 'OrderNo',
            'Product Brand': 'Brand',
            'Class': 'Class',
            'Ship Date': 'Ship Date',
            'Cancel Date': 'Cancel Date',
            'Qty Due': 'QtyDue',
            'Vendor Ref. #': 'Vendor Ref#',
            'Product Code': 'Style #',
            'V Attribute 2': 'Style Size',
            'Order Vendor VLU': 'VLU',
            'V Attribute 1': 'Color',
            'Qty ordered': 'Qty Ordered',
            'Qty Received': 'Qty Received'
        }

        old_df.rename(columns=column_mapping, inplace=True)
        updated_df.rename(columns=column_mapping, inplace=True)

        result_df = pd.DataFrame(columns=list(updated_df.columns) + ['Update Type'])

        unchanged_rows_count = 0  # Counter for unchanged rows

        # NEW and UPDATED records
        for index, new_row in updated_df.iterrows():
            key_columns = ['OrderNo', 'VLU']
            key_values = new_row[key_columns].values

            matching_rows = old_df[
                (old_df['OrderNo'] == key_values[0]) & (old_df['VLU'] == key_values[1])
            ]

            if matching_rows.empty:
                result_df = pd.concat([result_df, new_row.to_frame().T.assign(**{'Update Type': 'New'})], ignore_index=True)
            else:
                old_row = matching_rows.iloc[0]
                if new_row['Style Size'] == old_row['Style Size']:
                    if (new_row['Qty Ordered'] != old_row['Qty Ordered'] or 
                        new_row['Qty Received'] != old_row['Qty Received'] or 
                        new_row['QtyDue'] != old_row['QtyDue']):
                        result_df = pd.concat([result_df, new_row.to_frame().T.assign(**{'Update Type': 'Updated'})], ignore_index=True)
                    else:
                        unchanged_rows_count += 1  # Increment count for unchanged rows
                else:
                    result_df = pd.concat([result_df, new_row.to_frame().T.assign(**{'Update Type': 'New'})], ignore_index=True)

        # REMOVED records
        for index, old_row in old_df.iterrows():
            key_columns = ['OrderNo', 'VLU']
            key_values = old_row[key_columns].values

            matching_rows = updated_df[
                (updated_df['OrderNo'] == key_values[0]) & (updated_df['VLU'] == key_values[1])
            ]

            if matching_rows.empty:
                result_df = pd.concat([result_df, old_row.to_frame().T.assign(**{'Update Type': 'Removed'})], ignore_index=True)

        result_df = result_df.reset_index(drop=True)

        total_time = time.time() - start_time

        response_data = {
            "total_time": total_time,
            "unchanged_rows_count": unchanged_rows_count,
            "start_row": start_row,
            "end_row": end_row,
            "old_row_count": len(old_df),
            "updated_row_count": len(updated_df),
            "result": result_df.to_dict(orient='records'),
        }

        return response_data
    
    except Exception as e:
        # Log the exception for debugging
        print(f"Exception during CSV comparison: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/calculate_qty_due/")
async def api_calculate_qty_due(file: UploadFile, order_no: Optional[str] = None):
    return calculate_qty_due(file, order_no)

@app.post("/count_total_rows/")
async def api_count_total_rows(file: UploadFile, batch_size: int):
    return count_total_rows(file, batch_size)

@app.post("/compare_csv_sheets/")
async def api_compare_csv_sheets(old_file: UploadFile, updated_file: UploadFile, start_row: int = 0, end_row: Optional[int] = None):
    return compare_csv_sheets(old_file, updated_file, start_row, end_row)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
