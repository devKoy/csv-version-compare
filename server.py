import pandas as pd
import time
import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

app = FastAPI()

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update this with the appropriate domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def calculate_qty_due(file_path, order_no=None):
    try:
        df = pd.read_excel(file_path, sheet_name='Details', header=1)
        df['QtyOrdered'] = pd.to_numeric(df['QtyOrdered'], errors='coerce').fillna(0).astype(int)
        df['QtyReceived'] = pd.to_numeric(df['QtyReceived'], errors='coerce').fillna(0).astype(int)

        if order_no:
            df = df[df['OrderNo'] == order_no]

        grouped_df = df.groupby('OrderNo').agg({'QtyOrdered': 'sum', 'QtyReceived': 'sum'}).reset_index()
        grouped_df['QtyDue'] = grouped_df['QtyOrdered'] - grouped_df['QtyReceived']
        result_dict = grouped_df.to_dict(orient='records')
        return result_dict
    except Exception as e:
        return {"error": str(e)}

def count_total_rows(file_path, batch_size):
    try:
        df = pd.read_excel(file_path, sheet_name='Details', header=1)
        total_rows = len(df)
        total_loops = (total_rows + batch_size - 1) // batch_size

        batch_points = []
        start = 0
        for i in range(total_loops):
            end = min(start + batch_size, total_rows)
            batch_points.append({"start": start, "end": end})
            start = end + 1

        return {"total_rows": total_rows, "total_loops": total_loops, "batch_points": batch_points}
    except Exception as e:
        return {"error": str(e)}

def compare_csv_sheets(old_df, updated_df, min_row=0, max_row=None):
    try:
        start_time = time.time()

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

        if max_row:
            old_df_subset = old_df.iloc[min_row:max_row].copy()
            updated_df_subset = updated_df.iloc[min_row:max_row].copy()
        else:
            old_df_subset = old_df.copy()
            updated_df_subset = updated_df.copy()

        old_df_subset.fillna("", inplace=True)
        updated_df_subset.fillna("", inplace=True)

        result_df = pd.DataFrame(columns=list(updated_df.columns) + ['Update Type'])

        unchanged_rows_count = 0
        new_rows_count = 0
        removed_rows_count = 0
        updated_rows_count = 0

        matched_indices = set()

        for new_index, new_row in updated_df_subset.iterrows():
            key_columns = ['OrderNo', 'VLU', 'Style #']
            key_values = new_row[key_columns].values

            matching_rows = old_df[
                (old_df['OrderNo'] == key_values[0]) & 
                (old_df['VLU'] == key_values[1]) & 
                (old_df['Style #'] == key_values[2])
            ]

            if matching_rows.empty:
                result_df = pd.concat([result_df, new_row.to_frame().T.assign(**{'Update Type': 'New'})], ignore_index=True)
                new_rows_count += 1
            else:
                found_match = False
                for old_index, old_row in matching_rows.iterrows():
                    if old_index in matched_indices:
                        continue

                    if (new_row['Qty Ordered'] == old_row['Qty Ordered'] and
                        new_row['Qty Received'] == old_row['Qty Received'] and
                        new_row['QtyDue'] == old_row['QtyDue']):
                        unchanged_rows_count += 1
                        matched_indices.add(old_index)
                        found_match = True
                        break
                    elif not found_match:
                        result_df = pd.concat([result_df, new_row.to_frame().T.assign(**{'Update Type': 'Updated'})], ignore_index=True)
                        updated_rows_count += 1
                        matched_indices.add(old_index)
                        found_match = True
                        break

                if not found_match:
                    result_df = pd.concat([result_df, new_row.to_frame().T.assign(**{'Update Type': 'New'})], ignore_index=True)
                    new_rows_count += 1

        for old_index, old_row in old_df_subset.iterrows():
            if old_index not in matched_indices:
                result_df = pd.concat([result_df, old_row.to_frame().T.assign(**{'Update Type': 'Removed'})], ignore_index=True)
                removed_rows_count += 1

        result_df = result_df.reset_index(drop=True)

        total_time = time.time() - start_time
        return {
            'result_df': result_df,
            'total_time': total_time,
            'unchanged_rows_count': unchanged_rows_count,
            'new_rows_count': new_rows_count,
            'removed_rows_count': removed_rows_count,
            'updated_rows_count': updated_rows_count,
            'min_row': min_row,
            'max_row': max_row,
            'old_row_count': len(old_df),
            'updated_row_count': len(updated_df)
        }
    
    except Exception as e:
        print(f"Exception during CSV comparison: {str(e)}")
        raise

@app.post("/compare-sheets")
async def compare_sheets(old_file: UploadFile = File(...), updated_file: UploadFile = File(...), min_row: int = 0, max_row: int = None):
    try:
        old_df = pd.read_excel(old_file.file, sheet_name='Details', header=1)
        updated_df = pd.read_excel(updated_file.file, sheet_name='Details', header=1)

        comparison_result = compare_csv_sheets(old_df, updated_df, min_row, max_row)

        response_data = {
            "total_time": comparison_result['total_time'],
            "unchanged_rows_count": comparison_result['unchanged_rows_count'],
            "new_rows_count": comparison_result['new_rows_count'],
            "removed_rows_count": comparison_result['removed_rows_count'],
            "updated_rows_count": comparison_result['updated_rows_count'],
            "start_row": comparison_result['min_row'],
            "end_row": comparison_result['max_row'],
            "old_row_count": comparison_result['old_row_count'],
            "updated_row_count": comparison_result['updated_row_count'],
            "result": comparison_result['result_df'].to_dict(orient='records')
        }

        return JSONResponse(content=response_data)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/calculate-qty-due")
async def calculate_qty_due_endpoint(file: UploadFile = File(...), order_no: str = None):
    try:
        result = calculate_qty_due(file.file, order_no)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/count-total-rows")
async def count_total_rows_endpoint(file: UploadFile = File(...), batch_size: int = 100):
    try:
        result = count_total_rows(file.file, batch_size)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
