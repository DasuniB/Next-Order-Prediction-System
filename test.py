from flask import Flask, request, jsonify
import pandas as pd
import os
import traceback
''' testing app.py for new functioonalies
    1. changed port to 7205
    2. trying to testingcreate database connection to get data from mysql instead of csv data

'''
# Import custom modules
try:
    from next_order.data_handling.extract_data import extract_dataset
    from next_order.data_handling.clean_data import clean_dataset
    from next_order.prediction_and_output.prediction import prediction
    from next_order.prediction_and_output.output import output
    import database
except ImportError as e:
    print(f"Import error: {e}")
    traceback.print_exc()

# Initialize Flask app (API only, no frontend)
app = Flask(__name__)

# Global variables
visit_freq = 7
# data_path="D:/DASUNI/PROJECTS/Next_Order_Prediction-with api-with qty calculation for several times visits -main/ranith_data_logs/ranith.csv"
data_path="/root/Desktop/D-ML-projects/Next_Order_Prediction_Production_Ready_API/ranith_data_logs/next_order_prediction_dataset_with_rep_id.csv"
# data_path="E:/DASUNI/CAREER PATH/Evision Micro Systems/Vs code projects/ranith_data_logs/nadun.csv"

'''
# Processes a dataset and saves the predictions into a MySQL database.
@app.route('/process-dataset', methods=['POST'])
def process_data_and_save_result_in_database():
    try:
        # Optional repid parameter. If 'all' or omitted, process every rep in the dataset.
        repid = request.args.get('repid', None)
        csv_data = data_path
        print(f"[DEBUG] Reading CSV file: {csv_data}")
        # Check if file exists
        if not os.path.exists(csv_data):
            return jsonify({"status": "error", "message": f"CSV file not found: {csv_data}"}), 404
        full_data = pd.read_csv(csv_data)
        print(f"[DEBUG] Unique repids in dataset: {full_data['repcode'].astype(str).unique().tolist()}")
        # Validate repcode column exists
        if 'repcode' not in full_data.columns:
            return jsonify({"status": "error", "message": "Dataset does not contain a 'repcode' column."}), 400

        # Determine which repids to process
        available_repids = full_data['repcode'].astype(str).unique().tolist()
        if repid is None or str(repid).lower() == 'all':
            repids_to_process = available_repids
        else:
            if str(repid) not in available_repids:
                print(f"[DEBUG] Rep ID {repid} not found in dataset repcode column.")
                return jsonify({"status": "error", "message": f"Rep ID {repid} not found in dataset. Please check your REP ID"}), 400
            repids_to_process = [str(repid)]

        processed_reps = []
        errors = {}
        # Process each rep separately and commit per rep
        for rep in repids_to_process:
            try:
                rep_str = str(rep)
                rep_data = full_data[full_data['repcode'].astype(str) == rep_str]
                if rep_data.empty:
                    print(f"[DEBUG] No data for rep {rep_str}, skipping.")
                    continue
                db = database.get_connection()
                cursor = db.cursor()
                outlet_ids = list(rep_data['customercode'].unique())
                for outlet_id, outlet_data in rep_data.groupby("customercode"):
                    predicted_data = []
                    data = outlet_data
                    extracted_dataset = extract_dataset(data, outlet_id)
                    cleaned_data, ProductIDs, next_visit_date, outlier_product_ids = clean_dataset(extracted_dataset, visit_freq)
                    predicted_df = prediction(ProductIDs, cleaned_data, next_visit_date, visit_freq)
                    print(f"Predicted DataFrame columns: {predicted_df.columns}")
                    print(f"Predicted DataFrame shape: {predicted_df.shape}")
                    if predicted_df.empty:
                        print("Warning: Predicted DataFrame is empty for outlet {0} rep {1}".format(outlet_id, rep_str))
                        continue

                    for idx, row in predicted_df.iterrows():
                        # Ensure Next_Visit_Date is a datetime string in MySQL format
                        try:
                            next_visit_date_value = row['Next_Visit_Date'] if pd.notnull(row['Next_Visit_Date']) else None
                        except KeyError:
                            print(f"Warning: 'Next_Visit_Date' column not found in row {idx}. Available columns: {row.index.tolist()}")
                            continue
                        if next_visit_date_value is not None:
                            if not isinstance(next_visit_date_value, pd.Timestamp):
                                try:
                                    next_visit_date_dt = pd.to_datetime(next_visit_date_value)
                                except Exception:
                                    next_visit_date_dt = None
                            else:
                                next_visit_date_dt = next_visit_date_value
                        else:
                            next_visit_date_dt = None
                        # Convert to MySQL DATETIME string if not None
                        if next_visit_date_dt is not None:
                            next_visit_date_str = next_visit_date_dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            next_visit_date_str = None
                        # Check for existing entry with same repid, outletid, product, and next_visit_date
                        cursor.execute(
                            "SELECT 1 FROM suggested_order WHERE repid=%s AND outletid=%s AND product=%s AND Next_Visit_Date=%s",
                            (rep_str, outlet_id, row['ProductID'], next_visit_date_str)
                        )
                        exists = cursor.fetchone()
                        if not exists:
                            predicted_data.append((rep_str, outlet_id, row['ProductID'], row.get('predicted_qty', None), next_visit_date_str))
                    if predicted_data:
                        cursor.executemany(
                            "INSERT INTO suggested_order(repid,outletid,product,Qty,Next_Visit_Date) VALUES (%s, %s, %s, %s, %s)",
                            predicted_data
                        )
                        db.commit()
                db.close()
                processed_reps.append(rep_str)
            except Exception as rep_e:
                print(f"Error processing rep {rep}: {rep_e}")
                traceback.print_exc()
                errors[str(rep)] = str(rep_e)
                try:
                    db.close()
                except:
                    pass

        result = {"status": "success", "processed_reps": processed_reps}
        if errors:
            result["errors"] = errors
        return jsonify(result)
    except Exception as e:
        print(f"Error in process_dataset: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"}), 500
'''
'''
 Clear previous predictions
Insert Only the New Predictions
Avoid Duplicate Checks (not needed anymore)'''

# Processes a dataset and saves the predictions into a MySQL database.
@app.route('/process-dataset', methods=['POST'])
def process_data_and_save_result_in_database():
    try:
        # Optional repid parameter. If 'all' or omitted, process every rep in the dataset.
        repid = request.args.get('repid', None)
        csv_data = data_path
        print(f"[DEBUG] Reading CSV file: {csv_data}")

        # Check if file exists
        if not os.path.exists(csv_data):
            return jsonify({"status": "error", "message": f"CSV file not found: {csv_data}"}), 404

        full_data = pd.read_csv(csv_data)
        print(f"[DEBUG] Unique repids in dataset: {full_data['repcode'].astype(str).unique().tolist()}")

        # Validate repcode column exists
        if 'repcode' not in full_data.columns:
            return jsonify({"status": "error", "message": "Dataset does not contain a 'repcode' column."}), 400

        # ======================
        # CLEAR PREDICTION TABLE
        # ======================
        try:
            db_clear = database.get_connection()
            cursor_clear = db_clear.cursor()
            cursor_clear.execute("TRUNCATE TABLE suggested_order_2")  # Reset table every run
            db_clear.commit()
            cursor_clear.close()
            db_clear.close()
            print("[DEBUG] Cleared table: suggested_order")
        except Exception as clear_e:
            print(f"[ERROR] Failed to clear suggested_order: {clear_e}")
            return jsonify({"status": "error", "message": "Failed to clear suggested_order table."}), 500

        # Determine which repids to process
        available_repids = full_data['repcode'].astype(str).unique().tolist()
        if repid is None or str(repid).lower() == 'all':
            repids_to_process = available_repids
        else:
            if str(repid) not in available_repids:
                print(f"[DEBUG] Rep ID {repid} not found in dataset repcode column.")
                return jsonify({"status": "error", "message": f"Rep ID {repid} not found in dataset. Please check your REP ID"}), 400
            repids_to_process = [str(repid)]

        processed_reps = []
        errors = {}

        # =================================
        # Process each rep separately
        # =================================
        for rep in repids_to_process:
            try:
                rep_str = str(rep)
                rep_data = full_data[full_data['repcode'].astype(str) == rep_str]

                if rep_data.empty:
                    print(f"[DEBUG] No data for rep {rep_str}, skipping.")
                    continue

                db = database.get_connection()
                cursor = db.cursor()
                outlet_ids = list(rep_data['customercode'].unique())

                for outlet_id, outlet_data in rep_data.groupby("customercode"):
                    predicted_data = []
                    data = outlet_data

                    extracted_dataset = extract_dataset(data, outlet_id)
                    cleaned_data, ProductIDs, next_visit_date, outlier_product_ids = clean_dataset(extracted_dataset, visit_freq)
                    predicted_df = prediction(ProductIDs, cleaned_data, next_visit_date, visit_freq)

                    if predicted_df.empty:
                        print("Warning: Predicted DataFrame is empty for outlet {0} rep {1}".format(outlet_id, rep_str))
                        continue

                    for idx, row in predicted_df.iterrows():
                        try:
                            next_visit_date_value = row['Next_Visit_Date'] if pd.notnull(row['Next_Visit_Date']) else None
                        except KeyError:
                            print(f"Warning: 'Next_Visit_Date' column not found in row {idx}. Available columns: {row.index.tolist()}")
                            continue

                        if next_visit_date_value is not None:
                            if not isinstance(next_visit_date_value, pd.Timestamp):
                                try:
                                    next_visit_date_dt = pd.to_datetime(next_visit_date_value)
                                except Exception:
                                    next_visit_date_dt = None
                            else:
                                next_visit_date_dt = next_visit_date_value
                        else:
                            next_visit_date_dt = None

                        if next_visit_date_dt is not None:
                            next_visit_date_str = next_visit_date_dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            next_visit_date_str = None

                        # Append for insert (duplicate not needed to check since full table cleared)
                        predicted_data.append((rep_str, outlet_id, row['ProductID'], row.get('predicted_qty', None), next_visit_date_str))

                    if predicted_data:
                        cursor.executemany(
                            "INSERT INTO suggested_order_2(repid,outletid,product,Qty,Next_Visit_Date) VALUES (%s, %s, %s, %s, %s)",
                            predicted_data
                        )
                        db.commit()

                db.close()
                processed_reps.append(rep_str)

            except Exception as rep_e:
                print(f"Error processing rep {rep}: {rep_e}")
                traceback.print_exc()
                errors[str(rep)] = str(rep_e)
                try:
                    db.close()
                except:
                    pass

        result = {"status": "success", "processed_reps": processed_reps}
        if errors:
            result["errors"] = errors

        return jsonify(result)

    except Exception as e:
        print(f"Error in process_dataset: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"}), 500


@app.route('/lookup-prediction', methods=['POST'])
def lookup_prediction():
    """Lookup predictions for a given repid, outletid and date.

    Request JSON: { "repid": "<repid>", "outletid": "<outletid>", "date": "YYYY-MM-DD" }
    Returns rows from `suggested_order` where `repid` and `outletid` match and
    `Next_Visit_Date` is within ±2 days of the provided date.
    """
    try:
        payload = request.get_json(force=True)
        repid = str(payload.get('repid')) if payload.get('repid') is not None else None
        outletid = payload.get('outletid')
        date_str = payload.get('date')

        if not repid or not outletid or not date_str:
            return jsonify({"status": "error", "message": "Missing required fields: repid, outletid, date"}), 400

        import datetime
        try:
            # Parse provided date (accepts YYYY-MM-DD or full ISO)
            provided_date = datetime.datetime.fromisoformat(date_str)
        except Exception:
            try:
                provided_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            except Exception:
                return jsonify({"status": "error", "message": "Invalid date format. Use YYYY-MM-DD or ISO format."}), 400

        # Create a window of ±2 days (inclusive)
        start_dt = (provided_date - datetime.timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = (provided_date + datetime.timedelta(days=2)).replace(hour=23, minute=59, second=59, microsecond=0)

        db = database.get_connection()
        cursor = db.cursor()
        # query = (
        #     "SELECT product, Qty, Next_Visit_Date FROM suggested_order "
        #     "WHERE repid=%s AND outletid=%s AND Next_Visit_Date IS NOT NULL "
        #     "AND Next_Visit_Date BETWEEN %s AND %s"
        # )
        query = (
            "SELECT product, Qty FROM suggested_order_2 "
            "WHERE repid=%s AND outletid=%s AND Next_Visit_Date IS NOT NULL "
            "AND Next_Visit_Date BETWEEN %s AND %s"
        )
        cursor.execute(query, (repid, outletid, start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')))
        rows = cursor.fetchall()
        db.close()

        # Format results as list of dicts
        # results = []
        # for r in rows:
        #     prod, qty, next_visit = r
        #     # Ensure next_visit is string
        #     if hasattr(next_visit, 'strftime'):
        #         next_visit_str = next_visit.strftime('%Y-%m-%d %H:%M:%S')
        #     else:
        #         next_visit_str = str(next_visit)
        #     results.append({"product": prod, "Qty": qty, "Next_Visit_Date": next_visit_str})

        # return jsonify({"status": "success", "provided_date": provided_date.strftime('%Y-%m-%d'), "window": {"start": start_dt.strftime('%Y-%m-%d %H:%M:%S'), "end": end_dt.strftime('%Y-%m-%d %H:%M:%S')}, "predictions": results})

        results = []
        for r in rows:
            prod, qty= r
            results.append({"product": prod, "Qty": qty})
        return jsonify({"status": "success", "predictions": results})
        # return jsonify({"status": "success", "provided_date": provided_date.strftime('%Y-%m-%d'), "window": {"start": start_dt.strftime('%Y-%m-%d %H:%M:%S'), "end": end_dt.strftime('%Y-%m-%d %H:%M:%S')}, "predictions": results})
    except Exception as e:
        print(f"Error in lookup_prediction: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"}), 500



if __name__ == '__main__':
    print("Starting Flask application...")
    app.run(debug=True, host='0.0.0.0', port=7205)