from flask import Flask, request, jsonify
import pandas as pd
import os
import traceback

# Import custom modules
try:
    from next_order.data_handling.extract_data import extract_dataset
    from next_order.data_handling.clean_data import clean_dataset
    # from next_order.prediction_and_output.prediction import prediction
    from next_order.prediction_and_output.prediction2 import prediction
    from next_order.prediction_and_output.output import output
    import database                    
    import database_source             
except ImportError as e:
    print(f"Import error: {e}")
    traceback.print_exc()

app = Flask(__name__)

visit_freq = 7

#        PROCESS DATASET FROM DATABASE INSTEAD OF CSV
@app.route('/process-dataset', methods=['POST'])
def process_data_and_save_result_in_database():
    try:       
        repid_param = request.args.get('repid', None)

        print("[DEBUG] Connecting to SOURCE database to fetch sales data...")

        # LOAD DATASET FROM SOURCE DATABASE (NOT CSV)
    
        try:
            src_db = database_source.get_source_connection()
            src_cursor = src_db.cursor(dictionary=True)

            # Table query
            # query = """SELECT Date as date,
            #             InvoiceNo as invoiceno,
            #             CustomerCode as customercode,
            #             CustomerName as customername,
            #             ProductCode as productcode,
            #             ProductName as productname,
            #             Qty as qty,
            #             RepCode as repcode 
            #             FROM sales_flat where Date>='2025-05-01' and Date<='2025-12-01';
                        # """
            query = """
                    select i.Date     as date,
                        i.SerialNo as invoiceno,
                        e.code     as customercode,
                        e.name     as ccustomername,
                        p.code     as productcode,
                        p.name     as productname,
                        il.qty     as qty,
                        shn.code   as repcode
                    from invoice_lines il
                            inner join invoices i on i.id = il.invoiceid
                            inner join external_parties e on e.id = i.OtherPartyId
                            inner join products p on p.id = il.ProductId
                            inner join sales_hierarchy_nodes shn on shn.id = i.SalesHierarchyNodeId
                    where i.Date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
                    AND i.Date <  CURDATE()
                    and i.deleted = 0
                    and i.TypeId = 103
        """     
            src_cursor.execute(query)
             
            rows = src_cursor.fetchall()
            src_cursor.close()
            src_db.close()

            if not rows:
                return jsonify({"status": "error", "message": "No data returned from source DB"}), 400

            # Convert DB rows → DataFrame
            full_data = pd.DataFrame(rows)

            print(f"[DEBUG] Loaded {len(full_data)} rows from sales_flat")

        except Exception as db_e:
            print(f"[ERROR] Source DB error: {db_e}")
            traceback.print_exc()
            return jsonify({"status": "error", "message": "Failed to read data from source database"}), 500

        # Validate repcode column
        if 'repcode' not in full_data.columns:
            return jsonify({"status": "error", "message": "'repcode' column missing from dataset"}), 400

        
        # CLEAR TARGET TABLE BEFORE INSERTING
       
        try:
            db_clear = database.get_connection()
            cursor_clear = db_clear.cursor()
            cursor_clear.execute("TRUNCATE TABLE suggested_order")
            db_clear.commit()
            cursor_clear.close()
            db_clear.close()
            print("[DEBUG] Cleared table: suggested_order")
        except Exception as clear_e:
            print(f"[ERROR] Failed to clear suggested_order: {clear_e}")
            return jsonify({"status": "error", "message": "Failed to clear suggested_order table"}), 500

        # Determine which repcodes to process
        available_repids = full_data['repcode'].astype(str).unique().tolist()
        if repid_param is None or str(repid_param).lower() == 'all':
            repids_to_process = available_repids
        else:
            if repid_param not in available_repids:
                return jsonify({"status": "error", "message": f"Rep ID {repid_param} not found in dataset"}), 400
            repids_to_process = [repid_param]

        processed_reps = []
        errors = {}
        # aggregate model usage counts across the whole run (console-only)
        total_model_usage = {}

        
        # PROCESS REPS
        for rep in repids_to_process:
            try:
                rep_str = str(rep)
                rep_data = full_data[full_data['repcode'].astype(str) == rep_str]

                if rep_data.empty:
                    print(f"[DEBUG] No data for rep {rep_str}, skipping.")
                    continue

                db = database.get_connection()
                cursor = db.cursor()

                # Group by outlet
                for outlet_id, outlet_data in rep_data.groupby("customercode"):

                    predicted_data = []
                    extracted_dataset = extract_dataset(outlet_data, outlet_id)
                    cleaned_data, ProductIDs, next_visit_date, outlier_ids = clean_dataset(
                        extracted_dataset, visit_freq
                    )
                    # predicted_df = prediction(ProductIDs, cleaned_data, next_visit_date, visit_freq)
                    predicted_df = prediction(ProductIDs, cleaned_data, next_visit_date, visit_freq)

                    # Aggregate model usage counts (console-only)
                    if not predicted_df.empty and 'model_used' in predicted_df.columns:
                        try:
                            vc = predicted_df['model_used'].value_counts().to_dict()
                            for k, v in vc.items():
                                total_model_usage[k] = total_model_usage.get(k, 0) + int(v)
                        except Exception as e:
                            print(f"[WARN] Failed to aggregate model counts for outlet {outlet_id}: {e}")

                    if predicted_df.empty:
                        print(f"[DEBUG] No predictions for outlet {outlet_id}")
                        continue

                    # Build insert list
                    for idx, row in predicted_df.iterrows():
                        next_visit_val = row.get('Next_Visit_Date')
                        try:
                            next_visit_dt = pd.to_datetime(next_visit_val) if pd.notnull(next_visit_val) else None
                            next_visit_str = next_visit_dt.strftime('%Y-%m-%d %H:%M:%S') if next_visit_dt else None
                        except:
                            next_visit_str = None

                        predicted_data.append((
                            rep_str,
                            outlet_id,
                            row['ProductID'],
                            row.get('predicted_qty', None),
                            next_visit_str
                        ))

                    # INSERT INTO TARGET DATABASE
                    if predicted_data:
                        cursor.executemany(
                            "INSERT INTO suggested_order(repid,outletid,product,Qty,Next_Visit_Date) "
                            "VALUES (%s,%s,%s,%s,%s)",
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

        # Print aggregated model usage summary to console
        if total_model_usage:
            print("Aggregated model usage across run:")
            for m, cnt in sorted(total_model_usage.items(), key=lambda x: -x[1]):
                print(f"  {m}: {cnt} products")
        else:
            print("No model usage recorded during this run.")

        result = {"status": "success", "processed_reps": processed_reps}
        if errors:
            result["errors"] = errors

        return jsonify(result)

    except Exception as e:
        print(f"Error in process_dataset: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

#  PREDICTION LOOKUP (unchanged)

@app.route('/lookup-prediction', methods=['POST'])
def lookup_prediction():
    try:
        payload = request.get_json(force=True)
        repid = str(payload.get('repid')) if payload.get('repid') else None
        outletid = payload.get('outletid')
        date_str = payload.get('date')

        if not repid or not outletid or not date_str:
            return jsonify({"status": "error", "message": "Missing required fields"}), 400

        import datetime
        try:
            provided_date = datetime.datetime.fromisoformat(date_str)
        except:
            provided_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

        start_dt = provided_date - datetime.timedelta(days=2)
        end_dt = provided_date + datetime.timedelta(days=2)

        db = database.get_connection()
        cursor = db.cursor()

        query = (
            "SELECT product, Qty FROM suggested_order "
            "WHERE repid=%s AND outletid=%s AND Next_Visit_Date BETWEEN %s AND %s"
        )
        cursor.execute(query, (
            repid, outletid,
            start_dt.strftime('%Y-%m-%d 00:00:00'),
            end_dt.strftime('%Y-%m-%d 23:59:59')
        ))

        results = [{"product": r[0], "Qty": r[1]} for r in cursor.fetchall()]
        db.close()

        return jsonify({"status": "success", "predictions": results})

    except Exception as e:
        print("Error in lookup_prediction:", e)
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500
    
if __name__ == '__main__':
    print("Starting Flask server...")
    app.run(debug=True, host='127.0.0.1', port=5000)
