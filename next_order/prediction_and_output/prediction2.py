import pandas as pd
from datetime import datetime, timedelta
from scipy.stats import skew
from next_order.prediction_and_output import output

# def prediction(ProductIDs,cleaned_data,next_visit_date,extracted_data_test):

#   df = []

#   # Assuming ProductIDs and flat_data_extracted_oid are defined
#   for product in ProductIDs:
#       # Filter data for the current product
#       flat_data_extracted_oid_pid = cleaned_data[cleaned_data['ProductID'] == product]
#       print(f"Processing ProductID: {product}")

#       if len(flat_data_extracted_oid_pid) >= 2:
#           # Ensure 'Date' column is in datetime format and sort
#           flat_data_extracted_oid_pid['Date'] = pd.to_datetime(flat_data_extracted_oid_pid['Date'])
#           flat_data_extracted_oid_pid = flat_data_extracted_oid_pid.sort_values(by='Date')

#           # Calculate time differences
#           flat_data_extracted_oid_pid['Time_Difference'] = flat_data_extracted_oid_pid['Date'].diff().dt.days

#           # Calculate metrics
#           average_gap = flat_data_extracted_oid_pid['Time_Difference'].mean()
#           median_gap = flat_data_extracted_oid_pid['Time_Difference'].median()
#           maximum_gap = flat_data_extracted_oid_pid['Time_Difference'].max()
#           last_purchase_date = flat_data_extracted_oid_pid['Date'].max()

#           # Predict next purchase date
#           next_purchase_date = last_purchase_date + timedelta(days=int(median_gap))

#           # Quantity metrics
#           mean_qty = flat_data_extracted_oid_pid['Qty'].mean()
#           median_qty = flat_data_extracted_oid_pid['Qty'].median()
#           skewness_qty = skew(flat_data_extracted_oid_pid['Qty'])

#           # Predict quantity based on skewness
#           predicted_qty = round(mean_qty) if abs(skewness_qty) < 0.5 else round(median_qty)

#           # Extract YearMonth for grouping
#           flat_data_extracted_oid_pid['YearMonth'] = flat_data_extracted_oid_pid['Date'].dt.to_period('M')

#           # Group by ProductID and YearMonth for sum and mean
#           sum_qty_per_month = flat_data_extracted_oid_pid.groupby(['ProductID', 'YearMonth'])['Qty'].sum().reset_index()
#           mean_qty_per_month = flat_data_extracted_oid_pid.groupby(['ProductID', 'YearMonth'])['Qty'].mean().reset_index()
#           count_visit_per_month=flat_data_extracted_oid_pid.groupby(['ProductID', 'YearMonth'])['Date'].count().reset_index()

#           # Merge monthly summaries
#           monthly_Qty = pd.merge(sum_qty_per_month, mean_qty_per_month, on=['ProductID', 'YearMonth'])
#           monthly_Qty = pd.merge(monthly_Qty, count_visit_per_month, on=['ProductID', 'YearMonth'])

#           monthly_Qty.columns = ['ProductID', 'YearMonth', 'Qty_sum', 'Qty_mean','visits per month']


#           # Condition to append to results
#           # if next_purchase_date>next_visit_date:
#           #   threshold =(next_purchase_date-next_visit_date).days/6
#           # else:
#           #   threshold =(next_visit_date-next_purchase_date).days/6
#           threshold = median_gap / 7
#           # threshold = average_gap / 7
#           # threshold=(next_visit_date-last_visit_date).days/6
#           # threshold = 10
#           print(f"threshold: {threshold}")
#           round(threshold)
#           from_next_visit_date_to_last_purchase_date_gap = next_visit_date - last_purchase_date

#           if (
#               from_next_visit_date_to_last_purchase_date_gap + timedelta(days=int(threshold)) >= timedelta(days=int(median_gap)) and
#               from_next_visit_date_to_last_purchase_date_gap < timedelta(maximum_gap) + timedelta(days=int(threshold))
#           ):
#               df.append([
#                   product, last_purchase_date,next_purchase_date, next_visit_date, average_gap,
#                   median_gap, maximum_gap, from_next_visit_date_to_last_purchase_date_gap,predicted_qty
#               ])
#       else:
#           print(f"Not enough data for ProductID: {product}")
#   df=pd.DataFrame(df)
#   if not df.empty:
#       df.columns=['ProductID', 'last_purchase_date','next_purchase_date','next_visit_date','average_gap','Median gap','Maximum_gap','next_visit_date_to_last_purchase_date_gap','predicted_qty']
#       df=pd.merge(df,extracted_data_test[['ProductID','Qty']],on='ProductID',how='left')
#       df.rename(columns={'Qty': 'actual_qty'}, inplace=True)
#       df['actual_qty'].fillna(0, inplace=True)
#       df
# #   output(df)
#   return df

# def prediction(ProductIDs,cleaned_data,next_visit_date,visit_freq):

#   df = []
#   if not isinstance(next_visit_date, pd.Timestamp):
#         next_visit_date = pd.Timestamp(next_visit_date)

#   # Assuming ProductIDs and flat_data_extracted_oid are defined
#   for product in ProductIDs:
#       # Filter data for the current product
#       flat_data_extracted_oid_pid = cleaned_data[cleaned_data['ProductID'] == product]
#       print(f"Processing ProductID: {product}")

#       if len(flat_data_extracted_oid_pid) >= 2:
#           # Ensure 'Date' column is in datetime format and sort
#           flat_data_extracted_oid_pid['Date'] = pd.to_datetime(flat_data_extracted_oid_pid['Date'])
#           flat_data_extracted_oid_pid = flat_data_extracted_oid_pid.sort_values(by='Date')

#           # Calculate time differences
#           flat_data_extracted_oid_pid['Time_Difference'] = flat_data_extracted_oid_pid['Date'].diff().dt.days

#           # Calculate metrics
#           average_gap = flat_data_extracted_oid_pid['Time_Difference'].mean()
#           median_gap = flat_data_extracted_oid_pid['Time_Difference'].median()
#           maximum_gap = flat_data_extracted_oid_pid['Time_Difference'].max()
#           last_purchase_date = flat_data_extracted_oid_pid['Date'].max()

#           # Predict next purchase date
#           next_purchase_date = last_purchase_date + timedelta(days=int(median_gap))

#           # Quantity metrics
#           mean_qty = flat_data_extracted_oid_pid['Qty'].mean()
#           median_qty = flat_data_extracted_oid_pid['Qty'].median()
#           skewness_qty = skew(flat_data_extracted_oid_pid['Qty'])

#           # Predict quantity based on skewness
#           predicted_qty = round(mean_qty) if abs(skewness_qty) < 0.5 else round(median_qty)

#           # Extract YearMonth for grouping
#           flat_data_extracted_oid_pid['YearMonth'] = flat_data_extracted_oid_pid['Date'].dt.to_period('M')

#           # Group by ProductID and YearMonth for sum and mean
#           sum_qty_per_month = flat_data_extracted_oid_pid.groupby(['ProductID', 'YearMonth'])['Qty'].sum().reset_index()
#           mean_qty_per_month = flat_data_extracted_oid_pid.groupby(['ProductID', 'YearMonth'])['Qty'].mean().reset_index()
#           count_visit_per_month=flat_data_extracted_oid_pid.groupby(['ProductID', 'YearMonth'])['Date'].count().reset_index()

#           # Merge monthly summaries
#           monthly_Qty = pd.merge(sum_qty_per_month, mean_qty_per_month, on=['ProductID', 'YearMonth'])
#           monthly_Qty = pd.merge(monthly_Qty, count_visit_per_month, on=['ProductID', 'YearMonth'])

#           monthly_Qty.columns = ['ProductID', 'YearMonth', 'Qty_sum', 'Qty_mean','visits per month']


#           # Condition to append to results
#           # if next_purchase_date>next_visit_date:
#           #   threshold =(next_purchase_date-next_visit_date).days/6
#           # else:
#           #   threshold =(next_visit_date-next_purchase_date).days/6
#           threshold = visit_freq / 2
#           # threshold = average_gap / 7
#           # threshold=(next_visit_date-last_visit_date).days/6
#           # threshold = 10
#           print(f"threshold: {threshold}")
#           round(threshold)
#           from_next_visit_date_to_last_purchase_date_gap = next_visit_date - last_purchase_date

#           if (
#               from_next_visit_date_to_last_purchase_date_gap + timedelta(days=int(threshold)) >= timedelta(days=int(median_gap)) and
#               from_next_visit_date_to_last_purchase_date_gap < timedelta(maximum_gap) + timedelta(days=int(threshold))
#           ):
#               df.append([
#                   product, last_purchase_date,next_purchase_date, next_visit_date, average_gap,
#                   median_gap, maximum_gap, from_next_visit_date_to_last_purchase_date_gap,predicted_qty
#               ])
#       else:
#           print(f"Not enough data for ProductID: {product}")
          
#   df=pd.DataFrame(df)
#   if not df.empty:
#       df.columns=['ProductID', 'last_purchase_date','next_purchase_date','Next_Visit_Date','average_gap','Median_gap','Maximum_gap','next_visit_date_to_last_purchase_date_gap','predicted_qty']
#     #   df=pd.merge(df,extracted_data_test[['ProductID','Qty']],on='ProductID',how='left')
#     #   df.rename(columns={'Qty': 'actual_qty'}, inplace=True)
#     #   df['actual_qty'].fillna(0, inplace=True)
#   else:
#       print("Warning: No predictions generated - DataFrame is empty")
#   return df


def prediction(ProductIDs, cleaned_data, next_visit_date, visit_freq):

    df = []
    # counts of models actually used for products that passed the threshold and were added to results
    model_counts = {}
    if not isinstance(next_visit_date, pd.Timestamp):
        next_visit_date = pd.Timestamp(next_visit_date)

    for product in ProductIDs:
        flat_data_extracted_oid_pid = cleaned_data[cleaned_data['ProductID'] == product]
        print(f"Processing ProductID: {product}")

        if len(flat_data_extracted_oid_pid) >= 2:
            flat_data_extracted_oid_pid['Date'] = pd.to_datetime(flat_data_extracted_oid_pid['Date'])
            flat_data_extracted_oid_pid = flat_data_extracted_oid_pid.sort_values(by='Date')

            flat_data_extracted_oid_pid['Time_Difference'] = flat_data_extracted_oid_pid['Date'].diff().dt.days

            average_gap = flat_data_extracted_oid_pid['Time_Difference'].mean()
            median_gap = flat_data_extracted_oid_pid['Time_Difference'].median()
            maximum_gap = flat_data_extracted_oid_pid['Time_Difference'].max()
            last_purchase_date = flat_data_extracted_oid_pid['Date'].max()
             # Quantity metrics
            mean_qty = flat_data_extracted_oid_pid['Qty'].mean()
            median_qty = flat_data_extracted_oid_pid['Qty'].median()
            skewness_qty = skew(flat_data_extracted_oid_pid['Qty'])

            # ---------------- MODEL SELECTION (train/test) ----------------
            # Prepare time series and define train/test split (time-based split: last N records as test)
            ts = flat_data_extracted_oid_pid[['Date', 'Qty']].sort_values(by='Date').reset_index(drop=True)
            n = len(ts)
            # use last max(1, 20%) as test if enough data, else last 1
            if n >= 5:
                test_size = max(1, int(n * 0.2))
            else:
                test_size = 1
            train_ts = ts.iloc[:-test_size].copy()
            test_ts = ts.iloc[-test_size:].copy()
            # default assume prophet not available until explicitly checked
            prophet_available = False

            # ensure we have at least one train record
            if train_ts.empty:
                # fallback to simple skewness approach
                predicted_qty = round(mean_qty) if abs(skewness_qty) < 0.5 else round(median_qty)
                best_model = 'skewness_fallback'
            else:
                # evaluate three methods: skewness, moving average (MA), prophet (if available)
                errors = {}
                preds = {}

                # 1) skewness method (train stats)
                train_mean = train_ts['Qty'].mean()
                train_median = train_ts['Qty'].median()
                skew_train = skew(train_ts['Qty'])
                skew_pred = round(train_mean) if abs(skew_train) < 0.5 else round(train_median)
                preds['skewness'] = [skew_pred] * test_size
                errors['skewness'] = float((test_ts['Qty'] - preds['skewness']).abs().mean())

                # 2) moving average model - try windows and pick best window on train (validate on test)
                ma_best_err = float('inf')
                ma_best_k = 1
                ma_pred_val = round(train_ts['Qty'].tail(1).mean())
                for k in [2, 3, 4, 5]:
                    if len(train_ts) >= k:
                        val = round(train_ts['Qty'].tail(k).mean())
                        candidate_preds = [val] * test_size
                        err = float((test_ts['Qty'] - candidate_preds).abs().mean())
                        if err < ma_best_err:
                            ma_best_err = err
                            ma_best_k = k
                            ma_pred_val = val
                preds['ma'] = [ma_pred_val] * test_size
                errors['ma'] = ma_best_err

                # 3) Prophet model (if available)
                prophet_available = True
                try:
                    from prophet import Prophet
                except Exception:
                    try:
                        from fbprophet import Prophet
                    except Exception:
                        prophet_available = False

                if prophet_available:
                    try:
                        # prophet requires a DataFrame with ds and y
                        m = Prophet(yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False)
                        prophet_train = train_ts.rename(columns={'Date': 'ds', 'Qty': 'y'})[['ds', 'y']]
                        m.fit(prophet_train)
                        future = test_ts.rename(columns={'Date': 'ds'})[['ds']]
                        forecast = m.predict(future)
                        pf_preds = forecast['yhat'].round().astype(int).tolist()
                        preds['prophet'] = pf_preds
                        errors['prophet'] = float((test_ts['Qty'] - pd.Series(pf_preds)).abs().mean())
                    except Exception:
                        prophet_available = False

                # choose best by lowest MAE
                if not prophet_available:
                    # remove prophet if present
                    errors = {k: v for k, v in errors.items() if k in ['skewness', 'ma']}
                best_model = min(errors, key=errors.get)

                # Retrain best model on full series and predict for next_visit_date
                if best_model == 'skewness':
                    # use stats from full series
                    full_mean = flat_data_extracted_oid_pid['Qty'].mean()
                    full_median = flat_data_extracted_oid_pid['Qty'].median()
                    predicted_qty = round(full_mean) if abs(skew(flat_data_extracted_oid_pid['Qty'])) < 0.5 else round(full_median)
                elif best_model == 'ma':
                    # use best k found earlier on train, but recompute k against full data: use same k as ma_best_k
                    k = ma_best_k
                    k = min(k, len(flat_data_extracted_oid_pid['Qty']))
                    predicted_qty = round(flat_data_extracted_oid_pid['Qty'].tail(k).mean())
                elif best_model == 'prophet' and prophet_available:
                    try:
                        # retrain on full series and forecast next_visit_date
                        m = Prophet(yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False)
                        prophet_full = flat_data_extracted_oid_pid.rename(columns={'Date': 'ds', 'Qty': 'y'})[['ds', 'y']]
                        m.fit(prophet_full)
                        future_df = pd.DataFrame({'ds': [pd.to_datetime(next_visit_date)]})
                        forecast = m.predict(future_df)
                        predicted_qty = int(round(float(forecast['yhat'].iloc[0])))
                    except Exception:
                        # fallback
                        predicted_qty = round(mean_qty) if abs(skewness_qty) < 0.5 else round(median_qty)
                else:
                    # fallback
                    predicted_qty = round(mean_qty) if abs(skewness_qty) < 0.5 else round(median_qty)

            # end model selection


            next_purchase_date = last_purchase_date + timedelta(days=int(median_gap))

            threshold = visit_freq / 2
            print(f"threshold: {threshold}")
            round(threshold)
            from_next_visit_date_to_last_purchase_date_gap = next_visit_date - last_purchase_date

            if (
                from_next_visit_date_to_last_purchase_date_gap + timedelta(days=int(threshold)) >= timedelta(days=int(median_gap))
                and from_next_visit_date_to_last_purchase_date_gap < timedelta(maximum_gap) + timedelta(days=int(threshold))
            ):
                # print which model was used for this product
                print(f"Product {product}: predicted_qty={predicted_qty} calculated using model '{best_model}'")
                if not prophet_available:
                    print(f"Product {product}: Prophet not available; Prophet was skipped for model selection")
                # increment model usage count for summary
                model_counts[best_model] = model_counts.get(best_model, 0) + 1
                df.append([
                    product, last_purchase_date, next_purchase_date, next_visit_date, average_gap,
                    median_gap, maximum_gap, from_next_visit_date_to_last_purchase_date_gap, predicted_qty, best_model
                ])
        else:
            print(f"Not enough data for ProductID: {product}")

    df = pd.DataFrame(df)
    if not df.empty:
        df.columns = ['ProductID', 'last_purchase_date', 'next_purchase_date', 'Next_Visit_Date', 
                      'average_gap', 'Median_gap', 'Maximum_gap', 
                      'next_visit_date_to_last_purchase_date_gap', 'predicted_qty', 'model_used']
        # print summary of how many products used each model
        print("Model usage summary:")
        for m, cnt in model_counts.items():
            print(f"  {m}: {cnt} products")
    else:
        print("Warning: No predictions generated - DataFrame is empty")
    return df
