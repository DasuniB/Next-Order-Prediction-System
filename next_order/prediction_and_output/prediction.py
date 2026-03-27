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

            # Predict quantity based on skewness
            predicted_qty = round(mean_qty) if abs(skewness_qty) < 0.5 else round(median_qty)
            
            # # ---------------- NEW QTY CALCULATION ---------------- #

            # # ORDER CYCLE = median gap from history
            # order_cycle = average_gap

            # # Extract last orders
            # qty_series = flat_data_extracted_oid_pid['Qty'].tail(5)
            
            # # if less than 3, skip product
            # if len(qty_series) < 3:
            #     print(f"Skipping ProductID {product} due to insufficient order history for qty calculation")
            #     continue

            # # if between 3 and 4 orders -> take last 3
            # if len(qty_series) < 5:
            #     qty_series = qty_series.tail(3)
            # else:
            #     qty_series = qty_series.tail(5)
            # # avg qty from selected orders
            # avg_qty = qty_series.mean()

            # # Days since last order
            # days_since_last_order = (next_visit_date - last_purchase_date).days

            # # Demand ratio
            # demand_ratio = days_since_last_order / order_cycle if order_cycle > 0 else 0

            # # Predicted Qty (ratio * average qty)
            # predicted_qty = demand_ratio * avg_qty

            # # Apply caps (1 to 2x avg qty)
            # predicted_qty = max(0, predicted_qty)               # lower cap
            # predicted_qty = min(predicted_qty, avg_qty * 2)     # upper cap

            # predicted_qty = round(predicted_qty)

            # # ---------------- END NEW QTY LOGIC ---------------- #

            next_purchase_date = last_purchase_date + timedelta(days=int(median_gap))

            threshold = visit_freq / 2
            print(f"threshold: {threshold}")
            round(threshold)
            from_next_visit_date_to_last_purchase_date_gap = next_visit_date - last_purchase_date

            if (
                from_next_visit_date_to_last_purchase_date_gap + timedelta(days=int(threshold)) >= timedelta(days=int(median_gap))
                and from_next_visit_date_to_last_purchase_date_gap < timedelta(maximum_gap) + timedelta(days=int(threshold))
            ):
                df.append([
                    product, last_purchase_date, next_purchase_date, next_visit_date, average_gap,
                    median_gap, maximum_gap, from_next_visit_date_to_last_purchase_date_gap, predicted_qty
                ])
        else:
            print(f"Not enough data for ProductID: {product}")

    df = pd.DataFrame(df)
    if not df.empty:
        df.columns = ['ProductID', 'last_purchase_date', 'next_purchase_date', 'Next_Visit_Date', 
                      'average_gap', 'Median_gap', 'Maximum_gap', 
                      'next_visit_date_to_last_purchase_date_gap', 'predicted_qty']
    else:
        print("Warning: No predictions generated - DataFrame is empty")
    return df
