# import numpy as np

# def clean_dataset(extracted_data):
#   # extracted_data.shape
#   next_visit_date=extracted_data['Date'].max()
#   # print(f'Next visit date : {next_visit_date}')
#   extracted_data_test=extracted_data[extracted_data['Date']==next_visit_date]
#   extracted_data = extracted_data[extracted_data['Date'] != next_visit_date]
#   ProductIDs_count=extracted_data['ProductID'].value_counts()
#   ProductIDs_count
#   ProductIDs_filterd_count = ProductIDs_count[ProductIDs_count >= 2]
#   ProductIDs_filterd_count
#   filtered_products=extracted_data['ProductID'].isin(ProductIDs_filterd_count.index)
#   extracted_data= extracted_data[filtered_products]
#   ProductIDs=extracted_data['ProductID'].unique()
#   ProductIDs

#   outlier_product_ids = []
#   for product in ProductIDs:
#     extracted_data_pid = extracted_data[extracted_data['ProductID'] == product]
#     extracted_data_pid =extracted_data_pid.sort_values(by='Date')
#     extracted_data_pid['Time_Difference'] = extracted_data_pid['Date'].diff().dt.days
#     # Calculate IQR for Time_Difference
#     Q1 = extracted_data_pid['Time_Difference'].quantile(0.25)
#     Q3 = extracted_data_pid['Time_Difference'].quantile(0.75)
#     IQR = Q3 - Q1
#     lower_bound = Q1 - 3.5 * IQR
#     upper_bound = Q3 + 3.5 * IQR

#     # Identify outliers
#     outliers = extracted_data_pid[
#         (extracted_data_pid['Time_Difference'] < lower_bound) |
#         (extracted_data_pid['Time_Difference'] > upper_bound)
#     ]

#     # Remove outlier product codes
#     outlier_product_ids_current = outliers['ProductID'].unique()
#     outlier_product_ids.extend(outlier_product_ids_current)

#   # print(f"Outlier product codes: {outlier_product_ids}")
#     # Remove rows with outlier product IDs from the original DataFrame
#   ProductIDs = ProductIDs[~np.isin(ProductIDs, outlier_product_ids)]

#   # predict.prediction(ProductIDs,extracted_data,next_visit_date,extracted_data_test)
#   return extracted_data,extracted_data_test,ProductIDs,next_visit_date,outlier_product_ids
from datetime import timedelta
import numpy as np


def clean_dataset(extracted_data,visit_freq=7):
  # extracted_data.shape
  # extracted_data.head()
  # extracted_data.tail()
  next_visit_date=extracted_data['Date'].max()+timedelta(days=visit_freq)
  # print(f'Next visit date : {next_visit_date}')
  # extracted_data_test=extracted_data[extracted_data['Date']==next_visit_date]
  # extracted_data = extracted_data[extracted_data['Date'] != next_visit_date]
  ProductIDs_count=extracted_data['ProductID'].value_counts()
  ProductIDs_count
  ProductIDs_filterd_count = ProductIDs_count[ProductIDs_count >= 2]
  ProductIDs_filterd_count
  filtered_products=extracted_data['ProductID'].isin(ProductIDs_filterd_count.index)
  extracted_data= extracted_data[filtered_products]
  ProductIDs=extracted_data['ProductID'].unique()
  ProductIDs

  outlier_product_ids = []
  for product in ProductIDs:
    extracted_data_pid = extracted_data[extracted_data['ProductID'] == product]
    extracted_data_pid =extracted_data_pid.sort_values(by='Date')
    extracted_data_pid['Time_Difference'] = extracted_data_pid['Date'].diff().dt.days
    # Calculate IQR for Time_Difference

    Q1 = extracted_data_pid['Time_Difference'].quantile(0.25)
    Q3 = extracted_data_pid['Time_Difference'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 3.5 * IQR
    
    upper_bound = Q3 + 3.5 * IQR

    # Identify outliers
    outliers = extracted_data_pid[
        (extracted_data_pid['Time_Difference'] < lower_bound) |
        (extracted_data_pid['Time_Difference'] > upper_bound)
    ]

    # Remove outlier product codes
    outlier_product_ids_current = outliers['ProductID'].unique()
    outlier_product_ids.extend(outlier_product_ids_current)

  # print(f"Outlier product codes: {outlier_product_ids}")
    # Remove rows with outlier product IDs from the original DataFrame
  ProductIDs = ProductIDs[~np.isin(ProductIDs, outlier_product_ids)]
  return extracted_data,ProductIDs,next_visit_date,outlier_product_ids
