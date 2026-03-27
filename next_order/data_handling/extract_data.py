import pandas as pd
from next_order.data_handling.clean_data import clean_dataset
# def extract_dataset(data,outlet_id,rep_id):
#   print(data.info())
#   extracted_data=data[['date','customercode','code','name','Qty','Rep_Code']]
#   extracted_data['Date']=pd.to_datetime(extracted_data['date'])
#   extracted_data.drop('date',axis=1,inplace=True)
#   extracted_data['Qty']=extracted_data['Qty'].astype('Int64')
#   extracted_data['ProductID']=extracted_data['code'].astype('str')
#   extracted_data.drop('code',axis=1,inplace=True)
#   extracted_data['OutletId']=extracted_data['customercode']
#   extracted_data.drop('customercode',axis=1,inplace=True)
#   extracted_data['ProductName']=extracted_data['name']
#   extracted_data.drop('name',axis=1,inplace=True)
#   extracted_data['Rep_Code']=extracted_data['Rep_Code'].astype('str')
#   extracted_data = extracted_data.groupby(['Date', 'ProductID', 'OutletId','ProductName','Rep_Code'])['Qty'].sum().reset_index()
#   print(f'extracted dataset info: {extracted_data.info()}')
#   extracted_data_oid=extracted_data[extracted_data.OutletId==outlet_id]
#   extracted_data_oid.drop('OutletId',axis=1,inplace=True)
#   extracted_data_oid_rid=extracted_data_oid[extracted_data_oid.Rep_Code==rep_id]
#   extracted_data_oid_rid.drop('Rep_Code',axis=1,inplace=True)
#   print(f'extracted dataset info: {extracted_data_oid_rid.info()}')
#   # clean_data(extracted_data_oid_rid)
#   return extracted_data_oid_rid



def extract_dataset(data,outlet_id):
  print(data.info())
  extracted_data=data[['date','customercode','productcode','productname','qty','repcode']]
  extracted_data['Date']=pd.to_datetime(extracted_data['date'])
  extracted_data.drop('date',axis=1,inplace=True)
  extracted_data['Qty']=extracted_data['qty'].astype('Int64')
  extracted_data['ProductID']=extracted_data['productcode'].astype('str')
  extracted_data.drop('productcode',axis=1,inplace=True)
  extracted_data['OutletId']=extracted_data['customercode']
  extracted_data.drop('customercode',axis=1,inplace=True)
  extracted_data['ProductName']=extracted_data['productname']
  extracted_data.drop('productname',axis=1,inplace=True)
  extracted_data['Rep_Code']=extracted_data['repcode'].astype('str')
  extracted_data = extracted_data.groupby(['Date', 'ProductID', 'OutletId','ProductName','Rep_Code'])['Qty'].sum().reset_index()
  print(f'extracted dataset info: {extracted_data.info()}')
  extracted_data_oid=extracted_data[extracted_data.OutletId==outlet_id]
  extracted_data_oid.drop('OutletId',axis=1,inplace=True)
  # extracted_data_oid_rid=extracted_data_oid[extracted_data_oid.Rep_Code==rep_id]
  # extracted_data_oid_rid.drop('Rep_Code',axis=1,inplace=True)
  print(f'extracted dataset info: {extracted_data_oid.info()}')
  return extracted_data_oid
