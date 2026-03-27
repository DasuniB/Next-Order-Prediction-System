def output(df):
  results_df = df[['ProductID', 'predicted_qty']]
  return results_df