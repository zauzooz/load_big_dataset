import pandas as pd
import numpy as np

def reduce_dataset(df, int_cast=False, obj_to_category=False):
  cols = df.columns.to_list()
  for col in cols:
    col_type = df[col].dtype
    if col_type != object and col_type.name != 'category' and 'datetime' not in col_type.name:
      c_min = df[col].min()
      c_max = df[col].max()
      treat_as_int = str(col_type)[:3] == 'int'
      # if int_cast and not treat_as_int:
      #   treat_as_int = check_if_integer(df[col])
      if treat_as_int:
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.uint8).min and c_max < np.iinfo(np.uint8).max:
                    df[col] = df[col].astype(np.uint8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.uint16).min and c_max < np.iinfo(np.uint16).max:
                    df[col] = df[col].astype(np.uint16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.uint32).min and c_max < np.iinfo(np.uint32).max:
                    df[col] = df[col].astype(np.uint32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
                elif c_min > np.iinfo(np.uint64).min and c_max < np.iinfo(np.uint64).max:
                    df[col] = df[col].astype(np.uint64)
      else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
    elif 'datetime' not in col_type.name and obj_to_category:
      df[col] = df[col].astype('category')
  return df

PATH = "your dataset path here" ## change it
chunks =[
    reduce_dataset(chunk)
    for chunk in pd.read_csv(PATH, chunksize=500_000)
]

df = pd.DataFrame()
while len(chunks)>0:
  df = pd.concat([df, chunks[0]])
  del chunks[0]
del chunks