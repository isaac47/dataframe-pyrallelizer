import pandas as pd
import numpy as np
import itertools
from multiprocessing import  Pool, cpu_count 
from tqdm import tqdm, tqdm_notebook
#We need tqdm package for bar visualization purpose
try:
    tqdm_notebook.tqdm.pandas(desc="chunk process")
except:
    tqdm.pandas(desc="chunk process")


def select_cores(n_cpu):
    if n_cpu == -1:
        return cpu_count()
    elif n_cores > 1:
        return n_cores
    else:
        return 'Error : the number of cores is not well define.'

def func(df, _args):
    column = _args[0]
    c_func = _args[1]
    col_apply_on = _args[2]

    df[column] = df[col_apply_on].apply(c_func, axis=1)
    return df

def func_processor(args):
    return func(*args)

def parallelize_dataframe(df, func_processor=func_processor, n_cpu=-1, **kwargs):
    try:
        if kwargs['output_column']:
            if isinstance(kwargs['output_column'],str):
                column = kwargs['output_column']
            else:
                return 'Error : Column name output is not well define.'
    except:
        column = 'pyparallelizer_col'
    try:
        col_apply_on = kwargs['input_columns']
    except:
        col_apply_on = list(df.columns)
            
    _args = [column,kwargs['function_ToApply'],col_apply_on]
    n_cores = select_cores(n_cpu)
    
    if len(df) < n_cores:
        n_cores = len(df)
    df_split = np.array_split(df, n_cores)
    with Pool(n_cores) as pool:
        df = pd.concat(pool.map(func_processor, zip(df_split, itertools.repeat(_args))))
    return df

def func_to_apply(x,args_list):
    #your function
    return x

if __name__ == '__main__':

    v = Pyparallelizer()
    dfc = v.parallelize_dataframe(dfc,
                                  function_to_Apply=func_to_apply, 
                                  output_column='outplut_col', 
                                  input_columns=['text'],
                                  func_args = [])

