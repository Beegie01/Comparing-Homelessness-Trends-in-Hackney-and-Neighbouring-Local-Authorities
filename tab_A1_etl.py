import copy
import pandas as pd
import numpy as np
from helper_utils import ProjUtil as pjl
from pg_settings import pg_var as pvr
from sqlalchemy import text

# configure display settings
pd.options.display.width = None
pd.options.display.max_columns = None
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 300)

def run_app (sample_filename: str="Detailed_LA_202503.ods"):
    # extract data from source to dataframe
    # root_dir = "https://godsvisionenterprise24-my.sharepoint.com/personal/o_aibangbee_godsvisionenterprise24_onmicrosoft_com/Documents/Documents/Workspace/IT Career/Cedarstone"
    root_dir = "/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Interview/Hackney Council"
    read_fdr = "SourceData"
    sample_fname = sample_filename
    sep = '/'
    file_path = sep.join([root_dir, read_fdr, sample_fname])
    shname = 'A1'
    col_name = "quarter_ending"

    # get quarter value from file name
    qtr_val = pjl.get_qtr_from_fname(file_path)
    # extract and clean data from most recent quarterly file
    # and use as model structure
    sample_qdf = pjl.clean_tab_a1(abs_file_path=file_path,
                                  sheet_name=shname,
                                  assign_col=col_name,
                                  assign_val=qtr_val)
    # print(sample_qdf.info())
    # print(sample_qdf.head(15))

    # append remaining quarterly records to sample quarter data
    src_fdr_path = sep.join([root_dir, read_fdr])
    filename_dict = pjl.get_full_path(folder_path=src_fdr_path, specify_ftype=["xls", "ods"])
    # print(filename_dict)

    # exclude sample file from dictionary list
    filename_dict.pop(sample_fname)
    # print(filename_dict)

    # append all sample quarter to other quarters
    all_qtr_df = pjl.append_to_sample_df(sample_df=sample_qdf,
                                         file_path_dict=filename_dict,
                                         assign_colname=col_name,
                                         sheet_name=shname,
                                         use_func='a1').sort_values(by="quarter_ending")

    # print(all_qtr_df.info())
    # print(all_qtr_df['quarter_ending'].value_counts())

    df = copy.deepcopy(all_qtr_df)
    print(df.head())

    # engineer a combo variable for delta columns (ie unique row identifiers)
    col_order = list(df.columns)
    delta_id = 'delta_id'
    df = pjl.concat_column_values(df,
                                  column_names=col_order,
                                  delta_id=delta_id)
    final_df = copy.deepcopy(df)
    print(final_df.info())

    # create instance of database session for the database
    db_user = pvr['db_user']
    db_pwd = pvr['db_pwd']
    db_name = pvr['db_name']
    db_host = pvr['db_host']
    db_port = pvr['db_port']
    dbase_cred = pjl.dbase_conn_sqlalchemy(dbase_name=db_name,
                                           dbase_password=db_pwd,
                                           dbase_port=db_port)
    dbase_engine = dbase_cred['engine']
    dbase_conn = dbase_cred['connection']

    # get data from database table
    db_table_name = 'tab_a1'
    db_schema = 'staging'
    db_table = pjl.sqlalchem_select_query(table_name=db_table_name,
                                          schema_name=db_schema,
                                          dbase_engine=dbase_engine,
                                          dbase_conn=dbase_conn)

    intg_cols = ['initial_assessments', 'owed_prevention_or_relief_duty',
                 'prevention_duty_owed', 'relief_duty_owed']
    fltg_cols = ['households_in_area_000s']

    # replace Python NoneType with pandas NaN
    db_table = db_table.fillna(value=np.nan)

    db_table = pjl.recast_dtypes(db_table,
                                 int_cols=intg_cols,
                                 flt_cols=fltg_cols)
    print(db_table.info())

    # engineer a combo variable for delta columns (ie unique row identifiers)
    db_table = pjl.concat_column_values(db_table,
                                        column_names=col_order,
                                        delta_id=delta_id)
    print(db_table.info())

    # load into target database table
    print('\nLOADING FRESH RECORDS INTO TARGET')
    delta_load = pjl.run_delta_load_to_db(new_data=final_df,
                                          old_data=db_table,
                                          delta_col_name=delta_id,
                                          database_table_name=db_table_name,
                                          db_engine=dbase_engine,
                                          db_schema=db_schema)

    # delete old record from database
    print('\nCHECKING FOR REDUNDANT RECORD IN THE DATABASE')
    db_rec_del = pjl.run_delta_load_to_db(new_data=db_table,
                                          old_data=final_df,
                                          delta_col_name=delta_id,
                                          database_table_name=db_table_name,
                                          db_engine=dbase_engine,
                                          db_schema=db_schema,
                                          load_to_db=False)
    print(f'\nDelete {db_rec_del.shape[0]} old record form database')
    print(db_rec_del)

    # load to dataframe csv for export
    read_fdr = "TransformedData"
    wfile_name = f'tab_a1_cleaned.csv'
    wfile_path = sep.join([root_dir, read_fdr, wfile_name])
    # wfile_path = f'DBLoad\\property_registration_{date_tag}.csv'
    final_df.to_csv(wfile_path, index=False, encoding='utf8')

    # refresh materialized view
    refresh_schema = "core"
    refresh_view = "initial_asmt_summary"
    pjl.refresh_pgsql_mview(db_conn=dbase_conn,
                            refresh_schema=refresh_schema,
                            refresh_mview_name=refresh_view)

    # load transformed view onto dataframe for csv export
    db_schema = 'public'
    db_transformation_name = 'initial_asmt_summary'
    df = pjl.sqlalchem_select_query(table_name=db_transformation_name,
                                    schema_name=db_schema,
                                    dbase_engine=dbase_engine,
                                    dbase_conn=dbase_conn)
    print(df.info())

    final_df = copy.deepcopy(df)

    # close engine
    dbase_conn.close()
    dbase_engine.dispose()

    # export transformation to csv file
    wfile_name = f'initial_assessment_summary.csv'
    wfile_path = sep.join([root_dir, read_fdr, wfile_name])
    # wfile_path = f'DBLoad\\property_registration_{date_tag}.csv'
    final_df.to_csv(wfile_path, index=False, encoding='utf8')

    print(all_qtr_df['quarter_ending'].value_counts())
    print('finish')

# run_app()