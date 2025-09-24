import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
import copy
import re
import os


class ProjUtil:

    @staticmethod
    def refresh_pgsql_mview(db_conn: 'database connection',
                            refresh_schema: str,
                            refresh_mview_name: str):
        """refresh materialized view"""

        db_conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_schema}.{refresh_mview_name};"))
        print("Materialized view refreshed successfully.")

    @staticmethod
    def get_qtr_from_fname(filename: str, delim_char="_"):
        """get quarter from file name"""

        word_list = filename.split(sep=delim_char)
        qtr_val = None
        for i in range(len(word_list)):
            if '202' in word_list[i]:
                result = re.findall(r'\d+', word_list[i])
                qtr_val = int(result[0])
        return qtr_val

    @staticmethod
    def get_full_path(folder_path: str, specify_ftype: 'str or list' = None) -> dict:
        '''get dictionary of "filename"":"fullpath" for all the
        files contained within the given folder
        folder_path: full path of folder in which to search
        specify_ftype: only retrieve the specified file type'''

        filenames_list = os.listdir(folder_path)
        file_dict = dict()
        sep = "/"
        if specify_ftype is not None and isinstance(specify_ftype, str):
            for i in range(len(filenames_list)):
                if f'.{specify_ftype}' in filenames_list[i]:
                    fpath = sep.join([folder_path, filenames_list[i]])
                    file_dict[filenames_list[i]] = fpath
        elif specify_ftype is not None and isinstance(specify_ftype, list):
            for f in range(len(specify_ftype)):
                for i in range(len(filenames_list)):
                    if f'.{specify_ftype[f]}' in filenames_list[i]:
                        fpath = sep.join([folder_path, filenames_list[i]])
                        file_dict[filenames_list[i]] = fpath
        else:
            for i in range(len(filenames_list)):
                fpath = sep.join([folder_path, filenames_list[i]])
                file_dict[filenames_list[i]] = fpath
        return file_dict

    @staticmethod
    def recast_dtypes(df: 'pandas dataframe', show_progress=True, date_cols: 'list of date column names' = list(),
                      flt_cols: 'list of date column names' = list(),
                      int_cols: 'list of integer column names' = list(),
                      str_cols: 'list of alphanumeric columns' = list()):
        """enforce column data type in the given dataframe
        output: pandas dataframe"""

        if len(date_cols):
            for i in range(len(date_cols)):
                date_dtype = df[date_cols[i]].astype('datetime64[ns]')
                df[date_cols[i]] = date_dtype
                if show_progress:
                    print(f"{date_cols[i]}'s DATATYPE CHANGED!")
            if show_progress:
                print('date columns assigned')

        if len(flt_cols):
            for i in range(len(flt_cols)):
                flt_dtype = pd.to_numeric(df[flt_cols[i]], errors='coerce').astype('float64')
                df[flt_cols[i]] = flt_dtype
                if show_progress:
                    print(f"{flt_cols[i]}'s DATATYPE CHANGED!")
            if show_progress:
                print('decimal columns assigned')

        if len(int_cols):
            for i in range(len(int_cols)):
                int_dtype = pd.to_numeric(df[int_cols[i]], errors='coerce')  # Converts invalid entries to NaN
                int_dtype = int_dtype.astype('Int64')
                df[int_cols[i]] = int_dtype
                if show_progress:
                    print(f"{int_cols[i]}'s DATATYPE CHANGED!")
            if show_progress:
                print('integer columns assigned')

        if len(str_cols):
            for i in range(len(str_cols)):
                str_dtype = df[str_cols[i]].astype('str')
                df[str_cols[i]] = str_dtype
                if show_progress:
                    print(f"{str_cols[i]}'s DATATYPE CHANGED!")
            if show_progress:
                print('string columns assigned')

        print(df.info())
        if show_progress:
            print('\nCOLUMN DATA TYPE RECAST COMPLETE\n')

        return df

    @staticmethod
    def append_to_sample_df(sample_df: "pandas dataframe",
                            file_path_dict: dict,
                            assign_colname: str,
                            sheet_name: str = None,
                            prefix: str="prevention",
                            use_func: str="a1") -> 'pandas dataframe':
        """append data extracted from all remaining files in the source folder
        to data extracted from model file (aka sample file)
        returns dataframe containing the complete dataset from files in source folder"""

        df_list = [sample_df]
        # for excel files
        for f, fpath in file_path_dict.items():
            file_name, file_ext = f.split('.')
            # print(file_comp)
            print(f'\nReading {file_name}')
            qtr_val = ProjUtil.get_qtr_from_fname(file_name)
            if use_func not in ["a1", "ta1", "ta2"]:
                funct_call = f"ProjUtil.clean_tab_{use_func}(abs_file_path=fpath, sheet_name=sheet_name, assign_col=assign_colname, assign_val=qtr_val, prefix=prefix)"
            else:
                funct_call = f"ProjUtil.clean_tab_{use_func}(abs_file_path=fpath, sheet_name=sheet_name, assign_col=assign_colname, assign_val=qtr_val)"
            df = eval(funct_call)
            # print(df.info())
            df_list.append(df)
            # print(f'Appending {f}')
            # append each qtr df into one df
            # cache_df = sample_df._append(other=df)

        cache_df = pd.concat(df_list)

        return cache_df

    @staticmethod
    def switch_col_values(ser: 'pandas series', mapper: 'dictionary object'):
        """switch an old value with a new one as mapped in the given dictionary
        output: pandas series"""

        new_ser = ser.apply(lambda x: mapper[x])
        return new_ser

    @staticmethod
    def sqlalchem_select_query(table_name: str, schema_name: str, dbase_engine: "sqlalchemy engine instance",
                               dbase_conn: "database connection instance"):
        """retrieve data from database table
         output: dataframe object"""

        # create metadata object
        db_metadata = db.MetaData()

        # db_table = db.Table('support_plan_log', db_metadata, schema='supported housing', autoload_with=db_engine)
        db_table = db.Table(table_name, db_metadata, schema=schema_name, autoload_with=dbase_engine)
        print('TABLE OBJECT CREATED')

        # collect table header
        header = db_table.columns.keys()

        # create select query object for table
        query = db.select(db_table)
        # execute query
        output = dbase_conn.execute(query)
        # store query result
        data = output.fetchall()
        # parse query result as dataframe
        df = pd.DataFrame(data=data, columns=header)
        print('\nQUERY OUTPUT IS AVAILABLE')

        return df

    @staticmethod
    def concat_column_values(df: 'pd.DataFrame', column_names: 'list of columns' = list(), separator='-;-',
                             delta_id='delta_id'):
        """concatenate multiple columns into a single column
        df: dataframe containing parent dataset
        column_names: list of columns whose values are to be selected for concatenation
        separator: separating character
        delta_id: name of new column containing concatenated values

        Output: dataframe including concatenated column"""

        col_size = len(column_names)
        n_records = df.shape[0]
        if n_records > 0:
            if col_size < 1:
                # print(df.astype(str).agg(func=f'{separator}'.join, axis='columns'))
                df[delta_id] = df.astype(str).agg(func=f'{separator}'.join, axis='columns')
            else:
                # print(df[column_names].astype(str).agg(func=f'{separator}'.join, axis='columns'))
                df[delta_id] = df[column_names].astype(str).agg(func=f'{separator}'.join, axis='columns')
        else:
            df[delta_id] = ''
        print(df.info())
        return df

    @staticmethod
    def dbase_conn_sqlalchemy(dbase_name: str, dbase_password: str, dbase_driver: str = 'postgresql',
                              dbase_username: str = 'postgres', dbase_host: str = 'localhost', dbase_port: int = 5432):
        """connect to a database session using sqlalchemy
        output: dict(engine obj, connection obj)"""

        # create instance of database session for the given database
        db_engine = db.create_engine(
            url=f'{dbase_driver}://{dbase_username}:{dbase_password}@{dbase_host}:{dbase_port}/{dbase_name}')
        print('ENGINE CREATED')

        # connect to database instance
        db_conn = db_engine.connect()
        print('CONNECTION CREATED')

        return {'engine': db_engine, 'connection': db_conn}

    @staticmethod
    def run_delta_load_to_db(new_data: 'pd.DataFrame', old_data: "pd.DataFrame", delta_col_name: str,
                             database_table_name: str, db_engine: 'sqlalchemy create engine obj', db_schema: str,
                             load_to_db=True):
        """run delta load logic into a connected database
        :parameter
        new_data: dataframe containing fresh data
        old_data: dataframe containing data in the existing dbms table
        delta_col_name:identify the differences in values of the delta columns in both datasets
        load_to_db - if true, load data in the delta_load dataframe into the target dbms table

        Logic:
        - get delta column data of both new and existing records
        - only select the records not present in the existing database table
        """

        # get fresh data's unique values in the delta column
        new_set = set(new_data[delta_col_name])
        print(f'\n{len(new_set)} new records found')
        # print(new_set)

        # get existing data's unique values in the delta column
        old_set = set(old_data[delta_col_name])
        print(f'\n{len(old_set)} existing db records found')
        # print(old_set)

        # get values from new data not present in the existing db data
        delta_id = new_set.difference(old_set)
        # select only fresh data
        # use_colnames = list(old_data.columns)
        delta_load = new_data.loc[new_data[delta_col_name].isin(delta_id)]

        # drop the delta_id before loading to the database
        delta_load = delta_load.drop(columns=delta_col_name)
        print('\nDelta Load:')
        print(delta_load.info())

        # load only fresh data into target database table
        if load_to_db:
            delta_load.to_sql(name=database_table_name, con=db_engine, schema=db_schema, if_exists='append',
                              index=False)
            print("Delta load done!")

        return delta_load

    @staticmethod
    def add_constant_col(df: 'pandas dataframe', new_colname: str = 'row_count', const_val: 'any type' = 1):
        """engineer an additional column with constant value to given dataframe object
        output: df"""

        df.loc[:, new_colname] = const_val
        # print(df.info())
        return df

    @staticmethod
    def pull_file_xl(file_path: str, sheet_name: str, skiprows: int = 0):
        """read excel file in the given file location
        output: dataframe"""

        xl_file = pd.read_excel(io=file_path, sheet_name=sheet_name, skiprows=skiprows)  # , engine='openpyxl')

        return xl_file

    @staticmethod
    def drop_empty_axis(df: 'pandas dataframe', axis="columns", valid_row_indicator: 'integer' = 1, show_progress=True):
        """remove completely empty columns
        thresh=> number of valid rows to indicate the columns to keep
        output: pandas dataframe"""

        df = df.dropna(axis=axis, thresh=valid_row_indicator)

        if (show_progress == True) and (axis == 'columns'):
            print('\nEMPTY COLUMNS REMOVED\n')
        elif (show_progress == True) and (axis == 'rows'):
            df = df.reset_index(drop=True)
            print('\nEMPTY ROWS REMOVED\n')

        return df

    @staticmethod
    def clean_tab_a1(abs_file_path: str, sheet_name='A1',
                     skiprows=1, assign_col='quarter_ending',
                     assign_val=202406):

        print(f"\nNOW CLEANING:\n {sheet_name} {assign_col.upper()+': '+str(assign_val)}")

        # extract data from xl file into dataframe
        xl_df = ProjUtil.pull_file_xl(file_path=abs_file_path, sheet_name=sheet_name, skiprows=skiprows)
        # print(xl_df.info())
        df = copy.deepcopy(xl_df)

        # filter out blank records - completely empty records
        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=1)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        # filter out useless columns - containing mostly blank roecords
        dropped_null_cols = ProjUtil.drop_empty_axis(df, valid_row_indicator=20)
        # print(dropped_null_cols.info())
        df = copy.deepcopy(dropped_null_cols)
        # print(df.head(15))

        # map columns to appropriate column names
        # col_list = list(df.columns)
        # print(col_list)
        sel_colnames = ['system_id', 'local_authority', 'initial_assessments', 'owed_prevention_or_relief_duty',
                        'prevention_duty_owed', 'relief_duty_owed', 'households_in_area_000s']
        col_mapper = {"Unnamed: 0": "system_id",
                      "Unnamed: 1": "local_authority",
                      "Total number of households assessed1,2": "initial_assessments",
                      "Total households assessed as owed a duty": "owed_prevention_or_relief_duty",
                      "Threatened with homelessness - Prevention duty owed": "prevention_duty_owed",
                      "Homeless - Relief duty owed": "relief_duty_owed",
                      "Unnamed: 15": "households_in_area_000s"}
        # print(list(col_mapper.values()))
        assigned_col_mapper = df.rename(columns=col_mapper)
        # print(assigned_col_mapper.head(10))
        df = copy.deepcopy(assigned_col_mapper)

        # remove duplicate records
        no_duplicates = df.drop_duplicates()
        # print(no_duplicates.info())
        df = copy.deepcopy(no_duplicates)

        # col_list = list(df.columns)
        # print(col_list)

        # retain only relevant columns
        select_relevant_columns = df[sel_colnames]
        # print(select_relevant_columns.info())
        df = copy.deepcopy(select_relevant_columns)
        start_index = df.loc[df['local_authority'] == 'ENGLAND'].index
        skip_header_rows = df.iloc[start_index[0]:].reset_index(drop=True)
        # print(skip_header_rows.info())
        df = copy.deepcopy(skip_header_rows)

        # filter out all records where system_id is blank
        cond = df["system_id"].notna()
        dropped_null_sys_id = df.loc[cond].reset_index(drop=True)
        # print(dropped_null_sys_id.info())
        df = copy.deepcopy(dropped_null_sys_id)

        # filter out useless rows - containing mostly blank records
        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=3)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        # engineer quarter indicator feature
        added_quarter_indicator = ProjUtil.add_constant_col(df=df, new_colname=assign_col, const_val=assign_val)
        print(added_quarter_indicator.info())

        df = copy.deepcopy(added_quarter_indicator)
        # print(df.head(10))

        # engineer neighbouring local authority indicator
        neighbouring_la_list = ['Southwark', 'Islington',
                                'Haringey', 'Lambeth',
                                'Tower Hamlets', 'Camden',
                                'Waltham Forest', 'Hammersmith & Fulham',
                                'Newham']
        added_neighbouring_la_indicator = df['local_authority'].apply(lambda x: 1 if x in neighbouring_la_list else 0)
        df['is_neighbouring_la'] = added_neighbouring_la_indicator
        # print(added_neighbouring_la_indicator)

        # replace system placeholder for missing values ".." with a more intuitive one - ""
        col_list = ['initial_assessments', 'owed_prevention_or_relief_duty',
                     'prevention_duty_owed', 'relief_duty_owed',
                    'households_in_area_000s']
        for i in range(len(col_list)):
            replace_null_placeholder = df[col_list[i]].astype("str").str.replace("..", "")
            df[col_list[i]] = copy.deepcopy(replace_null_placeholder)
            print(col_list[i])
        print(df.loc[df['local_authority'] == "Camden"])

        # recast column datatypes
        intg_cols = ['initial_assessments', 'owed_prevention_or_relief_duty',
                     'prevention_duty_owed', 'relief_duty_owed']
        fltg_cols = ['households_in_area_000s']
        print(df[intg_cols].head(5))
        changed_data_types = ProjUtil.recast_dtypes(df, int_cols=intg_cols, flt_cols=fltg_cols)
        print(changed_data_types.info())
        df = copy.deepcopy(changed_data_types)

        print("\nCLEANING COMPLETE!")
        return df

    @staticmethod
    def clean_tab_a2(abs_file_path: str, sheet_name='A1', skiprows=1, assign_col='quarter_ending', assign_val=202406, prefix='prevention'):

        print(f"\nNOW CLEANING:\n {sheet_name} {assign_col.upper() + ': ' + str(assign_val)}")
        xl_df = ProjUtil.pull_file_xl(file_path=abs_file_path, sheet_name=sheet_name, skiprows=skiprows)
        # print(xl_df.info())
        df = copy.deepcopy(xl_df)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=1)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        dropped_null_cols = ProjUtil.drop_empty_axis(df, valid_row_indicator=20)
        # print(dropped_null_cols.info())
        df = copy.deepcopy(dropped_null_cols)
        # print(df.head(15))

        # col_list = list(df.columns)
        # print(col_list)
        sel_colnames = ['system_id', 'local_authority', f'{prefix}_duty_owed', 'family_or_friend_terminations',
                        'ast_private_rented_terminations',
                        'domestic_abuse_terminations', 'non_violent_relationship_breakdown_terminations',
                        'social_rented_tenancy_terminations',
                        'supported_housing_terminations', 'non_ast_private_rented_terminations',
                        'other_violence_or_harassment_terminations',
                        'institution_departures', 'home_office_asylum_support_terminations',
                        'new_home_for_illness_or_disability',
                        'loss_of_placement_or_sponsorship', 'for_other_or_unknown_reasons']
        col_mapper = {"Unnamed: 0": "system_id",
                      "Unnamed: 1": "local_authority",
                      "Unnamed: 4": f"{prefix}_duty_owed",
                      "Family or friends no longer willing or able to accommodate": "family_or_friend_terminations",
                      "End of private rented tenancy - assured shorthold": "ast_private_rented_terminations",
                      "Domestic abuse": "domestic_abuse_terminations",
                      "Non-violent relationship breakdown with partner": "non_violent_relationship_breakdown_terminations",
                      "End of social rented tenancy": "social_rented_tenancy_terminations",
                      "Eviction from supported housing": "supported_housing_terminations",
                      "End of private rented tenancy - not assured shorthold": "non_ast_private_rented_terminations",
                      "Other violence or harrassment": "other_violence_or_harassment_terminations",
                      "Left institution with no accommodation available": "institution_departures",
                      "Required to leave accommodation provided by Home Office as asylum support": "home_office_asylum_support_terminations",
                      "Home no longer suitable - disability / ill health": "new_home_for_illness_or_disability",
                      "Unnamed: 56": "loss_of_placement_or_sponsorship",
                      "Other reasons / not known6": "for_other_or_unknown_reasons"}
        # print(list(col_mapper.values()))
        assigned_col_mapper = df.rename(columns=col_mapper)
        # print(assigned_col_mapper.head(10))
        df = copy.deepcopy(assigned_col_mapper)

        no_duplicates = df.drop_duplicates()
        # print(no_duplicates.info())
        df = copy.deepcopy(no_duplicates)

        # col_list = list(df.columns)
        # print(col_list)

        # sel_cols = [col_list[i] for i in range(len(col_list)) if "Unnamed:" not in col_list[i]]
        # print(sel_cols)
        select_relevant_columns = df[sel_colnames]
        # print(select_relevant_columns.info())
        df = copy.deepcopy(select_relevant_columns)
        start_index = df.loc[df['local_authority'] == 'ENGLAND'].index
        skip_header_rows = df.iloc[start_index[0]:].reset_index(drop=True)
        # print(skip_header_rows.info())
        df = copy.deepcopy(skip_header_rows)

        cond = df["system_id"].notna()
        dropped_null_sys_id = df.loc[cond].reset_index(drop=True)
        # print(dropped_null_sys_id.info())
        df = copy.deepcopy(dropped_null_sys_id)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=3)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        added_quarter_indicator = ProjUtil.add_constant_col(df=df, new_colname=assign_col, const_val=assign_val)
        # print(added_quarter_indicator.info())

        df = copy.deepcopy(added_quarter_indicator)
        # print(df.head(10))

        # engineer neighbouring local authority indicator
        neighbouring_la_list = ['Southwark', 'Islington',
                                'Haringey', 'Lambeth',
                                'Tower Hamlets', 'Camden',
                                'Waltham Forest', 'Hammersmith & Fulham',
                                'Newham']
        added_neighbouring_la_indicator = df['local_authority'].apply(lambda x: 1 if x in neighbouring_la_list else 0)
        df['is_neighbouring_la'] = added_neighbouring_la_indicator
        # print(added_neighbouring_la_indicator)

        # replace system placeholder for missing values ".." with a more intuitive one - ""
        col_list = [f'{prefix}_duty_owed', 'family_or_friend_terminations',
                    'ast_private_rented_terminations',
                    'domestic_abuse_terminations', 'non_violent_relationship_breakdown_terminations',
                    'social_rented_tenancy_terminations',
                    'supported_housing_terminations', 'non_ast_private_rented_terminations',
                    'other_violence_or_harassment_terminations',
                    'institution_departures', 'home_office_asylum_support_terminations',
                    'new_home_for_illness_or_disability',
                    'loss_of_placement_or_sponsorship', 'for_other_or_unknown_reasons']
        for i in range(len(col_list)):
            replace_null_placeholder = df[col_list[i]].astype("str").str.replace("..", "")
            df[col_list[i]] = copy.deepcopy(replace_null_placeholder)
            print(col_list[i])
        print(df.loc[df['local_authority'] == "Camden"])

        # recast column datatypes
        intg_cols = [f'{prefix}_duty_owed', 'family_or_friend_terminations',
                    'ast_private_rented_terminations',
                    'domestic_abuse_terminations', 'non_violent_relationship_breakdown_terminations',
                    'social_rented_tenancy_terminations',
                    'supported_housing_terminations', 'non_ast_private_rented_terminations',
                    'other_violence_or_harassment_terminations',
                    'institution_departures', 'home_office_asylum_support_terminations',
                    'new_home_for_illness_or_disability',
                    'loss_of_placement_or_sponsorship', 'for_other_or_unknown_reasons']
        # fltg_cols = ['total_households_in_area_000s']
        print(df[intg_cols].head(5))
        changed_data_types = ProjUtil.recast_dtypes(df, int_cols=intg_cols)#, flt_cols=fltg_cols)
        print(changed_data_types.info())
        df = copy.deepcopy(changed_data_types)

        print("\nCLEANING COMPLETE!")
        return df

    @staticmethod
    def clean_tab_p1(abs_file_path: str, sheet_name='A1', skiprows=1, assign_col='quarter_ending', assign_val=202406,
                     prefix="prevention"):

        print(f"\nNOW CLEANING:\n {sheet_name} {assign_col.upper() + ': ' + str(assign_val)}")
        xl_df = ProjUtil.pull_file_xl(file_path=abs_file_path, sheet_name=sheet_name, skiprows=skiprows)
        # print(xl_df.info())
        df = copy.deepcopy(xl_df)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=1)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        dropped_null_cols = ProjUtil.drop_empty_axis(df, valid_row_indicator=20)
        # print(dropped_null_cols.info())
        df = copy.deepcopy(dropped_null_cols)
        # print(df.head(15))
        # col_list = list(df.columns)
        # print(col_list)
        sel_colnames = ['system_id', 'local_authority', f'{prefix}_duty_ended',
                        'secured_accommodation', 'homelessness',
                        'contact_lost', 'no_further_action_after_56days',
                        'applicant_withdrew_or_deceased', 'no_longer_eligible',
                        'rejected_offered_accommodation', 'uncooperative', 'not_known']
        col_mapper = {"Unnamed: 0": "system_id",
                      "Unnamed: 1": "local_authority",
                      f"Total number of households where {prefix} duty ended1,2": f"{prefix}_duty_ended",
                      "Secured accommodation for 6+ months": "secured_accommodation",
                      "Homeless (including intentionally homeless)": "homelessness",
                      "Contact lost": "contact_lost",
                      "56 days elapsed and no further action": "no_further_action_after_56days",
                      "Withdrew application / applicant deceased": "applicant_withdrew_or_deceased",
                      "No longer eligible": "no_longer_eligible",
                      "Refused suitable accommodation offer": "rejected_offered_accommodation",
                      "Refused to cooperate": "uncooperative",
                      "Not known6": "not_known"}
        print(list(col_mapper.values()))

        assigned_col_mapper = df.rename(columns=col_mapper)
        # print(assigned_col_mapper.head(10))
        df = copy.deepcopy(assigned_col_mapper)

        no_duplicates = df.drop_duplicates()
        # print(no_duplicates.info())
        df = copy.deepcopy(no_duplicates)

        # col_list = list(df.columns)
        # print(col_list)

        # sel_cols = [col_list[i] for i in range(len(col_list)) if "Unnamed:" not in col_list[i]]
        # print(sel_cols)
        select_relevant_columns = df[sel_colnames]
        # print(select_relevant_columns.info())
        df = copy.deepcopy(select_relevant_columns)
        start_index = df.loc[df['local_authority'] == 'ENGLAND'].index
        skip_header_rows = df.iloc[start_index[0]:].reset_index(drop=True)
        # print(skip_header_rows.info())
        df = copy.deepcopy(skip_header_rows)

        cond = df["system_id"].notna()
        dropped_null_sys_id = df.loc[cond].reset_index(drop=True)
        # print(dropped_null_sys_id.info())
        df = copy.deepcopy(dropped_null_sys_id)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=3)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        added_quarter_indicator = ProjUtil.add_constant_col(df=df, new_colname=assign_col, const_val=assign_val)
        # print(added_quarter_indicator.info())

        df = copy.deepcopy(added_quarter_indicator)
        # print(df.head(10))

        # engineer neighbouring local authority indicator
        neighbouring_la_list = ['Southwark', 'Islington',
                                'Haringey', 'Lambeth',
                                'Tower Hamlets', 'Camden',
                                'Waltham Forest', 'Hammersmith & Fulham',
                                'Newham']
        added_neighbouring_la_indicator = df['local_authority'].apply(lambda x: 1 if x in neighbouring_la_list else 0)
        df['is_neighbouring_la'] = added_neighbouring_la_indicator
        # print(added_neighbouring_la_indicator)

        # replace system placeholder for missing values ".." with a more intuitive one - ""
        col_list = [f'{prefix}_duty_ended',
                    'secured_accommodation', 'homelessness',
                    'contact_lost', 'no_further_action_after_56days',
                    'applicant_withdrew_or_deceased', 'no_longer_eligible',
                    'rejected_offered_accommodation', 'uncooperative', 'not_known']
        for i in range(len(col_list)):
            replace_null_placeholder = df[col_list[i]].astype("str").str.replace("..", "")
            df[col_list[i]] = copy.deepcopy(replace_null_placeholder)
            print(col_list[i])
        print(df.loc[df['local_authority'] == "Camden"])

        # recast column datatypes
        intg_cols = [f'{prefix}_duty_ended',
                    'secured_accommodation', 'homelessness',
                    'contact_lost', 'no_further_action_after_56days',
                    'applicant_withdrew_or_deceased', 'no_longer_eligible',
                    'rejected_offered_accommodation', 'uncooperative', 'not_known']
        # fltg_cols = ['total_households_in_area_000s']
        print(df[intg_cols].head(5))
        changed_data_types = ProjUtil.recast_dtypes(df, int_cols=intg_cols)  # , flt_cols=fltg_cols)
        print(changed_data_types.info())
        df = copy.deepcopy(changed_data_types)

        print("\nCLEANING COMPLETE!")
        return df

    @staticmethod
    def clean_tab_r1(abs_file_path: str, sheet_name='A1', skiprows=1, assign_col='quarter_ending', assign_val=202406,
                     prefix="prevention"):

        print(f"\nNOW CLEANING:\n {sheet_name} {assign_col.upper() + ': ' + str(assign_val)}")
        xl_df = ProjUtil.pull_file_xl(file_path=abs_file_path, sheet_name=sheet_name, skiprows=skiprows)
        # print(xl_df.info())
        df = copy.deepcopy(xl_df)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=1)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        dropped_null_cols = ProjUtil.drop_empty_axis(df, valid_row_indicator=20)
        # print(dropped_null_cols.info())
        df = copy.deepcopy(dropped_null_cols)
        # print(df.head(15))
        # col_list = list(df.columns)
        # print(col_list)
        sel_colnames = ['system_id', 'local_authority', f'{prefix}_duty_ended',
                        'secured_accommodation', 'after_56days_deadline', 'contact_lost',
                        'applicant_withdrew_or_deceased', 'rejected_final_accommodation_offered',
                        'intentionally_homeless_from_accommodation_provided', 'accepted_by_another_la',
                        'no_longer_eligible', 'uncooperative_and_served_notice', 'not_known']
        col_mapper = {"Unnamed: 0": "system_id",
                      "Unnamed: 1": "local_authority",
                      f"Total number of households where {prefix} duty ended1,2": f"{prefix}_duty_ended",
                      "Secured accommodation for 6+ months": "secured_accommodation",
                      "56 days elapsed": "after_56days_deadline",
                      "Contact lost": "contact_lost",
                      "Withdrew application / applicant deceased": "applicant_withdrew_or_deceased",
                      "Refused final accommodation": "rejected_final_accommodation_offered",
                      "Intentionally homeless from accommodation provided": "intentionally_homeless_from_accommodation_provided",
                      "Local connection referral accepted by other LA": "accepted_by_another_la",
                      "No longer eligible": "no_longer_eligible",
                      "Notice served due to refusal to cooperate": "uncooperative_and_served_notice",
                      "Not known": "not_known"}
        # print(list(col_mapper.values()))
        assigned_col_mapper = df.rename(columns=col_mapper)
        # print(assigned_col_mapper.head(10))
        df = copy.deepcopy(assigned_col_mapper)

        no_duplicates = df.drop_duplicates()
        # print(no_duplicates.info())
        df = copy.deepcopy(no_duplicates)

        # col_list = list(df.columns)
        # print(col_list)

        # sel_cols = [col_list[i] for i in range(len(col_list)) if "Unnamed:" not in col_list[i]]
        # print(sel_cols)
        select_relevant_columns = df[sel_colnames]
        # print(select_relevant_columns.info())
        df = copy.deepcopy(select_relevant_columns)
        start_index = df.loc[df['local_authority'] == 'ENGLAND'].index
        skip_header_rows = df.iloc[start_index[0]:].reset_index(drop=True)
        # print(skip_header_rows.info())
        df = copy.deepcopy(skip_header_rows)

        cond = df["system_id"].notna()
        dropped_null_sys_id = df.loc[cond].reset_index(drop=True)
        # print(dropped_null_sys_id.info())
        df = copy.deepcopy(dropped_null_sys_id)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=3)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        added_quarter_indicator = ProjUtil.add_constant_col(df=df, new_colname=assign_col, const_val=assign_val)
        # print(added_quarter_indicator.info())

        # engineer neighbouring local authority indicator
        neighbouring_la_list = ['Southwark', 'Islington',
                                'Haringey', 'Lambeth',
                                'Tower Hamlets', 'Camden',
                                'Waltham Forest', 'Hammersmith & Fulham',
                                'Newham']
        added_neighbouring_la_indicator = df['local_authority'].apply(lambda x: 1 if x in neighbouring_la_list else 0)
        df['is_neighbouring_la'] = added_neighbouring_la_indicator
        # print(added_neighbouring_la_indicator)

        df = copy.deepcopy(added_quarter_indicator)
        # print(df.head(10))

        # replace system placeholder for missing values ".." with a more intuitive one - ""
        col_list = [f'{prefix}_duty_ended',
                    'secured_accommodation', 'after_56days_deadline', 'contact_lost',
                    'applicant_withdrew_or_deceased', 'rejected_final_accommodation_offered',
                    'intentionally_homeless_from_accommodation_provided', 'accepted_by_another_la',
                    'no_longer_eligible', 'uncooperative_and_served_notice', 'not_known']
        for i in range(len(col_list)):
            replace_null_placeholder = df[col_list[i]].astype("str").str.replace("..", "")
            df[col_list[i]] = copy.deepcopy(replace_null_placeholder)
            print(col_list[i])
        print(df.loc[df['local_authority'] == "Camden"])

        # recast column datatypes
        intg_cols = [f'{prefix}_duty_ended',
                    'secured_accommodation', 'after_56days_deadline', 'contact_lost',
                    'applicant_withdrew_or_deceased', 'rejected_final_accommodation_offered',
                    'intentionally_homeless_from_accommodation_provided', 'accepted_by_another_la',
                    'no_longer_eligible', 'uncooperative_and_served_notice', 'not_known']
        # fltg_cols = ['total_households_in_area_000s']
        print(df[intg_cols].head(5))
        changed_data_types = ProjUtil.recast_dtypes(df, int_cols=intg_cols)  # , flt_cols=fltg_cols)
        print(changed_data_types.info())
        df = copy.deepcopy(changed_data_types)

        print("\nCLEANING COMPLETE!")
        return df

    @staticmethod
    def clean_tab_ta1(abs_file_path: str, sheet_name='A1', skiprows=1, assign_col='quarter_ending', assign_val=202406):

        print(f"\nNOW CLEANING:\n {sheet_name} {assign_col.upper() + ': ' + str(assign_val)}")
        xl_df = ProjUtil.pull_file_xl(file_path=abs_file_path, sheet_name=sheet_name, skiprows=skiprows)
        # print(xl_df.info())
        df = copy.deepcopy(xl_df)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=1)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        dropped_null_cols = ProjUtil.drop_empty_axis(df, valid_row_indicator=20)
        # print(dropped_null_cols.info())
        df = copy.deepcopy(dropped_null_cols)
        # print(df.head(15))
        # col_list = list(df.columns)
        # print(col_list)
        sel_colnames = ['system_id', 'local_authority', 'households_in_ta',
                        'ta_households_with_children', 'children_headcount_in_ta',
                        'bnb_ta_households', 'bnb_ta_with_children',
                        'bnb_ta_with_children_exceeding_6wks',
                        'bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal',
                        'bnb_ta_with_16yo_17yo_main_applicant', 'nightly_paid_ta_households',
                        'nightly_paid_ta_with_children', 'hostel_ta_households',
                        'hostel_ta_with_children', 'private_sector_ta',
                        'private_sector_ta_with_children', 'la_ha_owned_managed_ta_households',
                        'la_ha_owned_managed_ta_with_children', 'any_other_type_ta',
                        'any_other_type_ta_with_children', 'in_another_la_ta',
                        'no_secured_accommodation_ta', 'no_secured_accommodation_ta_with_children']
        col_mapper = {"Unnamed: 0": "system_id",
                      "Unnamed: 1": "local_authority",
                      f"Households in temporary accommodation at end of quarter1": "households_in_ta",
                      f"Households in temporary accommodation at end of quarter1 children": "ta_households_with_children",
                      "Unnamed: 8": "children_headcount_in_ta",
                      "Bed and breakfast hotels (including shared annexes)": "bnb_ta_households",
                      "Bed and breakfast hotels (including shared annexes) children": "bnb_ta_with_children",
                      "Unnamed: 12": "bnb_ta_with_children_exceeding_6wks",
                      "Unnamed: 13": "bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal",
                      "Unnamed: 14": "bnb_ta_with_16yo_17yo_main_applicant",
                      "Nightly paid, privately managed accommodation, self-contained": "nightly_paid_ta_households",
                      "Nightly paid, privately managed accommodation, self-contained children": "nightly_paid_ta_with_children",
                      "Hostels (including reception centres, emergency units and refuges)": "hostel_ta_households",
                      "Hostels (including reception centres, emergency units and refuges) children": "hostel_ta_with_children",
                      "Private sector accommodation leased by authority or by a registered provider": "private_sector_ta",
                      "Private sector accommodation leased by authority or by a registered provider children": "private_sector_ta_with_children",
                      "Local authority or Housing association (LA/HA) stock": "la_ha_owned_managed_ta_households",
                      "Local authority or Housing association (LA/HA) stock children": "la_ha_owned_managed_ta_with_children",
                      "Any other type of temporary accommodation (including private landlord and not known)2": "any_other_type_ta",
                      "Any other type of temporary accommodation (including private landlord and not known)2 children": "any_other_type_ta_with_children",
                      "In TA in another local authority district": "in_another_la_ta",
                      "Duty owed, no accommodation secured3": "no_secured_accommodation_ta",
                      "Duty owed, no accommodation secured3 children": "no_secured_accommodation_ta_with_children"}
        # print(list(col_mapper.values()))
        assigned_col_mapper = df.rename(columns=col_mapper)
        # print(assigned_col_mapper.head(10))
        df = copy.deepcopy(assigned_col_mapper)

        no_duplicates = df.drop_duplicates()
        # print(no_duplicates.info())
        df = copy.deepcopy(no_duplicates)

        # col_list = list(df.columns)
        # print(col_list)

        # sel_cols = [col_list[i] for i in range(len(col_list)) if "Unnamed:" not in col_list[i]]
        # print(sel_cols)
        select_relevant_columns = df[sel_colnames]
        # print(select_relevant_columns.info())
        df = copy.deepcopy(select_relevant_columns)
        start_index = df.loc[df['local_authority'] == 'ENGLAND'].index
        skip_header_rows = df.iloc[start_index[0]:].reset_index(drop=True)
        # print(skip_header_rows.info())
        df = copy.deepcopy(skip_header_rows)

        cond = df["system_id"].notna()
        dropped_null_sys_id = df.loc[cond].reset_index(drop=True)
        # print(dropped_null_sys_id.info())
        df = copy.deepcopy(dropped_null_sys_id)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=3)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        added_quarter_indicator = ProjUtil.add_constant_col(df=df, new_colname=assign_col, const_val=assign_val)
        # print(added_quarter_indicator.info())

        # engineer neighbouring local authority indicator
        neighbouring_la_list = ['Southwark', 'Islington',
                                'Haringey', 'Lambeth',
                                'Tower Hamlets', 'Camden',
                                'Waltham Forest', 'Hammersmith & Fulham',
                                'Newham']
        added_neighbouring_la_indicator = df['local_authority'].apply(lambda x: 1 if x in neighbouring_la_list else 0)
        df['is_neighbouring_la'] = added_neighbouring_la_indicator
        # print(added_neighbouring_la_indicator)

        df = copy.deepcopy(added_quarter_indicator)
        # print(df.head(10))

        # replace system placeholder for missing values ".." with a more intuitive one - ""
        col_list = ['households_in_ta',
                    'ta_households_with_children', 'children_headcount_in_ta',
                    'bnb_ta_households', 'bnb_ta_with_children',
                    'bnb_ta_with_children_exceeding_6wks',
                    'bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal',
                    'bnb_ta_with_16yo_17yo_main_applicant', 'nightly_paid_ta_households',
                    'nightly_paid_ta_with_children', 'hostel_ta_households',
                    'hostel_ta_with_children', 'private_sector_ta',
                    'private_sector_ta_with_children', 'la_ha_owned_managed_ta_households',
                    'la_ha_owned_managed_ta_with_children', 'any_other_type_ta',
                    'any_other_type_ta_with_children', 'in_another_la_ta',
                    'no_secured_accommodation_ta', 'no_secured_accommodation_ta_with_children']
        for i in range(len(col_list)):
            replace_null_placeholder = df[col_list[i]].astype("str").str.replace("..", "")
            df[col_list[i]] = copy.deepcopy(replace_null_placeholder)
            print(col_list[i])
        print(df.loc[df['local_authority'] == "Camden"])

        # recast column datatypes
        intg_cols = ['households_in_ta',
                    'ta_households_with_children', 'children_headcount_in_ta',
                    'bnb_ta_households', 'bnb_ta_with_children',
                    'bnb_ta_with_children_exceeding_6wks',
                    'bnb_ta_with_children_exceeding_6wks_awaiting_review_or_appeal',
                    'bnb_ta_with_16yo_17yo_main_applicant', 'nightly_paid_ta_households',
                    'nightly_paid_ta_with_children', 'hostel_ta_households',
                    'hostel_ta_with_children', 'private_sector_ta',
                    'private_sector_ta_with_children', 'la_ha_owned_managed_ta_households',
                    'la_ha_owned_managed_ta_with_children', 'any_other_type_ta',
                    'any_other_type_ta_with_children', 'in_another_la_ta',
                    'no_secured_accommodation_ta', 'no_secured_accommodation_ta_with_children']
        # fltg_cols = ['total_households_in_area_000s']
        print(df[intg_cols].head(5))
        changed_data_types = ProjUtil.recast_dtypes(df, int_cols=intg_cols)  # , flt_cols=fltg_cols)
        print(changed_data_types.info())
        df = copy.deepcopy(changed_data_types)

        print("\nCLEANING COMPLETE!")
        return df

    @staticmethod
    def clean_tab_ta2(abs_file_path: str, sheet_name='A1', skiprows=1, assign_col='quarter_ending', assign_val=202406):

        print(f"\nNOW CLEANING:\n {sheet_name} {assign_col.upper() + ': ' + str(assign_val)}")
        xl_df = ProjUtil.pull_file_xl(file_path=abs_file_path, sheet_name=sheet_name, skiprows=skiprows)
        # print(xl_df.info())
        df = copy.deepcopy(xl_df)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=1)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        dropped_null_cols = ProjUtil.drop_empty_axis(df, valid_row_indicator=20)
        print(dropped_null_cols.info())
        df = copy.deepcopy(dropped_null_cols)
        print(df.head(15))
        # col_list = list(df.columns)
        # print(col_list)
        sel_colnames = ['system_id',
                        'local_authority',
                        'households_in_ta',
                        'couple_with_children_ta',
                        'single_father_with_children_ta',
                        'single_mother_with_children_ta',
                        'single_parent_of_other_unknown_gender_with_children_ta',
                        'single_man_ta',
                        'single_woman_ta',
                        'single_other_gender_ta',
                        'all_other_household_types_ta']
        col_mapper = {"Unnamed: 0": "system_id",
                      "Unnamed: 1": "local_authority",
                      "Unnamed: 4":"households_in_ta",
                      "Couple with dependent children": "couple_with_children_ta",
                      "Single parent with dependent children  -  Male": "single_father_with_children_ta",
                      "Single parent with dependent children  -  Female": "single_mother_with_children_ta",
                      "Single parent with dependent children  -  Other/gender not known": "single_parent_of_other_unknown_gender_with_children_ta",
                      "Single adult  -  Male": "single_man_ta",
                      "Single adult  -  Female": "single_woman_ta",
                      "Single adult  -  Other/gender not known": "single_other_gender_ta",
                      "All other household types4": "all_other_household_types_ta"}
        # print(list(col_mapper.values()))
        assigned_col_mapper = df.rename(columns=col_mapper)
        # print(assigned_col_mapper.head(10))
        df = copy.deepcopy(assigned_col_mapper)

        no_duplicates = df.drop_duplicates()
        # print(no_duplicates.info())
        df = copy.deepcopy(no_duplicates)

        # col_list = list(df.columns)
        # print(col_list)

        # sel_cols = [col_list[i] for i in range(len(col_list)) if "Unnamed:" not in col_list[i]]
        # print(sel_cols)
        select_relevant_columns = df[sel_colnames]
        # print(select_relevant_columns.info())
        df = copy.deepcopy(select_relevant_columns)
        start_index = df.loc[df['local_authority'] == 'ENGLAND'].index
        skip_header_rows = df.iloc[start_index[0]:].reset_index(drop=True)
        # print(skip_header_rows.info())
        df = copy.deepcopy(skip_header_rows)

        cond = df["system_id"].notna()
        dropped_null_sys_id = df.loc[cond].reset_index(drop=True)
        # print(dropped_null_sys_id.info())
        df = copy.deepcopy(dropped_null_sys_id)

        dropped_null_rows = ProjUtil.drop_empty_axis(df, axis='rows', valid_row_indicator=3)
        # print(dropped_null_rows.info())
        df = copy.deepcopy(dropped_null_rows)

        added_quarter_indicator = ProjUtil.add_constant_col(df=df, new_colname=assign_col, const_val=assign_val)
        # print(added_quarter_indicator.info())

        df = copy.deepcopy(added_quarter_indicator)
        # print(df.head(10))

        # engineer neighbouring local authority indicator
        neighbouring_la_list = ['Southwark', 'Islington',
                                'Haringey', 'Lambeth',
                                'Tower Hamlets', 'Camden',
                                'Waltham Forest', 'Hammersmith & Fulham',
                                'Newham']
        added_neighbouring_la_indicator = df['local_authority'].apply(lambda x: 1 if x in neighbouring_la_list else 0)
        df['is_neighbouring_la'] = added_neighbouring_la_indicator
        # print(added_neighbouring_la_indicator)

        # replace system placeholder for missing values ".." with a more intuitive one - ""
        col_list = ['households_in_ta',
                    'couple_with_children_ta',
                    'single_father_with_children_ta',
                    'single_mother_with_children_ta',
                    'single_parent_of_other_unknown_gender_with_children_ta',
                    'single_man_ta',
                    'single_woman_ta',
                    'single_other_gender_ta',
                    'all_other_household_types_ta']
        for i in range(len(col_list)):
            replace_null_placeholder = df[col_list[i]].astype("str").str.replace("..", "")
            df[col_list[i]] = copy.deepcopy(replace_null_placeholder)
            print(col_list[i])
        print(df.loc[df['local_authority'] == "Camden"])

        # recast column datatypes
        intg_cols = ['households_in_ta',
                    'couple_with_children_ta',
                    'single_father_with_children_ta',
                    'single_mother_with_children_ta',
                    'single_parent_of_other_unknown_gender_with_children_ta',
                    'single_man_ta',
                    'single_woman_ta',
                    'single_other_gender_ta',
                    'all_other_household_types_ta']
        # fltg_cols = ['total_households_in_area_000s']
        print(df[intg_cols].head(5))
        changed_data_types = ProjUtil.recast_dtypes(df, int_cols=intg_cols)  # , flt_cols=fltg_cols)
        print(changed_data_types.info())
        df = copy.deepcopy(changed_data_types)

        print("\nCLEANING COMPLETE!")
        return df