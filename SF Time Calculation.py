from simple_salesforce import Salesforce
import configparser
import pandas as pd

def login():
    config_file_name = "path_for_login_config_file.CONFIG"
    config = configparser.ConfigParser()
    config.read(config_file_name)
    user = config['LOGIN']['user']
    pw = config['LOGIN']['pw']
    tok = config['LOGIN']['tok']
    dom = config['LOGIN']['dom']
    sf = Salesforce(username=user, password=pw, security_token=tok, domain=dom)
    return sf

def get_table_names():
    files_created_times_table = "API_first_table_name"
    event_times_table = "API_second_table_name"
    return files_created_times_table, event_times_table

def get_column_names():
    files_created_times_table_columns = ["T1C1", "T1C2", "T1C3", "T1C4", "T1C5", "T1C6"]
    event_times_table_columns = ["T2C1", "T2C2", "T2C3", "T2C4", "T2C5"]
    return files_created_times_table_columns, event_times_table_columns

def create_time_file_created_table(columns, table, sf):
    query = "SELECT {} FROM {}".format(','.join(columns), table)
    time_created_table = pd.DataFrame(sf.query_all(query)['records']).drop(columns=['attributes'])
    return time_created_table

def create_time_of_event_table(columns, table, sf):
    query = "SELECT {} FROM {}".format(','.join(columns), table)
    event_timestamp_table = pd.DataFrame(sf.query_all(query)['records']).drop(columns=['attributes'])
    return event_timestamp_table

def join_table(table1, table2, table1_key, table2_key):
    joined_table = pd.merge(table1, table2, how='inner', left_on=table1_key, right_on=table2_key)
    return joined_table


def main():
    files_created_times_table, event_times_table = get_table_names()
    files_created_times_table_columns, event_times_table_columns = get_column_names()

    files_created_times_table = create_time_file_created_table(files_created_times_table_columns,
                                                               files_created_times_table, sf=login())

    #filtering file_created_times_table below
    files_created_times_table = files_created_times_table[(files_created_times_table['field_to_check_column_name'] ==
                                                           'string_to_check_to_filter_for') |
                                                          (files_created_times_table['field_to_check_column_name'] ==
                                                           'string_to_check_to_filter_for') |
                                                          (files_created_times_table['field_to_check_column_name'] ==
                                                           'string_to_check_to_filter_for')]
    files_created_times_table.to_csv("path_to_save_table_to_csv", sep='\t', index=False)

    event_times_table = create_time_of_event_table(event_times_table_columns, event_times_table, sf=login())
    event_times_table.to_csv("path_to_save_table_to_csv", sep='\t', index=False)

    files_created_times_table = pd.read_csv("path_for_files_created_times_table_saved_previously", sep='\t')
    event_times_table = pd.read_csv("path_for_event_times_table_saved_previously", sep='\t')
    files_created_times_table = files_created_times_table[(files_created_times_table['field_to_check_column_name'] ==
                                                           'string_to_check_to_filter_for') |
                                                          (files_created_times_table['field_to_check_column_name'] ==
                                                           'string_to_check_to_filter_for')]
    files_created_times_table = files_created_times_table[(files_created_times_table['field_to_check_column_name'] ==
                                                           'string_to_check_to_filter_for') |
                                                          (files_created_times_table['field_to_check_column_name'] ==
                                                           'string_to_check_to_filter_for') |
                                                          (files_created_times_table['field_to_check_column_name'] ==
                                                           'string_to_check_to_filter_for')]
    files_created_times_table["ID_field_to_format_values_for_filter_below"] = \
        files_created_times_table["ID_field_to_format_values_for_filter_below"].str.slice(stop=15)
    joined_table = join_table(table1=files_created_times_table, table2=event_times_table,
                              table1_key="filed_created_times_table_key_to_event_times_table",
                              table2_key="event_times_table_key_to_filed_created_times_table")
    joined_table = joined_table[(joined_table['field_to_check_column_name'] == "string_to_check_to_filter_for") |
                                (joined_table['field_to_check_column_name'] == "string_to_check_to_filter_for") |
                                (joined_table['field_to_check_column_name'] == "string_to_check_to_filter_for")]
    joined_table = joined_table.sort_values(by=['field_to_sort_table_by_newest_first'])
    joined_table = joined_table.groupby("second_field_to_group_rows_and_sort_by").first()
    joined_table["field_name_to_format_values_to_datetime_accepted_format"] = joined_table["field_name_to_format_values_to_datetime_accepted_format"].replace(to_replace='T', value=' ', regex=True)
    joined_table["second_field_name_to_format_values_to_datetime_accepted_format"] = joined_table["second_field_name_to_format_values_to_datetime_accepted_format"].replace(to_replace='T', value=' ', regex=True)
    joined_table["field_name_to_format_values_to_datetime_accepted_format"] = joined_table["field_name_to_format_values_to_datetime_accepted_format"].str.slice(stop=19)
    joined_table["second_field_name_to_format_values_to_datetime_accepted_format"] = joined_table["second_field_name_to_format_values_to_datetime_accepted_format"].str.slice(stop=19)

    joined_table["field_formatted_above_to_read_as_datetime"] = pd.to_datetime(joined_table["field_formatted_above_to_read_as_datetime"])
    joined_table["second_field_formatted_above_to_read_as_datetime"] = pd.to_datetime(joined_table["second_field_formatted_above_to_read_as_datetime"])
    joined_table = joined_table[(joined_table["first_field_formatted_above"].dt.year >= 2022) & (joined_table["first_field_formatted_above"].dt.month == 9)]
    #filter for day of week and time below
    # weekday_table = joined_table[(joined_table["first_field_formatted_above"].dt.dayofweek < 5) & (joined_table["first_field_formatted_above"].dt.hour > 12)]
    # saturday_table = joined_table[(joined_table["first_field_formatted_above"].dt.dayofweek == 5) & (joined_table["first_field_formatted_above"].dt.hour > 13) & (joined_table["first_field_formatted_above"].dt.hour < 23)]
    # sunday_table = joined_table[(joined_table["first_field_formatted_above"].dt.dayofweek == 6) & (joined_table["first_field_formatted_above"].dt.hour > 16) & (joined_table["first_field_formatted_above"].dt.hour < 22)]
    # weekday_add_saturday_table = weekday_table.append(saturday_table)
    # add_sunday_table = weekday_add_saturday_table.append(sunday_table)
    # joined_table = add_sunday_table
    joined_table["new_field_to_create_for_difference_in_time"] = (joined_table["later_time_to_be_subtracted_from"] - joined_table["initial_time_to_subtract"]).astype('timedelta64[s]')
    joined_table = joined_table[(joined_table["difference_in_time_field"] < 1800)]
    joined_table = joined_table.sort_values("difference_in_time_field")
    print(files_created_times_table)
    joined_table.to_csv("path_to_save_final_table_to", sep='\t')
    print(joined_table["difference_in_time_field"].mean())
    # print(joined_table.groupby(["difference_in_time_field"] < 300)).count()
    print(joined_table.groupby('field_to_group_values_by')["difference_in_time_field"].mean())
    print(joined_table.groupby('field_to_group_values_by')["difference_in_time_field"].mean())
    print(joined_table.groupby(['field_to_group_values_by', 'second_field_to_group_values_by']).count())
    print(joined_table.groupby(['field_to_group_values_by']).count())

if __name__ == "__main__":
    main()
