import sys
import uuid
import json
import certifi
import requests
import traceback
import pandas as pd
from pymongo import MongoClient
import settings


def connect_db(db_name='noah', table_name='jobs'):
    """
    Connet to the MongoDB table.
    :param db_name: (str) The database's name of you want to connect.
    :param table_name: (str) The table's name of you want to connect.
    :return collection, client:(Tuple)
    """
    db_info = settings.DB_INFO
    if db_info['port']:  # Only for test in Win10, will be delete in Prod.
        print("Using Vmare test environment.")
        client = MongoClient(db_info['host'], db_info['port'])
    else:
        print("Using WayBase dev environment.")
        ca = certifi.where()
        client = MongoClient(db_info['host'], tlsCAFile=ca)
    database = client[db_name]
    collection = database[table_name]
    return collection, client


def df_from_table(db_name, table_name, mode='find', filter_q={}, column_q={}):
    """
    Obtain all data from the specified table.
    :param db_name:(str) Which MongoDB database you want to access.
    :param table_name:(str) Which table you want to access.
    :param mode:(str) Such as 'find' or 'aggregate'
    :param filter_q:(dict) Such as {} or {'isExpired':'False'}
    :param column_q:(dict) Such as {} or {"url": 1}
    :return df_result:(df) The dataframe which are are fully data of the table.
    """
    try:
        collection, client = connect_db(db_name, table_name)
        if mode == 'find':
            cursor = collection.find(filter_q, column_q)
        else:
            return pd.DataFrame(columns=["url"])
        df = pd.DataFrame(list(cursor))
        client.close()
        return df
    except Exception as error:
        print(error)
        print("Problem downloading ", table_name, " collection")
        return pd.DataFrame(columns=["url"])


def delete_duplicate_rows(df):
    """
    Delete the duplicate rows in the dataframe. Judge whether is same of .
    Delete one of them if the rows of the 'jobTitle', 'listingId' and 'issuer' are same.
    It is wrong that only judge the 'jobTitle' and 'listingId',
    because some items 'listingId' often are empty and 'jobTitle' are some,
    such as https://christiancareerscanada.com/careers/9699 and https://christiancareerscanada.com/careers/9616
    Only reserve the oldest item, delete the newer items because they are been copied from the oldest one.
    :param df:(df)
    :return df_merge:(df)
    """
    df['listingId'] = df['listingId'].apply(lambda x: str(x))  # avoid:  TypeError: unhashable type: 'list'
    try:
        series_new = df.groupby(['jobTitle', 'listingId', 'issuer'])['timestamp'].min()
    except Exception as error:
        print(error)
        print(traceback.format_exc())
        sys.exit()
    temp_df = series_new.to_frame()
    temp_df = temp_df.reset_index()
    df_merge = temp_df.merge(df, how='left', on=['jobTitle', 'listingId', 'issuer', 'timestamp'], indicator='i',).query("i=='both'")
    df_merge.drop('i', axis=1, inplace=True)
    return df_merge


def df_into_table(db_name, table_name, df_input):
    """
    Insert the input_df data to the table of MongoDB.
    :param df_input: (df) The data which you will used to insert.
    :param db_name:(str) Which MongoDB database you want to access.
    :param table_name:(str) Which table you want to access.
    :return True or False: (Boolean)
    """
    try:
        collection, client = connect_db(db_name, table_name)
        json_new_data = json.loads(df_input.T.to_json()).values()
        collection.insert_many(json_new_data)
        client.close()
        return True
    except Exception as error:
        print({error})
        print('Error happened. '.center(30, '-'))
        print(traceback.format_exc())
        return False


def post_get_data(name, postcode, street, region_locality):
    """
    Get data from API by POST way.
    :param name:(str)
    :param postcode:(str)
    :param street:(str)
    :param region_locality:(str)
    :return result:(dictionary)
    """
    name = name.strip() if name else ''
    postcode = postcode.strip() if postcode else ''
    street = street.strip() if street else ''
    region_locality = region_locality.strip() if region_locality else ''
    data = {
        "name": name,
        "postcode": postcode,
        "street": street,
        "region_locality": region_locality,
    }
    try:
        res = requests.post(url=settings.DA_SERVICES_URL, data=json.dumps(data))
        result = json.loads(res.text)["results"]
    except Exception as error:
        print(error)
        print(f"name=:{name}, street=:{street}, postcode=:{postcode}")
        print(traceback.format_exc())
        sys.exit()
    return result


def get_listing_id(df):
    """
    Get the data from postedjobs table, then get the 'listingId' by web API.
    :param df: (df) The dataframe which needs to add 'listingId' in it.
    :return result_df: (df) Columns include _id(jobs table's id), near, notes
    """
    columns = ['_id', 'issuer', 'postalCode', 'street', 'regionLocality']
    if set(columns).issubset(df.columns):
        df_temp = df.loc[:, columns]
        df_temp = df_temp.rename(columns={'postalCode': 'pst', 'street': 'ad', 'regionLocality': 'rl'})
        df_temp['value'] = df_temp.apply(
            lambda row: post_get_data(row['issuer'],
                                      row['pst'],
                                      row['ad'],
                                      row['rl']), axis=1)
        df_temp['listingId'] = df_temp["value"].apply(lambda x: x["listingId"])
        df_temp['matchingResults'] = df_temp["value"].apply(lambda x: {'score': x['score'],
                                                                       'textMatch': x['textMatch'],
                                                                       'near': x['near'],
                                                                       'notes': x['notes']})
        df_temp.drop('value', axis=1, inplace=True)
        df_temp.drop(['issuer', 'pst', 'ad', 'rl'], axis=1, inplace=True)
        result_df = pd.merge(df, df_temp, on='_id')
    else:
        print("Lack of columns in input df")
        result_df = df
    return result_df


def input_listing_to_db(df):
    """
    Get the listing id and input new data to the postedjobs collection
    :param df: (df) Index(['_id', 'description', 'issuer', 'jobTitle', 'location', 'postalCode','postedDate',
    'regionLocality', 'timestamp', 'url', 'website', 'scrapeDate', 'isExpired']
    :return insert_count: (int)
    """
    insert_count = 0
    df_listing = get_listing_id(df)
    df_listing_no_dup = delete_duplicate_rows(df_listing)
    df_listing_no_dup['_id'] = df_listing_no_dup['_id'].apply(lambda x: str(uuid.uuid4()))
    insert_result = df_into_table(settings.DB_INFO["database"], 'postedjobs', df_listing_no_dup)
    if insert_result:
        insert_count = df_listing_no_dup.shape[0]
        print(f"Insert {insert_count} rows from jobs to postedjobs successfully.")
    else:
        print("Insert data from jobs to postedjobs failed.")
    return insert_count


def compare_jobs_postedjobs(df_jobs, df_postedjobs):
    """
    Compare jobs and postedjobs, get the new data from jobs. Return the comparing result, includes two df.
    :param df_jobs_no_dup: (df)
    :param df_postedjobs: (df)
    :return res_jobs: (df) The data in jobs are new, so them will be input into postedjobs.
    :return res_postedjobs: (df) The data in postedjobs will be flagged as expired.
    """
    # Merge to compare
    df_postedjobs.drop('_id', axis=1, inplace=True)
    df_res = df_jobs.merge(df_postedjobs,
                           how="outer",
                           on=['url', 'jobTitle', 'description', 'postedDate', 'issuer', 'location'],
                           indicator='i',
                           suffixes=('', '_right'))
    # Find the expire data in postjobs collection, they can't find in jobs but exist in postjobs.
    res_postedjobs = df_res[df_res["i"] == 'right_only'].drop(['i'], axis=1)
    # Find the new data in jobs collection
    res_jobs = df_res[df_res['i'] == 'left_only'].drop(['i'], axis=1)
    if res_jobs.empty:
        print("Compare two tables, not find new data.")
    else:
        print(f"Compare two tables, find {res_jobs.shape[0]} rows new data in jobs collection.")
    return res_jobs, res_postedjobs


def modify_postedjobs_expired(df):
    """
    # Update the postedjobs, set the flag for expired data
    :param df: The data in postedjobs will be flagged as expired.
    :return modify_count: (int)
    """
    modify_count = 0
    if not df.empty:
        res_postedjobs_list = df.loc[:, ['url']].values.tolist()
        res_postedjobs_list = [i for j in res_postedjobs_list for i in j]
        col, client = connect_db(settings.DB_INFO['database'], 'postedjobs')
        col.update_many({'url': {'$in': res_postedjobs_list}}, {"$set": {"isExpired": "True"}})
        client.close()
        modify_count = df.shape[0]
        print(f"Modify {modify_count} rows as Expired in postedjobs collection.")
    return modify_count
