import json
import uuid
import time
import pandas as pd
from utils import functions, compare
import settings


def main(df_web):
    """
    Compare the input data set and jobs collection. Then inserts/modifies the jobs collection.
    :param df_web:(df) The data from the scraping.
    :return insert_count, modify_count:(int)
    """
    insert_count = modify_count = 0
    if not df_web.empty:
        spider_name_list = set(df_web.website.unique())
        query_name_list = [{"website": i} for i in spider_name_list]
        col, client = functions.connect_db(settings.DB_INFO['database'], 'jobs')
        cursor = col.find({"$or": query_name_list, 'isExpired': 'False'}, {'url': 1,
                                                                           'description': 1,
                                                                           'jobTitle': 1,
                                                                           'location': 1,
                                                                           'postalCode': 1,
                                                                           'postedDate': 1})
        df_db = pd.DataFrame(list(cursor))
        df_new, df_expired = compare.compare(df_web, df_db)
        if not df_new.empty:
            # Find new data in scraping, need to insert into job collection.
            df_new['_id'] = df_new['url'].apply(lambda x: str(uuid.uuid4()))
            df_new['scrapeDate'] = time.strftime('%Y-%m-%d', time.localtime())
            df_new['isExpired'] = 'False'
            df_new.reset_index(drop=True, inplace=True)
            # Insert new data to the jobs collection.
            print(f"Begin to input {spider_name_list}  {df_new.shape[0]} data to jobs.")
            new_data_json = json.loads(df_new.T.to_json()).values()
            col.insert_many(new_data_json)
            insert_count = df_new.shape[0]
        else:
            print("Sorry, not new data. So not needs to input jobs.".center(30, '-'))
        if not df_expired.empty:
            # Find the expired data, need to modify jobs collection. Make flag to 'isExpired'.
            df_expired = df_expired.loc[:, ['url']]
            df_expired.reset_index(drop=True, inplace=True)
            print(f"Will set {df_expired.shape[0]} rows data expired in jobs")
            data_list = df_expired.loc[:, ['url']].values.tolist()
            for i in data_list:
                col.update_one({"url": i[0]}, {"$set": {"isExpired": "True"}})
            modify_count = df_expired.shape[0]
        else:
            print("Sorry, No expired data in jobs.".center(30, '-'))
        client.close()
    else:
        print("The input data set is empty.")
    return insert_count, modify_count

