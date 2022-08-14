import json
import uuid
import time
import pandas as pd
from utils import settings, functions, compare


def main(df_web):
    """
    Compare the input data set and jobs collection. Then inserts/modifies the jobs collection.
    :param df_web:(df) The data from the scraping.
    :return insert_stat, modify_stat:True or False
    """
    if not df_web.empty:
        spider_name_list = set(df_web.loc[:, 'website'].values.tolist())
        query_name_list = [{"website": i} for i in spider_name_list]
        col, client = functions.connect_db(settings.DB_INFO['database'], 'jobs')
        cursor = col.find({"$or": query_name_list, 'isExpired': 'False'}, {'url': 1})
        df_db = pd.DataFrame(list(cursor))
        df_new, df_expired = compare.compare(df_web, df_db)
        if not df_new.empty:
            # Find new data in scraping, need to insert into job collection.
            df_new['_id'] = df_new['url'].apply(lambda x: str(uuid.uuid4()))
            df_new['scrape_date'] = time.strftime('%Y-%m-%d', time.localtime())
            df_new['isExpired'] = 'False'
            df_new.reset_index(drop=True, inplace=True)
            # Insert new data to the jobs collection.
            print(f"Begin to input {spider_name_list}  {df_new.shape[0]} data to jobs.")
            new_data_json = json.loads(df_new.T.to_json()).values()
            col.insert_many(new_data_json)
            insert_stat = True
        else:
            print("Sorry, not new data. So not needs to input jobs.".center(30, '-'))
            insert_stat = False
        if not df_expired.empty:
            # Find the expired data, need to modify jobs collection. Make flag to 'isExpired'.
            df_expired = df_expired.loc[:, ['url']]
            df_expired.reset_index(drop=True, inplace=True)
            print(f"Will set {df_expired.shape[0]} rows data expired in jobs")
            data_list = df_expired.loc[:, ['url']].values.tolist()
            for i in data_list:
                col.update_one({"url": i[0]}, {"$set": {"isExpired": "True"}})
            modify_stat = True
        else:
            print("Sorry, No expired data in jobs.".center(30, '-'))
            modify_stat = False
        client.close()
    else:
        print("The input data set is empty.")
        insert_stat = modify_stat = False
    return insert_stat, modify_stat


if __name__ == '__main__':
    # with open('d:/scrapy.json', encoding='UTF-8') as f:
    #     a = json.load(f)
    # df = pd.read_json(a, orient='columns')
    # print(main(df))
    pass
