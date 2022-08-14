import pandas as pd
from utils import settings, functions


def main():
    """
    Extract data from the jobs collection.
    Compare the data from jobs and postedjobs.
    If have new data, add listing id, then insert to the postedjobs collection.Using the sa-service to match listing id.
    IF have expired data, modify the postedjobs collection.
    :return result:True or False
    """
    insert_stat = modify_stat = False
    # Get the valid data from jobs collection
    df_jobs = functions.df_from_table(settings.DB_INFO['database'], 'jobs', 'find', {'isExpired': 'False'})
    # Delete the duplicate data from jobs collection
    print(f"Get {df_jobs.shape[0]} rows from jobs collection.")
    if not df_jobs.empty:
        df_temp = df_jobs.groupby(['jobTitle', 'issuer', 'location'])['url'].apply(functions.concat_func)
        df_new = df_temp.to_frame().reset_index()
        df_jobs.drop_duplicates(subset=['jobTitle', 'issuer', 'location'], keep='first', inplace=True)
        df_merge = pd.merge(df_jobs, df_new, on=['jobTitle', 'issuer', 'location'])
        df_merge.drop('url_x', axis=1, inplace=True)
        df_jobs_no_dup = df_merge.rename(columns={'url_y': 'url'})
        df_jobs_no_dup['url'] = df_jobs_no_dup['url'].apply(lambda x: x[0][0])
        print(f"df_jobs_no_dup have {df_jobs_no_dup.shape[0]} rows")
        # Get the valid data from postedjobs collection
        df_postedjobs = functions.df_from_table(settings.DB_INFO['database'], 'postedjobs', 'find',
                                                {'isExpired': 'False'}, {'url': 1})
        print(f"Get {df_postedjobs.shape[0]} rows from postedjobs collection.")
        # Compare jobs and postedjobs, get the new data of jobs.
        if not df_postedjobs.empty:
            df_postedjobs.drop('_id', axis=1, inplace=True)
            df_res = df_jobs_no_dup.merge(df_postedjobs, how="outer", on='url', indicator='i', suffixes=('', '_right'))
            # Find the new data in jobs collection
            res_jobs = df_res[df_res['i'] == 'left_only'].drop(['i'], axis=1)
            # Find the expire data in postjobs collection, they can't find in jobs but exist in postjobs.
            if res_jobs.empty:
                print("Compare two tables, not find new data.")
            else:
                print(f"Compare two tables, find {res_jobs.shape[0]} rows new data in jobs collection.")
                insert_df = res_jobs
                insert_stat = True
            res_postedjobs = df_res[df_res["i"] == 'right_only'].drop(['i'], axis=1)
            if res_postedjobs.empty:
                print("Compare two tables, not find expired data.")
                modify_stat = False
            else:
                # Update the postedjobs, set the flag for expired data
                res_postedjobs_list = res_postedjobs.loc[:, ['url']].values.tolist()
                col, client = functions.connect_db(settings.DB_INFO['database'], 'postedjobs')
                for i in res_postedjobs_list:
                    col.update_one({"url": i[0]}, {"$set": {"isExpired": "True"}})
                client.close()
                print(f"Modify {res_postedjobs.shape[0]} rows as Expired in postedjobs collection.")
                modify_stat = True
        else:
            print("Will input total data from jobs to postedjobs because postedjobs collection is empty.")
            insert_df = df_jobs_no_dup
            insert_stat = True
        # Get the listing id and input new data to the postedjobs collection
        if insert_stat:
            df_listing = functions.get_listing_id(insert_df)
            df_listing_no_dup = functions.delete_duplicate_rows(df_listing)
            insert_result = functions.df_into_table(settings.DB_INFO["database"], 'postedjobs', df_listing_no_dup)
            if insert_result:
                print(f"Insert {df_listing_no_dup.shape[0]} rows from jobs to postedjobs successfully.")
                insert_stat = True
            else:
                print("Insert data from jobs to postedjobs failed.")
                insert_stat = False
    else:
        print("Jobs collection has not valid data.")
    return insert_stat, modify_stat


if __name__ == '__main__':
    result_task = main()
    print(result_task)

