import settings
from utils import functions


def main():
    """
    Extract data from the jobs collection.
    Compare the data from the jobs and the postedjobs.
    If have new data, will add listing id, then insert them to the postedjobs collection.
    Using the sa-service to match listing id.
    If have expired data, will modify the postedjobs collection to add expried flag.
    :return insert_count, modify_count:(int)
    """
    insert_count = modify_count = 0
    # Get the valid data from the jobs collection
    df_jobs = functions.df_from_table(settings.DB_INFO['database'], 'jobs', 'find', {'isExpired': 'False'})
    # Delete the duplicate data from the jobs collection
    print(f"Get {df_jobs.shape[0]} rows from jobs collection.")
    if not df_jobs.empty:
        df_jobs_sort = df_jobs.sort_values("postedDate", ascending=False)
        df_jobs_no_dup = df_jobs_sort.drop_duplicates(subset=['jobTitle', 'issuer', 'location'], keep='first')
        # Get the valid data from the postedjobs collection
        df_postedjobs = functions.df_from_table(settings.DB_INFO['database'], 'postedjobs', 'find',
                                                {'isExpired': 'False'}, {'url': 1,
                                                                         'jobTitle': 1,
                                                                         'description': 1,
                                                                         'postedDate': 1,
                                                                         'issuer': 1,
                                                                         'location': 1})
        print(f"Get {df_postedjobs.shape[0]} rows from postedjobs collection.")
        if not df_postedjobs.empty:
            # Compare the jobs and the postedjobs, get the new data from the jobs.
            insert_df, res_postedjobs = functions.compare_jobs_postedjobs(df_jobs_no_dup, df_postedjobs)
            insert_count = insert_df.shape[0]
            if not res_postedjobs.empty:
                print("Compare two tables, find expired data, need to modify postedjobs to expired.")
                modify_count = functions.modify_postedjobs_expired(res_postedjobs)
            else:
                print("Compare two tables, not find expired data.")
        else:
            print("The postedjobs collection is empty. So input total data from jobs to postedjobs.")
            insert_df = df_jobs_no_dup
            insert_count = df_jobs_no_dup.shape[0]
        if insert_count:
            # Get the listing id and input new data to the postedjobs collection
            insert_count = functions.input_listing_to_db(insert_df)
    else:
        print("Jobs collection has not valid data.")
    return insert_count, modify_count


if __name__ == '__main__':
    result_task = main()
    print(result_task)

