import pandas as pd


def compare(df_web, df_db):
    """
    Two dataframe, df_web is from the scrapy and df_db is from the MongdoDB.
    They all have the 'url','description','jobTitle','location','postalCode','postedDate' 6 columns,
        judge new data by them. Compare them and only find the new data of df_web.
    :param df_web:(df) Data from the scrapy.
        columns are: ['description', 'issuer', 'jobTitle', 'location', 'postalCode', 'postedDate', 'regionLocality',
        'street', 'timestamp', 'url', 'website']
    :param df_db:(df) Data from the MongdoDB.
       columns are: ['_id','url','description','jobTitle','location','postalCode','postedDate']
    :return df_new:(df) Only the new data in df_web. Add new column ['_id', 'scrapeDate', 'isExpired']
    :return df_expired:(df) Only the expired 'url' and '_id' in df_db.
    """
    df_expired = pd.DataFrame(columns=["url"])
    if df_db.empty:
        print("!DB is empty")
        df_new = df_web
    else:
        print("DB has data. Begin to compare.")
        df = df_web.merge(df_db,
                          how="outer",
                          on=['url', 'description', 'jobTitle', 'location', 'postalCode', 'postedDate'],
                          indicator='i',
                          suffixes=('', '_right'))
        df_new = df.loc[df["i"] == 'left_only'].drop('i', axis=1)
        df_expired = df.loc[df["i"] == 'right_only'].drop('i', axis=1)
        df_expired = df_expired.loc[:, ['_id', 'url']]
    return df_new, df_expired
