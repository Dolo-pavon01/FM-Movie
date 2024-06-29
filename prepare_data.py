from numpy import nan
from pandas import DataFrame, concat, isna, Series
from bson.regex import Regex
from datetime import datetime

from mongo import Mongo


log = lambda x: print(f"{datetime.now().replace(microsecond=0)} | {x}")

mongo = Mongo()

db_testing = 'testing'

col_imdb_complete = 'imdb_complete'
col_tmdb_complete = 'tmdb_complete'
col_comments_rating = 'collaborative_db'

col_titlebasics = 'imdbTitleBasics'
col_titleratings = 'imdbTitleRatings'
col_namebasics = 'imdbNameBasics'

col_datatp = 'data_tp'

# Columns that will have the final dataframe
cols = [
    'imdbId',
    'country',
    'releaseDate',
    'votes',
    'rating',
    'reviews',
    'duration',
    'genres',
    'companies',
    'keywords',
    'directors',
    'cast',
]

"""
    imdbId 		: TitleBasics 'imdbId'
    type		: TitleBasics 'Type'
    country 	: _ ''
    releaseDate : _ '
    year		: TitleBasics 'startYear'
    votes 		: imdbTitleRatings 'numVotes'
    rating 		: imdbTitleRatings 'averageRaings'
    reviews 	: collaborative_db 'reviews_analysis.compound'
    duration 	: TitleBasics 'runtimeMinutes'
    genres 		: list TitleBasics 'genres'
    companies 	: list _ ''
    keywords 	: list _ ''
    directors 	: list NameBasics 'primaryName' # Filtrar por 'director' en primaryPosition, abrir por knownForTitles, matchear dejando primaryName
    cast 		: list _ ''
"""

imdbs_list = [
    "tt10133702",
    "tt10333266",
    "tt10350420",
    "tt10399902",
    "tt10402396",
    "tt10469410",
    "tt10712472",
    "tt11244166",
    "tt11257606",
    "tt11281192",
]
query_imdbs = {} # { 'imdbId': { '$in': imdbs_list } }



def get_data_from_mongo(database, collection, query, projection):

    log(f"Searching data in '{database}.{collection}'.")
    df = DataFrame()
    
    total_items = mongo.get_total_documents(
        database=database, collection=collection,
        query=query
    )
    step = 1000000
    log(f"\tThere are {total_items} to get.")

    for start in range(0, total_items+1, step):

        pipeline = [
            {'$skip': start},
            {'$limit': step},
            {'$match': query},
            {'$project': projection},
        ]

        payloads = mongo.get_group_db_item(database=database, collection=collection, query=pipeline)
        df = concat([df, DataFrame(payloads)])
        log(f"\t\t{df.shape[0]} / {total_items}".ljust(100))#, end='\r')

    if 'imdbId' in df.columns:
        df = df.dropna(subset='imdbId')
    df = df.replace(to_replace='\\N', value=None)
    df = df.replace(to_replace='\\\\N', value=None)

    log(f"\t{df.shape[0]} total items got.\n")
    return df



# Imdb TitleBasics data
projection = {
    '_id': 0,
    'imdbId': '$imdbId',
    'type': '$titleType',
    'year': '$startYear',
    'duration': '$runtimeMinutes',
    'genres': '$genres',
}
df_titlebasics = get_data_from_mongo(
    database=db_testing, collection=col_titlebasics, 
    query=query_imdbs, projection=projection
)
df_titlebasics['genres'] = df_titlebasics['genres'].apply(lambda x: x.split(',') if not ( isna(x) or x is None ) else None)
log(f"{df_titlebasics.shape[0]} items from {col_titlebasics} \n{df_titlebasics.head(2)}\n")
df = df_titlebasics.copy() # Columns: imdbId, type, year, duration, genres
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")
del df_titlebasics


# Imdb TitleRatings data
projection = {
    '_id': 0,
    'imdbId': '$imdbId',
    'rating': '$averageRating',
    'votes': '$numVotes',
}
df_titleratings = get_data_from_mongo(
    database=db_testing, collection=col_titleratings, 
    query=query_imdbs, projection=projection
)
log(f"{df_titleratings.shape[0]} items from {col_titleratings} \n{df_titleratings.head(2)}\n")
df = df.merge(df_titleratings, how='left', on='imdbId') # Add columns rating, votes
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")
del df_titleratings


# Letterboxd reviews data 
query = {} # { 'imdb_id': { '$in': imdbs_list } }
projection = {
    '_id': 0,
    'imdbId': '$imdb_id',
    'reviews': '$reviews_analysis.compound',
}
df_collaborativedb = get_data_from_mongo(
    database=db_testing, collection=col_comments_rating, 
    query=query, projection=projection
)
df_collaborativedb = df_collaborativedb.groupby('imdbId').agg({'reviews':'max'}).reset_index()
log(f"{df_collaborativedb.shape[0]} items from {col_comments_rating} \n{df_collaborativedb.head(2)}\n")
df = df.merge(df_collaborativedb, how='left', on='imdbId') # Add columns: reviews
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")
del df_collaborativedb


# # Imdb NameBasics data (directors)
query = {} # { '$or': [ { 'knownForTitles':Regex(f".*{v}.*", "i") } for v in imdbs_list ] }
projection = {
    '_id': 0,
    'profession': '$primaryProfession',
    'name': '$primaryName',
    'titles': '$knownForTitles'
}
df_namebasics = get_data_from_mongo(
    database=db_testing, collection=col_namebasics, 
    query=query, projection=projection
)
df_namebasics = df_namebasics[ df_namebasics['profession'].apply( lambda x: 'director' in x if not ( isna(x) or x is None ) else False ) ]
df_namebasics = df_namebasics[[ 'name', 'titles' ]]
df_namebasics['titles'] = df_namebasics['titles'].apply( lambda x: x.split(',') if not ( isna(x) or x is None ) else None )
df_namebasics = df_namebasics.explode(column='titles').rename(columns={'titles':'imdbId'})
# df_namebasics = df_namebasics[ df_namebasics['imdbId'].isin(imdbs_list) ]
df_directors = df_namebasics.groupby('imdbId').agg({'name':list}).reset_index()
df_directors = df_directors.rename(columns={'name':'directors'})
log(f"{df_directors.shape[0]} items from {col_namebasics} \n{df_directors.head(2)}\n")
df = df.merge(df_directors, how='left', on='imdbId') # Add column: directors
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")
del df_directors, df_namebasics


# Imdb complete data 
query = {} # { 'Id': { '$in': imdbs_list } }
projection = {
    '_id': 0,
    'imdbId': '$Id',
    'cast': '$Cast',
    'country': '$Country',
    'releaseDate': '$ReleaseDate',
    'keywords': '$Keywords',
    'companies': '$Companies',
}
df_imdb_complete = get_data_from_mongo(
    database=db_testing, collection=col_imdb_complete, 
    query=query, projection=projection
)
cond_company = df_imdb_complete['companies'].notna()
df_imdb_complete.loc[cond_company, 'companies'] = df_imdb_complete.loc[cond_company, 'companies'].apply( 
     lambda x: [ i['Name'] for i in x if i.get('Name') ] )
cond_country = df_imdb_complete['country'].notna()
df_imdb_complete.loc[cond_country, 'country'] = df_imdb_complete.loc[cond_country, 'country'].apply( lambda x: x[0] )
log(f"{df_imdb_complete.shape[0]} items from {col_imdb_complete} \n{df_imdb_complete.head(2)}\n")
df = df.merge(df_imdb_complete, how='left', on='imdbId') # Add columns: cast, country, releaseDate, keywords, companies
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")
del df_imdb_complete


# Tmdb complete data
projection = {
    '_id': 0,
    'imdbId': 1,
    'releaseDate': '$ReleaseDate',
    'votes': '$vote_count',
    'rating': '$vote_average',
    'duration': '$Duration',
    'genres': '$Genres',
    'country': '$Country',
    'keywords': '$Keywords',
    'cast': '$Cast',
    'companies': '$Companies',
    'directors': '$Directors'
}
df_tmdb = get_data_from_mongo(
    database=db_testing, collection=col_tmdb_complete, 
    query=query_imdbs, projection=projection
)
df_tmdb = df_tmdb.dropna(subset='imdbId')
df_tmdb = df_tmdb.drop_duplicates(subset='imdbId')
cond_company = df_tmdb['companies'].notna()
df_tmdb.loc[cond_company, 'companies'] = df_tmdb.loc[cond_company, 'companies'].apply( 
     lambda x: [ i['Name'] for i in x if i.get('Name') ] )
cond_country = df_tmdb['country'].notna()
df_tmdb.loc[cond_country, 'country'] = df_tmdb.loc[cond_country, 'country'].apply( lambda x: x[0] )
log(f"{df_tmdb.shape[0]} items from {col_tmdb_complete} \n{df_tmdb.head(2)}\n")


log(f"Setting null values")
df = df.fillna(value=nan).replace(to_replace=nan, value=None)
df = df.replace(to_replace='', value=None)
df = df.replace(to_replace='\\N', value=None)
df = df.replace(to_replace='\\\\N', value=None)
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")


def complete_data(df_data, df_tmdb):

    cols = df_data.columns.tolist()

    log(f"Completing data for {df_data.shape[0]} rows:\n")

    fields = [
		'releaseDate',
		'votes',
		'rating',
		'duration',
        'genres',
        'country',
        'keywords',
        'cast',
        'companies',
        'directors',
    ]

    for index, field in enumerate(fields):

        log(f"\tField '{field}' {index+1}/{len(fields)}")

        condition = ( df_data[field].isna() ) | ( df_data[field]=="" )
        null_data = df_data[ condition ]
        if null_data.empty: continue
        not_null_data = df_data[ ~condition ]
        log(f"\t\t{df_data['imdbId'].unique().size}  -  {null_data.shape[0]} null data ({null_data['imdbId'].unique().size})  |  {not_null_data.shape[0]} not null data ({not_null_data['imdbId'].unique().size}) .")

        df_tmdb_info = df_tmdb[['imdbId', field]]
        df_tmdb_info = df_tmdb_info[ ( df_tmdb_info[field].notna() ) & ( df_tmdb_info[field]!="" ) ]

        null_data.drop(columns=field, inplace=True)
        null_data = null_data.merge(df_tmdb_info, on='imdbId', how='left')
        log(f"\t\tData set : ")
        log(f"\t\t{df_data.shape[0]} - {df_data['imdbId'].unique().size}  |  {null_data.shape[0]} - {null_data['imdbId'].unique().size})  |  {not_null_data.shape[0]} - {not_null_data['imdbId'].unique().size}) .")

        df_data = concat([ null_data[cols] , not_null_data[cols] ])
        log(f"\t\tConcatenated : {df_data.shape[0]} rows.\n")
	
    return df_data


df = complete_data(df_data=df, df_tmdb=df_tmdb)
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")
del df_tmdb


log(f"Setting null values")
df = df.fillna(value=nan).replace(to_replace=nan, value=None)
df = df.replace(to_replace='', value=None)
df = df.replace(to_replace='\\N', value=None)
df = df.replace(to_replace='\\\\N', value=None)
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")


i, j, n = 0, 0, df.shape[0]
log(f"Inserting {n} items to mongo")
items_to_insert = list()
for index, row in df.iterrows():
    i += 1
    j += 1
    doc = row.to_dict()
    for k, v in doc.items():
        if v is nan:
            doc[k] = None
    items_to_insert.append(doc)
    if i >= 10000:
        print(f"\t{j} / {n}".ljust(100), end='\r')
        mongo.insert_many_items(
            database=db_testing, 
            collection=col_datatp, 
            payloads=items_to_insert
        )
        items_to_insert.clear()
        i = 0

mongo.insert_many_items(
    database=db_testing, 
    collection=col_datatp, 
    payloads=items_to_insert
)
items_to_insert.clear()


log(f"Deleting values with no rating")
df = df[ df['rating'].notna() ]
log(f"\t -->> {df.shape[0]} ITEMS <<--\n")
log(f"{df.shape[0]} rows to write in csv.")


def write_csv(df, first):
    csv_filename = 'tp_data.csv'
    mode = 'w' if first else 'a'
    df.to_csv(csv_filename, encoding='utf-8', index=False, header=first, mode=mode)


log(f"Writing data to csv")
n, step = df.shape[0], 50000
for i in range(0, n, step):
    a = df.iloc[i:i+step]
    print(f"\t{i+step} / {n}  |  {a.shape[0]} rows".ljust(100), end='\r')
    write_csv( df=a , first=i==0)


mongo.__del__()


log(f"FINISH")
