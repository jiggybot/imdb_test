import pandas as pd 
import requests
import urllib
from sqlalchemy import create_engine 
import os
import gzip
import shutil
import pymysql
import sqlalchemy as sa
import base64
import hashlib


def create_dbconn():
    """
    Customustom dbconn for reuse.
    """
    try:
        cnxn = create_engine(
        "mysql+pymysql://user:password@localhost/imdb?host=localhost?port=xxxx")
        #Conn string..depending on flavor
        return cnxn
    except:
        raise Exception("No db conn")

def hashkey(sourcedf, *column):
    """
    md5 hashkey based on pk (ie surrogat for joins and more)
    """
    destdf = sourcedf.assign(hashkey = pd.DataFrame(sourcedf[list(column)].values.sum(axis=1))
             [0].str.encode('utf-8').apply(lambda x: (hashlib.md5(x).hexdigest().upper())))
    return destdf

def explode_array_df(df, explodecol, sep, indexcol):
    """
    Explore arrays based on sep
    """
    df[explodecol] = df[explodecol].str.split(sep)
    df = df.explode(explodecol).reset_index(drop=True)
    cols = list(df.columns)
    cols.append(cols.pop(cols.index(indexcol)))
    df = df[cols]
    #Splitscity
    return df

def title_spec(cnxn):
    """
    Create title spec table
    """
     
    title_ak = pd.read_csv(os.path.join(data_path,'title.akas.tsv'),
        dtype = {'titleId':'str', 'ordering':'int', 'title':'str', 'region':'str',
        'language':'str', 'types':'str','attributes':'str',
        'isOriginalTitle':'Int64'},
        sep='\t',na_values='\\N',quoting=3, nrows=rows)

    #Droppa onödiga kolumner
    title_ak.drop(['types', 'attributes'], axis=1, inplace=True)
    
    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['titleId'].astype(str) + title_ak['ordering'].astype(str)
    title_spec = hashkey(title_ak, 'surrogate')
    
    #to db
    title_spec.to_sql('title_spec', cnxn, if_exists='replace', index=None, chunksize = 100000,
                    dtype=    { 'titleId': sa.types.NVARCHAR(length=1000) ,
                                'ordering': sa.INTEGER(), 
                                'title':  sa.types.NVARCHAR(length=1000),
                                'region': sa.types.NVARCHAR(length=1000),
                                'language': sa.types.NVARCHAR(length=1000),
                                'isOriginalTitle': sa.INTEGER(),
                                'surrogate': sa.types.NVARCHAR(length=1000),
                                'hashkey': sa.types.CHAR(length=32)
                                })
    #PK (fk sist pga av inläsning)
    with cnxn.connect() as con:
        con.execute('ALTER TABLE title_spec ADD PRIMARY KEY (titleId, ordering);')

def title_spec(cnxn):
    """
    Create title_spec table.
    """

    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS title_spec;')

    meta = sa.MetaData()
    print("Creating table title_spec...")

    imdbTable = sa.Table(
        "title_spec", meta,
        sa.Column("titleId", sa.VARCHAR(100), primary_key = True),
        sa.Column("ordering", sa.INTEGER(), primary_key = True),
        sa.Column("title", sa.VARCHAR(1000)),
        sa.Column("region", sa.VARCHAR(1000)),
        sa.Column("language", sa.VARCHAR(1000)),
        sa.Column("isOriginalTitle", sa.INTEGER()),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    title_ak = pd.read_csv(os.path.join(data_path,'title.akas.tsv'),
        dtype = {'titleId':'str', 'ordering':'int', 'title':'str', 'region':'str',
        'language':'str', 'types':'str','attributes':'str',
        'isOriginalTitle':'Int64'},
        sep='\t',na_values='\\N',quoting=3, nrows=rows)

    #Droppa onödiga kolumner
    title_ak.drop(['types', 'attributes'], axis=1, inplace=True)
    
    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['titleId'].astype(str) + '-' + title_ak['ordering'].astype(str)
    title_spec = hashkey(title_ak, 'surrogate')
    
    #to db
    title_spec.to_sql('title_spec', cnxn, if_exists='append', index=None, chunksize = 100000)

def title_type(cnxn):
    """
    Create title_type tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS title_type;')

    meta = sa.MetaData()
    print("Creating table title_type...")

    imdbTable = sa.Table(
        "title_type", meta,
        sa.Column("titleId", sa.VARCHAR(100),   primary_key = True),
        sa.Column("ordering", sa.INTEGER(),     primary_key = True),
        sa.Column("types", sa.VARCHAR(1000)),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["titleId", "types", "ordering"]
    title_ak = pd.read_csv(os.path.join(data_path,'title.akas.tsv'),
        dtype = {'titleId':'str','ordering':'int', 'types':'str'},
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak.dropna(subset = ['types'], inplace=True) 
    
    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['titleId'].astype(str) + '-' + title_ak['ordering'].astype(str)  + '-' + title_ak['types'].astype(str) 
    title_type = hashkey(title_ak, 'surrogate')
    
    
    #to db
    title_type.to_sql('title_type', cnxn, if_exists='append', index=None, chunksize = 100000)


def title_attrib(cnxn):
    """
    Create title_attrib tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS title_attrib;')

    meta = sa.MetaData()
    print("Creating table title_attrib...")

    imdbTable = sa.Table(
        "title_attrib", meta,
        sa.Column("titleId", sa.VARCHAR(100),   primary_key = True),
        sa.Column("ordering", sa.INTEGER(),     primary_key = True),
        sa.Column("attributes", sa.VARCHAR(1000)),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["titleId", "ordering", "attributes", ]
    title_ak = pd.read_csv(os.path.join(data_path,'title.akas.tsv'),
        dtype = {'titleId':'str','ordering':'int', 'attributes':'str'},
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak.dropna(subset = ['attributes'], inplace=True) 
    
    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['titleId'].astype(str) + '-' + title_ak['ordering'].astype(str)   
    title_attrib = hashkey(title_ak, 'surrogate')

    #to db
    title_attrib.to_sql('title_attrib', cnxn, if_exists='append', index=None, chunksize = 100000)

def title(cnxn):
    """
    Create title tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS title;')

    meta = sa.MetaData()
    print("Creating table title...")

    imdbTable = sa.Table(
        "title", meta,
        sa.Column("titleId", sa.VARCHAR(100),   primary_key = True),
        sa.Column("titleType", sa.VARCHAR(1000)),
        sa.Column("primaryTitle", sa.VARCHAR(1000)),
        sa.Column("originaltitle", sa.VARCHAR(1000)),
        sa.Column("IsAdult", sa.BOOLEAN),
        sa.Column("startYear", sa.INTEGER()),
        sa.Column("endYear", sa.INTEGER()),
        sa.Column("runtimeMinutes", sa.INTEGER()),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["tconst", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "titleType"]
    title_ak = pd.read_csv(os.path.join(data_path,'title.basics.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'tconst':'titleId'
    })
    
    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['titleId'].astype(str)   
    title = hashkey(title_ak, 'surrogate')
    
    #to db
    title.to_sql('title', cnxn, if_exists='append', index=None, chunksize = 100000)

def title_genre(cnxn):
    """
    Create title_genre tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS title_genre;')

    meta = sa.MetaData()
    print("Creating table title_genre...")

    imdbTable = sa.Table(
        "title_genre", meta,
        sa.Column("titleId", sa.VARCHAR(100),   primary_key = True),
        sa.Column("genre", sa.VARCHAR(200),    primary_key = True) ,
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
        
    )
    meta.create_all(cnxn)

    col_list = ["tconst", "genres"]
    title_ak = pd.read_csv(os.path.join(data_path,'title.basics.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'tconst':'titleId', 'genres':'genre', 
    })
    
    #Explodera and set hashkey.
    exploded = explode_array_df(title_ak, 'genre', ',', 'titleId')
    exploded['surrogate'] = exploded['titleId'].astype(str) + '-' + exploded['genre'].astype(str)  
    title_genre = hashkey(exploded, 'surrogate')

    #Change order
    title_genre = title_genre[['titleId', 'genre', 'surrogate', 'hashkey']]
    title_genre = title_genre[title_genre['genre'].notna()]

    title_genre.to_csv("SNEKTOWN", sep=';')

    #to db
    title_genre.to_sql('title_genre', cnxn, if_exists='append', index=None, chunksize = 100000)

def directors(cnxn):
    """
    Create directors tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS directors;')

    meta = sa.MetaData()
    print("Creating table directors...")

    imdbTable = sa.Table(
        "directors", meta,
        sa.Column("director", sa.VARCHAR(300),   primary_key = True),
        sa.Column("titleId", sa.VARCHAR(100),    primary_key = True, ),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["tconst", "directors"]
    title_ak = pd.read_csv(os.path.join(data_path,'title.crew.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'tconst':'titleId', 'directors':'director', 
    })

    #Dropa nulls
    title_ak = title_ak[title_ak['director'].notna()]
    
    #Explode and set hashkey
    exploded = explode_array_df(title_ak, 'director', ',', 'titleId')
    exploded['surrogate'] = exploded['titleId'].astype(str) + '-' + exploded['director'].astype(str)  
    directors = hashkey(exploded, 'surrogate')
    

    #to db
    directors.to_sql('directors', cnxn, if_exists='append', index=None, chunksize = 100000)

def writers(cnxn):
    """
    Create writers tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS writers;')

    meta = sa.MetaData()
    print("Creating table writers...")

    imdbTable = sa.Table(
        "writers", meta,
        sa.Column("writer", sa.VARCHAR(300),   primary_key = True),
        sa.Column("titleId", sa.VARCHAR(100),   primary_key = True),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["tconst", "writers"]
    title_ak = pd.read_csv(os.path.join(data_path,'title.crew.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'tconst':'titleId', 'writers':'writer', 
    })

    #Dropa nulls
    title_ak = title_ak[title_ak['writer'].notna()]
    
    #Explode and set hashkey
    exploded = explode_array_df(title_ak, 'writer', ',', 'titleId')
    exploded['surrogate'] = exploded['titleId'].astype(str) + '-' + exploded['writer'].astype(str)  
    writers = hashkey(exploded, 'surrogate')
    

    #to db
    writers.to_sql('writers', cnxn, if_exists='append', index=None, chunksize = 100000)

def episodes(cnxn):
    """
    Create episodes tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS episodes;')

    meta = sa.MetaData()
    print("Creating episodes table...")

    imdbTable = sa.Table(
        "episodes", meta,
        sa.Column("episodeId", sa.VARCHAR(300),   primary_key = True),
        sa.Column("titleId", sa.VARCHAR(100),   primary_key = True),
        sa.Column('seasonNumber', sa.INTEGER()),
        sa.Column('episodeNumber', sa.INTEGER()),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["tconst", "parentTconst", "seasonNumber", "episodeNumber"]
    title_ak = pd.read_csv(os.path.join(data_path,'title.episode.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'tconst':'episodeId', 'parentTconst':'titleId'
    })

    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['episodeId'].astype(str) + '-' +  title_ak['titleId'].astype(str)
    episodes = hashkey(title_ak, 'surrogate')
    
    #Dropa nulls
    episodes = episodes[episodes['seasonNumber'].notna()]

    #to db
    episodes.to_sql('episodes', cnxn, if_exists='append', index=None, chunksize = 100000)

def characters(cnxn):
    """
    Create characters tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS characters;')

    meta = sa.MetaData()
    print("Creating table characters...")

    imdbTable = sa.Table(
        "characters", meta,
        sa.Column("titleId",    sa.VARCHAR(100),     primary_key = True),
        sa.Column("personId",   sa.VARCHAR(50),      primary_key = True),
        sa.Column("character",  sa.VARCHAR(100),     primary_key = True),
        sa.Column("surrogate",  sa.VARCHAR(1000)),
        sa.Column("hashkey",    sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    col_list = ["tconst", "nconst", "characters"]
    title_ak = pd.read_csv(os.path.join(data_path,'title.principals.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'tconst':'titleId', 'nconst':'personId'
    })
    
    #Could be done easier lazy [" encasing "]
    title_ak['characters'] = title_ak.characters.str.replace('[\"\[\]]','',regex=True)
    title_ak['characters'] = title_ak.characters.str.replace('\\','|')

    #Exploda spec with multi
    title_ak = title_ak.assign(characters=title_ak.characters.str.split(',')).explode('characters').reset_index(drop=True)

    title_ak = title_ak.rename(columns={
    'characters':'character'
    })


    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['titleId'].astype(str) + '-' + title_ak['personId'].astype(str) + '-' + title_ak['character'].astype(str)
    characters = hashkey(title_ak, 'surrogate')
    
    #Dropa nulls
    characters = characters[characters['character'].notna()]
    
    #to db
    characters.to_sql('characters', cnxn, if_exists='append', index=None, chunksize = 100000)

def cast(cnxn):
    """
    Create cast tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS cast;')

    meta = sa.MetaData()
    print("Creating table cast...")

    imdbTable = sa.Table(
        "cast", meta,
        sa.Column("titleId", sa.VARCHAR(100),   primary_key = True),
        sa.Column("ordering", sa.INTEGER(),     primary_key = True),
        sa.Column("personId", sa.VARCHAR(300)),
        sa.Column("category", sa.VARCHAR(500)),
        sa.Column("job", sa.VARCHAR(500)),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    title_ak = pd.read_csv(os.path.join(data_path,'title.principals.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows)
    
    title_ak = title_ak.rename(columns={
    'tconst':'titleId', 'nconst':'personId'
    })
    
    #Droppa onödiga kolumner
    title_ak.drop(['characters'], axis=1, inplace=True)

    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['titleId'].astype(str) + '-' + title_ak['ordering'].astype(str)
    cast = hashkey(title_ak, 'surrogate')

    #to db
    cast.to_sql('cast', cnxn, if_exists='append', index=None, chunksize = 100000)

def ratings(cnxn):
    """
    Create ratings tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS ratings;')

    meta = sa.MetaData()
    print("Creating table ratings...")

    imdbTable = sa.Table(
        "ratings", meta,
        sa.Column("titleId", sa.VARCHAR(100),   primary_key = True),
        sa.Column("averageRating", sa.FLOAT()),
        sa.Column("numVotes", sa.INTEGER()),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    title_ak = pd.read_csv(os.path.join(data_path,'title.ratings.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows)
    
    title_ak = title_ak.rename(columns={
    'tconst':'titleId'
    })
    
    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['titleId'].astype(str)   
    ratings = hashkey(title_ak, 'surrogate')
    
    #to db
    ratings.to_sql('ratings', cnxn, if_exists='append', index=None, chunksize = 100000)

def person_profession(cnxn):
    """
    Create person_profession tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS person_profession;')

    meta = sa.MetaData()
    print("Creating person_profession table...")

    imdbTable = sa.Table(
        "person_profession", meta,
        sa.Column("personId", sa.VARCHAR(300),   primary_key = True),
        sa.Column("primaryProfession", sa.VARCHAR(100),   primary_key = True),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["nconst", "primaryProfession"]
    title_ak = pd.read_csv(os.path.join(data_path,'name.basics.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'nconst':'personId'
    })

    #Dropa nulls
    title_ak = title_ak[title_ak['primaryProfession'].notna()]
    
    #Explode and set hashkey
    exploded = explode_array_df(title_ak, 'primaryProfession', ',', 'personId')
    exploded['surrogate'] = exploded['personId'].astype(str) + '-' + exploded['primaryProfession'].astype(str)  
    person_profession = hashkey(exploded, 'surrogate')

    #to db
    person_profession.to_sql('person_profession', cnxn, if_exists='append', index=None, chunksize = 100000)

def person_known_for(cnxn):
    """
    Create person_known_for tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS person_known_for;')

    meta = sa.MetaData()
    print("Creating person_known_for table...")

    imdbTable = sa.Table(
        "person_known_for", meta,
        sa.Column("personId", sa.VARCHAR(300),   primary_key = True),
        sa.Column("knownForTitles", sa.VARCHAR(100),   primary_key = True),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["nconst", "knownForTitles"]
    title_ak = pd.read_csv(os.path.join(data_path,'name.basics.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'nconst':'personId'
    })

    #Dropa nulls
    title_ak = title_ak[title_ak['knownForTitles'].notna()]
    
    #Explode and set hashkey
    exploded = explode_array_df(title_ak, 'knownForTitles', ',', 'personId')
    exploded['surrogate'] = exploded['personId'].astype(str) + '-' + exploded['knownForTitles'].astype(str)  
    person_known_for = hashkey(exploded, 'surrogate')

    #to db
    person_known_for.to_sql('person_known_for', cnxn, if_exists='append', index=None, chunksize = 100000)

def person(cnxn):
    """
    Create person tabell. 
    """ 
    with cnxn.connect() as con:
        con.execute('DROP TABLE IF EXISTS person;')

    meta = sa.MetaData()
    print("Creating table person...")

    imdbTable = sa.Table(
        "person", meta,
        sa.Column("personId", sa.VARCHAR(100),   primary_key = True),
        sa.Column("primaryName", sa.VARCHAR(1000)),
        sa.Column("birthYear", sa.INTEGER),
        sa.Column("deathYear", sa.INTEGER),
        sa.Column("surrogate", sa.VARCHAR(1000)),
        sa.Column("hashkey", sa.CHAR(32)),
    )
    meta.create_all(cnxn)

    
    col_list = ["nconst", "primaryName", "birthYear", "deathYear"]
    title_ak = pd.read_csv(os.path.join(data_path,'name.basics.tsv'), 
        sep='\t',na_values='\\N',quoting=3, nrows=rows, usecols=col_list)
    
    title_ak = title_ak.rename(columns={
    'nconst':'personId'
    })
    
    #Business key/surrogate och hasha
    title_ak['surrogate'] = title_ak['personId'].astype(str)   
    person = hashkey(title_ak, 'surrogate')
    
    #to db
    person.to_sql('person', cnxn, if_exists='append', index=None, chunksize = 100000)
#########
#Kör här#
#########
#Specify data path and rows to read for test purpose
if __name__ == "__main__":
    
    data_path = './data'
    rows = 10000
    cnxn = create_dbconn()

    #Run all exporttables
    title_spec(cnxn)
    title_type(cnxn)
    title_attrib(cnxn)
    title(cnxn)
    title_genre(cnxn)
    directors(cnxn)
    writers(cnxn)
    episodes(cnxn)
    characters(cnxn)
    cast(cnxn)
    ratings(cnxn)
    person_profession(cnxn)
    person_known_for(cnxn)
    person(cnxn)


