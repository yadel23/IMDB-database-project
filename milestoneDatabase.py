# Yadel Negash
# Aidan Schmid


import json
import os
import pandas
import csv

from google.cloud.sql.connector import connector

# name db = ng-nuance-331022:us-central1:cscmilestoneproject
# user_name = root
# password: csc325password


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'eng-nuance-331022-a6900de56068.json'


# make the connection to the db
def make_connection():
    return connector.connect(
        "eng-nuance-331022:us-central1:cscmilestoneproject",
        "pymysql",
        user="root",
        password="csc325password",
        database=None
    )


def setup_db(cur):
    # Set up db
    cur.execute('DROP DATABASE IF EXISTS movies_db;')
    cur.execute('CREATE DATABASE IF NOT EXISTS movies_db;')
    cur.execute('USE movies_db;')

    # Main tables
    cur.execute('DROP TABLE IF EXISTS Movies;')
    cur.execute('DROP TABLE IF EXISTS Country;')
    cur.execute('DROP TABLE IF EXISTS Director;')
    cur.execute('DROP TABLE IF EXISTS Genre;')
    cur.execute('DROP TABLE IF EXISTS Language;')

    # All join tables
    cur.execute('DROP TABLE IF EXISTS Genre_Movies;')
    cur.execute('DROP TABLE IF EXISTS Country_Movies;')
    cur.execute('DROP TABLE IF EXISTS Director_Movies;')
    cur.execute('DROP TABLE IF EXISTS Language_Movies;')

    # Creating Movies Table
    cur.execute('''CREATE TABLE Movies (
        movie_id    INT NOT NULL AUTO_INCREMENT,
        movie_title     VARCHAR(120) NOT NULL,
        duration    INT,
        release_year    INT,
        PRIMARY KEY(movie_id)
        );
        ''')

    # Creating Country Table
    cur.execute('''CREATE TABLE Country (
        country_id      INT NOT NULL AUTO_INCREMENT,
        country_name   VARCHAR(120) NOT NULL UNIQUE,
        PRIMARY KEY(country_id)
        );
        ''')

    # Creating Director Table
    cur.execute('''CREATE TABLE Director (
        director_id     INT NOT NULL AUTO_INCREMENT,
        director_name   VARCHAR(120) NOT NULL UNIQUE,
        PRIMARY KEY(director_id)
        );
        ''')

    # Creating Genre Table
    cur.execute('''CREATE TABLE Genre (
        genre_id     INT NOT NULL AUTO_INCREMENT,
        genre_name   VARCHAR(120) NOT NULL UNIQUE,
        PRIMARY KEY(genre_id)
        );
        ''')

    # Creating Language Table
    cur.execute('''CREATE TABLE Language (
        language_id     INT NOT NULL AUTO_INCREMENT,
        language_name   VARCHAR(120) NOT NULL UNIQUE,
        PRIMARY KEY(language_id)
        );
        ''')

    # Creating Genre_Movies Table
    cur.execute('''CREATE TABLE Genre_Movies (
        genre_movies_id     INT NOT NULL AUTO_INCREMENT,
        genre_id1     INT,
        movie_id1    INT,
        FOREIGN KEY(movie_id1) REFERENCES Movies(movie_id),
        FOREIGN KEY(genre_id1) REFERENCES Genre(genre_id),
        PRIMARY KEY(genre_movies_id)
        );
        ''')

    # Creating Country_Movies Table
    cur.execute('''CREATE TABLE Country_Movies (
        country_movies_id     INT NOT NULL AUTO_INCREMENT,
        movie_id1       INT,
        country_id1     INT,
        FOREIGN KEY(movie_id1) REFERENCES Movies(movie_id),
        FOREIGN KEY(country_id1) REFERENCES Country(country_id), 
        PRIMARY KEY(country_movies_id)
        );
        ''')

    # Creating Director_Movies Table
    cur.execute('''CREATE TABLE Director_Movies (
        director_movies_id     INT NOT NULL AUTO_INCREMENT,
        movie_id1       INT,
        director_id1     INT,
        FOREIGN KEY(movie_id1) REFERENCES Movies(movie_id),
        FOREIGN KEY(director_id1) REFERENCES Director(director_id), 
        PRIMARY KEY(director_movies_id)
        );
        ''')

    # Creating Language_Movies Table
    cur.execute('''CREATE TABLE Language_Movies (
        language_movies_id     INT NOT NULL AUTO_INCREMENT,
        movie_id1       INT,
        language_id1     INT,
        FOREIGN KEY(movie_id1) REFERENCES Movies(movie_id),
        FOREIGN KEY(language_id1) REFERENCES Language(language_id), 
        PRIMARY KEY(language_movies_id)
        );
        ''')


def insert_data(cur):
    cur.execute('USE movies_db')

    # grabbing all the data from a csv downloaded from kaggle
    with open('IMDb_movies_1.1.csv', 'r', encoding='utf8') as csv_database:
        reader = csv.reader(csv_database)
        header = next(reader)
        cnt = 0
        if header != None:
            for row in reader:

                # Movies Table
                movie_title = row[1]
                duration = row[6]
                release_year = row[2]

                # Country Table
                country_name1 = row[7]
                country_name2 = row[8]
                country_name3 = row[9]

                # Director Table
                director_name1 = row[13]
                director_name2 = row[14]
                director_name3 = row[15]
                director_name4 = row[16]

                # Genre Table
                genre_name1 = row[3]
                genre_name2 = row[4]
                genre_name3 = row[5]

                # Language Table
                language_name1 = row[10]
                language_name2 = row[11]
                language_name3 = row[12]

                # Insert for Movies Table; Movies(movie_id, movie_title, duration, release_year)
                cur.execute('''INSERT INTO Movies (movie_title, duration, release_year)
                     VALUES ( %s, %s, %s )''', (movie_title, int(duration), int(release_year)))

                # Insert for Country Table
                # accounts for and only grabs unique values from the three columns of country names
                cur.execute('''INSERT IGNORE INTO Country (country_name)
                     VALUES ( %s)''', country_name1)
                cur.execute('''INSERT IGNORE INTO Country (country_name)
                     VALUES ( %s)''', country_name2)
                cur.execute('''INSERT IGNORE INTO Country (country_name)
                     VALUES ( %s)''', country_name3)

                # Insert for Director Table
                # accounts for and only grabs unique values from the four columns of director names
                cur.execute('''INSERT IGNORE INTO Director (director_name)
                     VALUES ( %s)''', director_name1)
                cur.execute('''INSERT IGNORE INTO Director (director_name)
                     VALUES ( %s)''', director_name2)
                cur.execute('''INSERT IGNORE INTO Director (director_name)
                     VALUES ( %s)''', director_name3)
                cur.execute('''INSERT IGNORE INTO Director (director_name)
                     VALUES ( %s)''', director_name4)

                # Insert for Genre Table
                # accounts for and only grabs unique values from the four columns of Genre names
                cur.execute('''INSERT IGNORE INTO Genre (genre_name)
                     VALUES ( %s)''', genre_name1)
                cur.execute('''INSERT IGNORE INTO Genre (genre_name)
                     VALUES ( %s)''', genre_name2)
                cur.execute('''INSERT IGNORE INTO Genre (genre_name)
                     VALUES ( %s)''', genre_name3)

                # Insert for Language Table
                # accounts for and only grabs unique values from the four columns of Genre names
                cur.execute('''INSERT IGNORE INTO Language (language_name)
                     VALUES ( %s)''', language_name1)
                cur.execute('''INSERT IGNORE INTO Language (language_name)
                     VALUES ( %s)''', language_name2)
                cur.execute('''INSERT IGNORE INTO Language (language_name)
                     VALUES ( %s)''', language_name3)

                # Gets the genre_id for the corresponding Genre name
                cur.execute('SELECT genre_id FROM Genre WHERE genre_name = %s OR genre_name = %s OR genre_name = %s', (genre_name1, genre_name2, genre_name3))
                genre_id = cur.fetchone()[0]

                # Gets the movie_id for the corresponding Movie title
                cur.execute('SELECT movie_id FROM Movies WHERE movie_title = %s', movie_title)
                movie_id = cur.fetchone()[0]

                # Insert for Genre_Movies Table
                cur.execute('''INSERT INTO Genre_Movies (genre_id1, movie_id1)
                VALUES (%s, %s)''', (genre_id, movie_id))

                # Gets the country_id for the corresponding country name
                cur.execute('SELECT country_id FROM Country WHERE country_name = %s OR country_name = %s OR '
                            'country_name = %s', (country_name1, country_name2, country_name3))
                country_id = cur.fetchone()[0]

                # Insert for Country_Movies Table
                cur.execute('''INSERT INTO Country_Movies (movie_id1, country_id1)
                VALUES (%s, %s)''', (movie_id, country_id))

                # Gets the director_id for the corresponding director name
                cur.execute('SELECT director_id FROM Director WHERE director_name = %s OR director_name = %s OR '
                            'director_name = %s OR director_name = %s', (director_name1, director_name2,
                                                                         director_name3, director_name4))
                director_id = cur.fetchone()[0]

                # Insert for Director_Movies Table
                cur.execute('''INSERT INTO Director_Movies (movie_id1, director_id1)
                VALUES (%s, %s)''', (movie_id, director_id))

                # Gets the language_id for the corresponding language name
                cur.execute('SELECT language_id FROM Language WHERE language_name = %s OR language_name = %s OR '
                            'language_name = %s', (language_name1, language_name2, language_name3))
                language_id = cur.fetchone()[0]

                # Insert for Language_Movies Table
                cur.execute('''INSERT INTO Language_Movies (movie_id1, language_id1)
                VALUES (%s, %s)''', (movie_id, language_id))

                cnt+=1
                if(cnt > 50):
                    break


cnx = make_connection()
cur = cnx.cursor()
print("Starting Setup...")
setup_db(cur)
print("Finished Setup.")
print("Starting Insert...")
insert_data(cur)
print("Finished Insert.")
cur.close()
cnx.commit()
cnx.close()
print("FINISHED")