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
    cur.execute('CREATE DATABASE IF NOT EXISTS movies_db;')
    cur.execute('USE movies_db;')

    # Main tables
    cur.execute('DROP TABLE IF EXISTS Movies;')
    # cur.execute('DROP TABLE IF EXISTS Country;')
    # cur.execute('DROP TABLE IF EXISTS Director;')
    # cur.execute('DROP TABLE IF EXISTS Genre;')
    # cur.execute('DROP TABLE IF EXISTS Language;')
    #
    # # All join tables
    # cur.execute('DROP TABLE IF EXISTS Country_Movies;')
    # cur.execute('DROP TABLE IF EXISTS Director_Movies;')
    # cur.execute('DROP TABLE IF EXISTS Genre_Movies;')
    # cur.execute('DROP TABLE IF EXISTS Language_Movies;')

    # Creating Movies Table
    cur.execute('''CREATE TABLE Movies (
        movie_id    INT NOT NULL AUTO_INCREMENT,
        movie_title     VARCHAR(120) NOT NULL,
        duration    INT,
        release_year    INT,
        PRIMARY KEY(movie_id)
        
        ''')

    # cur.execute('''CREATE TABLE Country (
    #     country_id      INT NOT NULL AUTO_INCREMENT,
    #     country_title   VARCHAR(120) NOT NULL,
    #     PRIMARY KEY(country_id);
    #     ''')
    #
    # cur.execute('''CREATE TABLE Director (
    #     director_id     INT NOT NULL AUTO_INCREMENT,
    #     director_name   VARCHAR(120) NOT NULL,
    #     PRIMARY KEY(director_id);
    #     ''')
    #
    # cur.execute('''CREATE TABLE Genre (
    #     genre_id     INT NOT NULL AUTO_INCREMENT,
    #     genre_name   VARCHAR(120) NOT NULL,
    #     PRIMARY KEY(genre_id);
    #     ''')
    #
    # cur.execute('''CREATE TABLE Language (
    #     language_id     INT NOT NULL AUTO_INCREMENT,
    #     language_name   VARCHAR(120) NOT NULL,
    #     PRIMARY KEY(language_id);
    #     ''')



def insert_data(cur):
    cur.execute('USE movies_db')


    #df = pandas.read_csv('IMDB_movies.csv', low_memory=False)
    # for row in df:
    #     movie_title = df["title"]
    #     duration = df["duration"]
    #     release_year = df["year"]


    # cur.execute('''INSERT INTO Movies (movie_title, duration, release_year)
    #         VALUES ( %s, %s, %s )''', ('your mom', int(duration.head(1)), int(2021)))

#movie_title.head(row + 1
#duration.head(row + 1)
#release_year.head(row + 1)





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









#
#
# fname = 'roster_data.json'
#
# # Data structure as follows:
# #   [
# #   [ "Charley", "si110", 1 ],
# #   [ "Mea", "si110", 0 ],
#
# # open the file and read
# str_data = open(fname).read()
# # load the data in a json object
# json_data = json.loads(str_data)
#
# # json data is loaded in a pyton list
# for entry in json_data:
#     name = entry[0]
#     title = entry[1]
#
#     print(name)
#     print(title)
#
#     # INSERT OR IGNORE satisfies the uniqueness constraint.
#     # the inserted data will be ignored if we try to add duplicates.
#     # works as both insert and update
#     cur.execute('''INSERT IGNORE INTO User (name)
#         VALUES ( %s )''', (name))
#
#     # look up the primary key from inserted data.
#     cur.execute('SELECT id FROM User WHERE name = %s ', (name,))
#     user_id = cur.fetchone()[0]
#
#     # same technique is used to insert the title
#     cur.execute('''INSERT IGNORE INTO Course (title)
#         VALUES ( %s )''', (title,))
#     cur.execute('SELECT id FROM Course WHERE title = %s ', (title,))
#     course_id = cur.fetchone()[0]
#
#     # insert both keys in the many to many connector table.
#     cur.execute('''INSERT IGNORE INTO Member
#         (user_id, course_id) VALUES ( %s, %s )''',
#                 (user_id, course_id))