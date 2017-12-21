#! /usr/bin/env python
from enum import Enum
import psycopg2


DBNAME = "news"


class Types(Enum):
    VIEWS = "views"
    ERRORS = "errors"


def connect():
    try:
        db = psycopg2.connect(database=DBNAME)
        c = db.cursor()
        return db, c
    except Exception as e:
        print(e)


def print_tuple_array(arr, units):
    """ Function to print tuple"""
    for(article_name, views) in arr:
        print('{0} : {1} {2}'.format(article_name, views, units.value))
    print('\n')


def popular_articles():
    """ Function to return popular articles"""
    query = '''
        select title, count(*) as views
       from log join articles
       on log.path = concat('/article/', articles.slug)
       group by title
       order by views desc
       limit 3;
        '''
    db, cursor = connect()
    cursor.execute(query)
    articles = cursor.fetchall()
    db.close()
    return articles


def popular_authors():
    """ Function to return popular authors"""
    query = '''
        select authors.name, views from authors, (select articles.author
        as author, count (*) as views from articles join log on log.path =
        concat ('/article/', articles.slug)
        where log.status like '%200%' group by articles.author order
        by views desc)
        as subq where authors.id = author;
    '''
    db, cursor = connect()
    cursor.execute(query)
    authors = cursor.fetchall()
    db.close()
    return authors


def major_errors():
    """ Function to return days where error percentage is over 1"""
    query = '''
    select to_char(date, 'FMMonth FMDD, YYYY'), ((err/total)*100) as ratio
       from (select time::date as date,
                    count(*) as total,
                    sum((status != '200 OK')::int)::float as err
                    from log
                    group by date) as errors
       where ((err/total)*100) > 1;
    '''

    db, cursor = connect()
    cursor.execute(query)
    errors = cursor.fetchall()
    db.close()
    return errors


if __name__ == '__main__':

    print("What are the most popular three articles of all time ?")
    articles = popular_articles()
    print_tuple_array(articles, Types.VIEWS)

    print("Who are the most popular article authors of all time ?")
    authors = popular_authors()
    print_tuple_array(authors, Types.VIEWS)

    print("On which days did more than 1% of requests lead to errors ?")
    errors = major_errors()
    print_tuple_array(errors, Types.ERRORS)
