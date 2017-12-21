from enum import Enum
import psycopg2


DBNAME = "news"


class Types(Enum):
    VIEWS = "views"
    ERRORS = "errors"

def print_tuple_array(arr, units):
    """ Function to print tuple"""
    for(article_name, views) in arr:
        print('{0} : {1} {2}'.format(article_name, views, units.value))
    print('\n')


def popular_articles():
    """ Function to return popular articles"""
    query = '''
        select articles.title , count(*) as views from articles join log
        on log.path like concat ('%',articles.slug,'%') where log.status
        like '%200%' group by articles.title order by views desc limit 3;
        '''
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    articles = c.fetchall()
    db.close()
    return articles


def popular_authors():
    """ Function to return popular authors"""
    query = '''
        select authors.name, views from authors, (select articles.author
        as author, count (*) as views from articles join log on log.path
        like concat ('%',  articles.slug, '%')
        where log.status like '%200%' group by articles.author order
        by views desc)
        as subq where authors.id = author;
    '''
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    authors = c.fetchall()
    db.close()
    return authors


def major_errors():
    """ Function to return days where error percentage is over 1"""
    query = '''
    select day, percentage from (
    select day, round((sum(requests)/(select count(*) from log where
    substring(cast(log.time as text), 0, 11) = day) * 100), 2) as
    percentage from (select substring(cast(log.time as text), 0, 11) as day,
    count(*) as requests from log where status like '%404%' group by day)
    as logpercentage group by day order by percentage desc) as subq
    where percentage >= 1
    '''

    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    errors = c.fetchall()
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
