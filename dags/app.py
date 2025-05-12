from datetime import datetime, timedelta
from airflow.models.dag import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


headers = {
    "Referer": 'https://books.toscrape.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}


def get_book_data(num_books, ti):
    base_url = "https://books.toscrape.com/catalogue/"

    def get_rating(star_name):
        # Converting the class name to a numerical rating
        rating_dict = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
        return rating_dict.get(star_name, 0)

    def get_book_description(book_url):
        response = requests.get(book_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        description_tag = soup.find("meta", {"name": "description"})
        description = description_tag["content"].strip() if description_tag else "No description"
        return description

    books = []

    for page in range(1, 2):  # Doing 1 page for testing
        url = f"{base_url}page-{page}.html"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        for article in soup.find_all("article", class_="product_pod"):
            title = article.h3.a["title"]
            price = article.find("p", class_="price_color").text
            rating = get_rating(article.find("p", class_="star-rating")["class"][1])
            
            book_url = base_url + article.h3.a['href']
            
            description = get_book_description(book_url)

            books.append({"title": title, "authors":None, "price": price, "rating": rating, "description": description})

    # for book in books[:1]:
    #     print(book)
    #     print(book['title'])
    #     print(book['price'])
    #     print(book['rating'])
    #     print(book['description'])
    #     print(book.keys())

    df = pd.DataFrame(books)
    # print(df)
    
    df.drop_duplicates(subset="title", inplace=True)
    
    ti.xcom_push(key='book_data', value=df.to_dict('records'))


def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, price, rating, description)
    VALUES (%s, %s, %s, %s, %s)
    """
    
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['title'], book['authors'], book['price'], book['rating'], book['description']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_books',
    default_args=default_args,
    description='DAG exercise books.toscrape.com to fetch  data and store it in Postgres',
    # schedule_interval=timedelta(days=1),
    # schedule_interval="@daily",
    catchup=False,
)


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_book_data,
    op_args=[50], # Number of books to scrape / for testing
    dag=dag,
)


create_table_task = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT,
        description TEXT
    );
    """,
    autocommit=True,
    dag=dag,
)


insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)


fetch_book_data_task >> create_table_task >> insert_book_data_task
