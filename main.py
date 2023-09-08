import requests
import json
import time
from datetime import date, timedelta
import pandas as pd
import psycopg2
from decouple import config


postgresql_conn = psycopg2.connect (
    database='postgres',
    user=config("PSQL_USER"),
    password=config("PSQL_PASSWORD"),
    host="localhost",
    port="5432"
)


bicycle_community = 'bicycles.stackexchange.com'
stackexchange_url = 'https://api.stackexchange.com/2.3'
from_date = (date.today() - timedelta(days=1)).strftime('%s')


def main():
    question_list = retrieve_bicycle_questions()
    insert_into_db(question_list)


def insert_into_db(question_list):
    chunked_list = []
    for i in range(0, len(question_list), 1000):
        chunked_list.append(question_list[i:i+1000])
    for chunk in chunked_list:
        try:
            psql_cursor = postgresql_conn.cursor()
            psql_cursor.executemany(f"""INSERT INTO stackoverflow.bicycle_questions(title, body_markdown, link, question_id
                                        creation_date, view_count, answer_count, is_answer)
                                        VALUES (%s, %s)""", chunk)
        except Exception as e:
            print(f'Error: {e}')



def retrieve_bicycle_questions():
    question_list = []
    api_call_wait_time = 1
    page = 1
    more_pages = True
    while more_pages:
        time.sleep(api_call_wait_time)
        r = requests.get(f'{stackexchange_url}/questions?fromdate={from_date}&order=desc&sort=activity&site={bicycle_community}'
                         f'&filter=!6WPIomnJQ*q5r&page={page}')
        if r.status_code != 200:
            print(f'Error: {r.text}')
            raise SystemExit(f'API call returned error code {r.status_code}')
        data_raw = r.content
        data = json.loads(data_raw)
        backoff = data.get('backoff', {})
        print(f'API Backoff Time: {backoff}')
        if backoff == {}:
            api_call_wait_time = 1
        else:
            api_call_wait_time = backoff + 1
        data_items = data.get('items', {})
        for item in data_items:
            title = item['title']
            body_markdown = item['body_markdown']
            link = item['link']
            question_id = item['question_id']
            creation_date = item['creation_date']
            view_count = item['view_count']
            answer_count = item['answer_count']
            is_answered = item['is_answered']
            question =  (title, body_markdown, link, question_id, creation_date, view_count, answer_count, is_answered)
            question_list.append(question)
        more_pages = data.get('has_more', {})
        page += 1
    return question_list


if __name__ == '__main__':
    main()