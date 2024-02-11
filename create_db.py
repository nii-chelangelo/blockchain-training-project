import psycopg2
import pandas as pd

print('Выполняю подключение к базе данных')

conn = psycopg2.connect(
    host="localhost",
    database="cv_base",
    user="postgres",
    password="postgres")

print('Подключение выполнено')

try:
    cv_path = 'C:/Users/Nina/MyProjects/blockchain_training_project/cv.csv'

    with conn:
        with conn.cursor() as curs:
            curs.execute("DROP TABLE IF EXISTS cv, cv_short;")

    print('Создаю и заполняю таблицу cv')

    with conn:
        with conn.cursor() as curs:
            curs.execute("CREATE TABLE cv (id_candidate text, \
                                          id_cv text, \
                                          birthday numeric,\
                                          date_creation text,\
                                          education_type text,\
                                          experience numeric,\
                                          gender text, \
                                          industry_code text, \
                                          position_name text,\
                                          profession_code bigint, \
                                          region_code bigint,\
                                          salary numeric, \
                                          skills text, \
                                          additional_skills text, \
                                          PRIMARY KEY (id_cv));")
    print('Таблица создана')
    with conn:
        with conn.cursor() as curs:
            curs.execute(f"COPY cv(id_candidate,\
                                    id_cv,\
                                    birthday,\
                                    date_creation,\
                                    education_type,\
                                    experience,\
                                    gender,\
                                    industry_code,\
                                    position_name,\
                                    profession_code,\
                                    region_code,\
                                    salary,\
                                    skills,\
                                    additional_skills)\
                            FROM '{cv_path}'\
                            DELIMITER ';'\
                            CSV HEADER;")

    print('Создаю и заполняю таблицу cv_short')

    with conn:
        with conn.cursor() as curs:
            curs.execute("CREATE TABLE cv_short AS SELECT * FROM cv LIMIT 10;")
    print('Таблица создана')

    print('База данных создана. Разрываю соединение')
    conn.close()
    print('Скрипт выполнен успешно')

except:
    conn.close()
    print('Возникла ошибка, разрываю соединение')