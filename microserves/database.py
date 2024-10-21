import datetime
import pymysql

from contextlib import closing

def get_connection():
    """Создаёт и возвращает новое соединение с базой данных."""
    return pymysql.connect(
        host='127.0.0.1',
        port = 3306,    # Замените на имя вашей базы данных
        user='root_rmr1',     # Замените на имя пользователя
        password='rmrpass!1',  # Замените на пароль
        database='RMR'   # Замените на имя базы данных
    )

# connection = pymysql.connect(
#         host='127.0.0.1',
#         port=3306,
#         user='root_rmr1',
#         password='rmrpass!1',
#         database='RMR')
# cursor = connection.cursor()


def delete_con_id(id):
    """Удаляет записи из таблицы condition по id, где start_time больше, чем 48 часов назад."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''DELETE FROM condition%s
            WHERE start_time > NOW() - INTERVAL 48 HOUR''',[id])
            connection.commit()


def Up_In(id):
    """Обновляет данные в таблице condition, основываясь на equipment_id."""
    delete_con_id(id)
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''select * from The_dump_of_requests where equipment_id = %s ORDER BY start_time ASC ''',[id])
            results= cursor.fetchall()
            new_results = []
            a = 1
            for row in results:
                try:
                    new_results.append((row[0],row[1],results[a][1],row[2],row[3]))
                    a+=1
                except:
                    new_results.append((row[0],row[1],None,row[2],row[3]))
            cursor.execute('''SELECT MAX(start_time) FROM condition%s ''',[id])
            max = cursor.fetchall()[0][0]
            try:

                cursor.execute('''UPDATE condition%s SET end_time = %s WHERE start_time=%s ''',[id,new_results[0][1],max])
            except:
                cursor.execute('''UPDATE condition%s SET end_time = %s WHERE start_time=%s ''',[id,None,max])
            table_name = f"condition{str(id)}"
            sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
            cursor.executemany(sql, new_results)
            connection.commit()


def sel_accid_num(number):
    """Выбирает записи из accident_table по номеру документа."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('select * from accident_table  where doc_number = %s',[number])
            return cursor.fetchall()

def del_Dump_48():
    """Удаляет записи из The_dump_of_requests, где start_time старше 48 часов."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''DELETE FROM The_dump_of_requests
                WHERE start_time < NOW() - INTERVAL 48 HOUR''')
            connection.commit()

def del_Dump_num(number):
    """Удаляет записи из The_dump_of_requests по номеру документа."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''DELETE FROM The_dump_of_requests
                where basis_document_number = %s ''',[number])
            connection.commit()

def sel_equip_dump_num(number):
    """Выбирает equipment_id из The_dump_of_requests по номеру документа."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''select equipment_id from The_dump_of_requests where basis_document_number = %s ''',[number])
            return cursor.fetchall()

def sel_EQUIP_code(code):
    """Выбирает id оборудования по его коду."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''select id from EQUIP where code = %s''',[code])
            return cursor.fetchall()

def in_accident(id,ts_start,ts_end,number):
    """Вставляет запись в accident_table."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''insert into accident_table  values(%s,%s,%s,%s,%s)''', [id,ts_start,ts_end,number])
            connection.commit()

def upd_accident(code,ts_start,ts_end,number):
    """Обновляет запись в accident_table."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''update accident_table  set code= %s,ts_start =%s,ts_end=%s where doc_number = %s''',[code,ts_start,ts_end,number])
            connection.commit()

def del_accident_num(number):
    """Удаляет запись из accident_table по номеру документа."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''delete from accident_table where doc_number = %s''',[number])
            connection.commit()

def in_dump(id,ts_start,status_id,number):
    """Обновляет записи в The_dump_of_requests по номеру документа."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''INSERT INTO The_dump_of_requests (equipment_id, start_time, state_id, basis_document_number) VALUES
                (%s, %s, %s, %s)''', [id,ts_start,status_id,number])
            connection.commit()

def up_dump(ts_start,status_id,number):
    """Обновляет записи в The_dump_of_requests по номеру документа и id оборудования."""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''update The_dump_of_requests set
                                start_time = %s,
                                state_id =%s
                                where basis_document_number = %s''',[ts_start,status_id,number])
            connection.commit()

def up_dump_equip(id, ts_start, status_id, number):
    """Обновляет записи в The_dump_of_requests по номеру документа и id оборудования(так же меняет id)"""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute('''UPDATE The_dump_of_requests
                        SET equipment_id = %s,
                            start_time = %s,
                            state_id = %s
                        WHERE basis_document_number = %s''',
                        [id, ts_start, status_id, number])
            connection.commit()

def sel_oper_date(date,id):
    """получает наработку за период указанные период времени по id"""
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            current_time = datetime.datetime.now()
            two_days_ago = current_time - datetime.timedelta(days=2)
            date_format = "%Y-%m-%d %H:%M:%S"
            date_object = datetime.datetime.strptime(date, date_format)
            if date_object >= two_days_ago:
                return 'timestamp invalid'
            cursor.execute('''select * from operating_time%s where ts > %s''',[int(id),date_format])
            operating = cursor.fetchall()
            count=0

            for i in operating:
                count+= i[1]

            return count
