from flask import Flask, request, jsonify
import pymysql
import datetime

app = Flask(__name__)

#хуйня из под коня
connection = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root_rmr1',
        password='rmrpass!1',
        database='RMR')
cursor = connection.cursor()


@app.route('/api', methods=['POST'])
def router():
    data_json = request.json
    try:
        auths = data_json.get('auth',{})
    except:
        return jsonify({"error":"Invalid authentication"}),611
    try:
        method = data_json['method']
    except:
        return jsonify({"error": "Invalid request body was passed1"}), 609
    try:
        data= data_json.get('data',{})
    except:
        return jsonify({"error": "Invalid request body was passed2"}), 609
    if auths['login']!= 'RMR'  or auths['password']!='RMRPASS':
        return jsonify({"error":"Invalid authentication"}),611

    if method == 'EquipmentCondition.Create' :

        return simple_create(data)
    elif method == 'EquipmentCondition.Delete':
        return simple_delete(data)
    elif method == 'RepairAct.Create':
        return accident_create(data)
    elif method == accident_delete(data):
        return accident_delete(data)
    else:
        return jsonify({"error": "Invalid request body was passed"}), 609


def accident_create(data):
    try:
        code = data['code']
    except:
        return jsonify({"error": "There is no hardware code"}), 601
    try:
        ts_start = data['ts_start']
    except:
        return jsonify({"error": "There is no hardware ts_start"}), 602
    try:
        ts_end = data['ts_end']
    except:
        return jsonify({"error": "There is no hardware ts_end"}), 603
    try:
        status_id = data['status']
    except:
        return jsonify({"error": "There is no hardware state"}), 604
    try:
        number= data['doc_number']
    except:
        return jsonify({"error": "There is no hardware doc_number"}), 605

    cursor.execute('''select id from EQUIP where code = %s''',[code])
    id = cursor.fetchall()
    if len(id) == 0:
        return jsonify({"error": "There is no hardware with this code"}), 606
    id = id[0][0]
    cursor.execute('select * from accident_table  where doc_number = %s',[number])
    num = cursor.fetchall()
    if len(num) == 0:
        cursor.execute('''insert into accident_table  values(%s,%s,%s,%s,%s)''', [id,ts_start,ts_end,status_id,number])
        connection.commit()
        return  jsonify({"success": "An entry has been added"}), 701
    else:
        cursor.execute('''update accident_table  set id= %s,ts_start =%s,ts_end=%s,status_id = %s where doc_number = %s''',[code,ts_start,ts_end,status_id,number])
        connection.commit()
        return jsonify({"success": "An entry has been updated"}), 702


def accident_delete(data):
    try:
        number= data['doc_number']
    except:
        return jsonify({"error": "There is no hardware doc_number"}), 605
    cursor.execute('''select * from accident_table  where doc_number = %s''',[number])


    if len(cursor.fetchall())==0:
        return jsonify({"error": "there is no document with this number"}), 607
    else:
        cursor.execute('''delete from accident_table where doc_number = %s''',[number])
        connection.commit()
        return jsonify({"success": "the record has been deleted"}), 701



def simple_create(data):
    try:
        code = data['code']
    except:
        return jsonify({"error": "There is no hardware code"}), 601
    try:
        ts_start = data['ts_start']
    except:
        return jsonify({"error": "There is no hardware ts_start"}), 602
    try:
        status_id = data['status']
    except:
        return jsonify({"error": "There is no hardware state"}), 604
    try:
        number= data['doc_number']
    except:
        return jsonify({"error": "There is no hardware doc_number"}), 605

    try:
        cursor.execute('''select id from EQUIP where code = %s''',[code])
        id = cursor.fetchall()
        if len(id) ==0:
            return jsonify({"error": "There is no hardware with this code"}), 606
    except:
        return jsonify({"error": "There is no hardware with this code"}), 606
    id = id[0][0]
       # Получение текущего времени
    current_time = datetime.datetime.now()
    if status_id == '000000001':
        status_id = 2
    elif status_id == '000000003':
        status_id = 1
    elif status_id == "000000002":
        status_id = 3
    else:
        return jsonify({"error":"There is no hardware state2"}),604

    # Вычитание двух дней из текущего времени
    two_days_ago = current_time - datetime.timedelta(days=2)
    date_format = "%Y-%m-%d %H:%M:%S"
    date_object = datetime.datetime.strptime(ts_start, date_format)
    # Сравнение времени
    if date_object < two_days_ago:
        return jsonify({"error": "The recording time is too old"}), 610


    cursor.execute('''DELETE FROM The_dump_of_requests
        WHERE start_time < NOW() - INTERVAL 48 HOUR''')
    cursor.execute('''select equipment_id from The_dump_of_requests where basis_document_number = %s ''',[number])
    another_id = cursor.fetchall()

    if len(another_id) == 0:
        id = int(id)

        cursor.execute('''INSERT INTO The_dump_of_requests (equipment_id, start_time, state_id, basis_document_number) VALUES
        (%s, %s, %s, %s)''', [id,ts_start,status_id,number])
        cursor.execute('''DELETE FROM simple%s
        WHERE start_time > NOW() - INTERVAL 48 HOUR''',[id])
        cursor.execute('''select * from The_dump_of_requests where equipment_id = %s  ORDER BY start_time ASC''',[id])
        results= cursor.fetchall()
        new_results = []
        a = 1
        for row in results:
            try:
                new_results.append((row[0],row[1],results[a][1],row[2],row[3]))
                a+=1
            except:
                new_results.append((row[0],row[1],None,row[2],row[3]))
        table_name = f"simple{str(id)}"
        cursor.execute('''SELECT MAX(start_time) FROM simple%s ''',[id])
        max = cursor.fetchall()[0][0]
        cursor.execute('''UPDATE simple%s SET end_time = %s WHERE start_time=%s ''',[id,new_results[0][1],max])
        sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
        # return jsonify({"error": f"{str(new_results)} "}), 400
        cursor.executemany(sql, new_results)
    else:
        another_id = int(another_id[0][0])

        if another_id == id:
            another_id = int(another_id)
            cursor.execute('''update The_dump_of_requests set
                        start_time = %s,
                        state_id =%s
                        where basis_document_number = %s''',[ts_start,status_id,number])
            cursor.execute('''DELETE FROM simple%s
        WHERE start_time > NOW() - INTERVAL 48 HOUR''',[another_id])
            cursor.execute('''select * from The_dump_of_requests where equipment_id = %s ORDER BY start_time ASC ''',[another_id])
            results= cursor.fetchall()
            new_results = []
            a = 1
            for row in results:
                try:
                    new_results.append((row[0],row[1],results[a][1],row[2],row[3]))
                    a+=1
                except:
                    new_results.append((row[0],row[1],None,row[2],row[3]))

            table_name = f"simple{str(another_id)}"
            cursor.execute('''SELECT MAX(start_time) from simple%s''',[id])
            max = cursor.fetchall()[0][0]
            cursor.execute('''UPDATE simple%s SET end_time = %s WHERE start_time=%s ''',[id,new_results[0][1],max])
            sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
            cursor.executemany(sql, new_results)

    # # # # Подтверждаем изменения
    # connection.commit()

        else:
            another_id = int(another_id)
            cursor.execute('''UPDATE The_dump_of_requests
                  SET equipment_id = %s,
                      start_time = %s,
                      state_id = %s
                  WHERE basis_document_number = %s''',
                  [id, ts_start, status_id, number])
            connection.commit()


            cursor.execute('''DELETE FROM simple%s
        WHERE start_time > NOW() - INTERVAL 48 HOUR''',[another_id])
            connection.commit()
            cursor.execute('''select * from The_dump_of_requests where equipment_id = %s ORDER BY start_time ASC''',[another_id])
            results= cursor.fetchall()
            new_results = []
            a = 1
            for row in results:
                try:
                    new_results.append((row[0],row[1],results[a][1],row[2],row[3]))
                    a+=1
                except:
                    new_results.append((row[0],row[1],None,row[2],row[3]))
            table_name = f"simple{str(another_id)}"
            cursor.execute('''SELECT MAX(start_time) FROM simple%s ''',[another_id])
            max = cursor.fetchall()[0][0]
            try:

                cursor.execute('''UPDATE simple%s SET end_time = %s WHERE start_time=%s ''',[another_id,new_results[0][1],max])
            except:
                cursor.execute('''UPDATE simple%s SET end_time = %s WHERE start_time=%s ''',[another_id,None,max])
            sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
            cursor.executemany(sql, new_results)
            connection.commit()



            id = int(id)
            cursor.execute('''DELETE FROM simple%s
        WHERE start_time > NOW() - INTERVAL 48 HOUR''',[id])
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
            cursor.execute('''SELECT MAX(start_time) FROM simple%s ''',[id])
            max = cursor.fetchall()[0][0]
            cursor.execute('''UPDATE simple%s SET end_time = %s WHERE start_time=%s ''',[id,new_results[0][1],max])
            table_name = f"simple{str(id)}"
            sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
            cursor.executemany(sql, new_results)
            connection.commit()
            return jsonify({"success": "An entry has been added"}), 701
    connection.commit()
    return jsonify({"success": "An entry has been added"}), 701


def simple_delete(data):
    try:
        number= data['doc_number']
    except:
        return jsonify({"error": "There is no hardware doc_number"}), 605
    cursor.execute('''select equipment_id from The_dump_of_requests where basis_document_number = %s ''',[number])
    id = cursor.fetchall()
    if len(id) == 0:
        return jsonify({"error":  "There is no document with this number"}), 607
    else:
        id = id[0][0]
        cursor.execute('''DELETE FROM The_dump_of_requests
        where basis_document_number = %s ''',[number])
        cursor.execute('''DELETE FROM simple%s
        WHERE start_time > NOW() - INTERVAL 48 HOUR''',[id])
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
        cursor.execute('''SELECT MAX(start_time) FROM simple%s ''',[id])
        max = cursor.fetchall()[0][0]
        cursor.execute('''UPDATE simple%s SET end_time = %s WHERE start_time=%s ''',[id,new_results[0][1],max])
        table_name = f"simple{str(id)}"
        sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
        cursor.executemany(sql, new_results)
        connection.commit()
        return jsonify({"success": "the record has been deleted"}), 703






if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000,debug=True)
