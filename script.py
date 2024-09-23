from flask import Flask, request, jsonify
import pymysql
app = Flask(__name__)


connection = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='MySAdmin',
        password='AdM_111155',
        database='mydatabase')
cursor = connection.cursor()


@app.route('/api', methods=['POST'])
def router():
    try:
        data = request.json
        method = data['method']
        type = data['type']
    except:
        return jsonify({"error": "Invalid request body was passed3"}), 400
    
    if type == 'condition':
        if method == 'create':
            return simple_create(data)
        elif method == 'delete':
            return simple_delete(data)
    elif type == 'accident':
        if method == 'create':
            return accident_create(data)
        elif method == 'delete':
            return accident_delete(data)


    return jsonify({"error":  "Invalid request body was passed"}), 400


def accident_create(data):
    try:
        code = data['code']
        ts_start = data['ts_start']
        ts_end = data['ts_end']
        status_id = data['status_id']
        number= data['doc_number']
    except:
        return jsonify({"error": "Invalid request body was passed4"}), 400
    cursor.execute('''select id from equipment4 where code = %s''',[code]) 
    id = cursor.fetchall()
    if len(id) == 0:
        return jsonify({"error": "Not this record"}), 400
    id = id[0][0]
    cursor.execute('select * from accident_table  where doc_number = %s',[number])
    num = cursor.fetchall()
    if len(num) == 0:
        cursor.execute('''insert into accident_table  values(%s,%s,%s,%s,%s)''', [id,ts_start,ts_end,status_id,number])
        connection.commit()
        return  jsonify({"success": "An entry has been added"}), 200
    else:
        cursor.execute('''update accident_table  set id= %s,ts_start =%s,ts_end=%s,status_id = %s where doc_number = %s''',[code,ts_start,ts_end,status_id,number])
        connection.commit()
        return jsonify({"success": "An entry has been updated"}), 200
    

def accident_delete(data):
    try:
        number= data['doc_number']
    except:
        return jsonify({"error": "Invalid request body was passed"}), 400
    cursor.execute('''select * from accident_table  where doc_number = %s''',[number])

    
    if len(cursor.fetchall())==0:
        return jsonify({"error": "Not this record"}), 400
    else:
        cursor.execute('''delete from accident_table where doc_number = %s''',[number])
        connection.commit()
        return jsonify({"success": "the record has been deleted"}), 200



def simple_create(data):
    try:    
        code = data['code']
        ts_start = data['ts_start']
        status_id = data['status_id']
        number= data['doc_number']
    except:
        return jsonify({"error":  "Invalid request body was passed2"}), 400
    
    try:
        cursor.execute('''select id from equipment4 where code = %s''',[code]) 
        id = cursor.fetchall()[0][0]
        if len(id) ==0:
            return jsonify({"error": "There is no hardware with this code"}), 400
    except:
        return jsonify({"error": "There is no hardware with this code"}), 400
    

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
        cursor.execute('''UPDATE simple%s SET end_time = %s WHERE end_time IS NULL ''',[id,new_results[0][1]])
        sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
        # return jsonify({"error": f"{str(new_results)} "}), 400
        cursor.executemany(sql, new_results)
    else:
        another_id = int(another_id[0][0])
        
        if another_id == id:
            another_id = int(another_id)
            cursor.execute('''update The_dump_of_requests set 
                        start_time = %s,
                        state_id VALUES=%s 
                        where basis_document_number = %s''',[ ts_start,status_id,number])
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
            cursor.execute('''UPDATE simple%s SET end_time = %s WHERE end_time IS NULL ''',[another_id,new_results[0][1]])
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
            cursor.execute('''UPDATE simple%s SET end_time = %s WHERE end_time IS NULL ''',[another_id,new_results[0][1]])
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
            cursor.execute('''UPDATE simple%s SET end_time = %s WHERE end_time IS NULL ''',[id,new_results[0][1]])
            table_name = f"simple{str(id)}"
            sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
            cursor.executemany(sql, new_results)
            connection.commit()
            return jsonify({"success": "An entry has been added"}), 200
    connection.commit()
    return jsonify({"success": "An entry has been added"}), 200


def simple_delete(data):
    try:    
        number= data['doc_number']
    except:
        return jsonify({"error": "Invalid request body was passed1"}), 400
    cursor.execute('''select equipment_id from The_dump_of_requests where basis_document_number = %s ''',[number])
    id = cursor.fetchall()
    if len(id) == 0:
        return jsonify({"error":  "There is no document with this number"}), 400
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
        cursor.execute('''UPDATE simple%s SET end_time = %s WHERE end_time IS NULL ''',[id,new_results[0][1]])
        table_name = f"simple{str(id)}"
        sql = f"INSERT INTO {table_name} (equipment_id, start_time ,end_time, state_id, basis_document_number) VALUES (%s, %s, %s,%s, %s)"
        cursor.executemany(sql, new_results)
        connection.commit()
        return jsonify({"success": "the record has been deleted"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000,debug=True)
