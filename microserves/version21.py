from flask import Flask, request, jsonify
import pymysql
import datetime
from database import *
from Success import *
from Error import *

app = Flask(__name__)

#словарь статусов
status_dict = {'000000001':2,'000000003':1,'000000002':3}
date_format = "%Y-%m-%d %H:%M:%S"



@app.route('/api', methods=['POST'])
def router():
    data_json = request.json
    try:
        auths = data_json.get('auth',{})
    except:
        return error611()
    if auths['login']!= 'RMR'  or auths['password']!='RMRPASS':
        return error611()
    try:
        method = data_json['method']
    except:
        return error609()

    if method == "OperatingTime.Get":
        try:
            data= data_json.get('data',[])
        except:
            return error609()
    else:
        try:
            data= data_json.get('data',{})
        except:
            return error609()
    #проверка вхождения значание из ключа метод в словарь функций и вызов функции по ключу, если он есть
    if method in methods_dict:
        return methods_dict[method](data)
    return error609()


def accident_create(data):
    try:
        code = data['code']
    except:
        return error601()
    try:
        ts_start = data['ts_start']
    except:
        return error602()
    try:
        ts_end = data['ts_end']
    except:
        return error603()
    try:
        number= data['doc_number']
    except:
        return error605()

    id = sel_EQUIP_code(code)
    if len(id) == 0:
        return error606()
    id = id[0][0]

    num = sel_accid_num(number)
    if len(num) == 0:
        in_accident(id,ts_start,ts_end,number)
        return success701()
    else:
        upd_accident(code,ts_start,ts_end,number)
        return success702()


def accident_delete(data):
    try:
        number= data['doc_number']
    except:
        return error605()

    if len(sel_accid_num(number))==0:
        return error607()
    else:
        del_accident_num(number)
        return success703()


def simple_create(data):
    try:
        code = data['code']
    except:
        return error601()
    try:
        ts_start = data['ts_start']
    except:
        return error602()
    try:
        status_id = data['status']
    except:
        return error604()
    try:
        number= data['doc_number']
    except:
        return error605()

    id = sel_EQUIP_code(code)
    if len(id) ==0:
        return error606()
    id = id[0][0]
       # Получение текущего времени
    current_time = datetime.datetime.now()
    #проверка наличия кода статуса в словаре и получения по ключи id в агг
    if status_id in status_dict:
        status_id = status_dict[status_id]
    else:
        return error608()

    # Вычитание двух дней из текущего времени
    two_days_ago = current_time - datetime.timedelta(days=2)
    date_object = datetime.datetime.strptime(ts_start, date_format)
    # Сравнение времени
    if date_object < two_days_ago:
        return error610()

    del_Dump_48()
    another_id = sel_equip_dump_num(number)

    if len(another_id) == 0:

        #1111
        id = int(id)
        in_dump(id,ts_start,status_id,number)
        Up_In(id)
    else:

        another_id = int(another_id[0][0])
        if another_id == id:
            #222222
            another_id = int(another_id)
            up_dump(ts_start,status_id,number)
            Up_In(another_id)
        else:
            #3333
            another_id = int(another_id)
            up_dump_equip(id, ts_start, status_id, number)
            Up_In(another_id)
            #44444
            Up_In(id)
            return success701()

    return success701()


def simple_delete(data):
    try:
        number= data['doc_number']
    except:
        return error605()

    id = sel_equip_dump_num(number)
    if len(id) == 0:
        return error607()
    else:
        id = id[0][0]
        del_Dump_num(number)
        #55555555555
        Up_In(id)
        return success703()

def OperatingTime_Get(data):
    data_new = []
    data_error = []
    for i in data:
        code = i['code']
        ts_start = i['ts_start']
        id = sel_EQUIP_code(code)
        if len(id) == 0:
            operating = "The code of equipment is missing or incorrect"
            data_error.append({'code': code,'description':operating})
        else:
            id =id[0][0]
            operating = round(sel_oper_date(ts_start,id)/3600000,2)
            if operating == 'timestamp invalid':
                data_error.append({'code': code,'description':operating})
            else:
                data_new.append({'code': code,'value':operating})

    original_url = request.url
    if len(data_error)== 0:
        response= jsonify({'data':data_new})
    else:
        response = jsonify({'data':data_new, 'error':data_error})
    return response

    #словарь функций
methods_dict = {'EquipmentCondition.Create':simple_create,'EquipmentCondition.Delete':simple_delete,'RepairAct.Create':accident_create,'RepairAct.Delete':accident_delete,"OperatingTime.Get":OperatingTime_Get}
if __name__ == '__main__':
  app.run(host='0.0.0.0', port=5000,debug=True)
