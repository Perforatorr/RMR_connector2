import requests


def apiAGG(id,antoher_id):
# URL для аутентификации
    url = "http://192.168.0.43:8080/rest/auth"

    # Данные для аутентификации
    payload = {
        "username": "admin",  # Имя пользователя
        "password": "admin"   # Пароль
    }
    # Заголовки для запроса

    headers = {
        "Content-Type": "application/json"  # Указываем тип содержимого
    }
    response = requests.post(url, json=payload, headers=headers)

    # Проверка результата
    if response.status_code == 200:
        print("Аутентификация прошла успешно!")
        # Извлечение токена из ответа
        token = response.json().get("token")
        print("Полученный токен:", token)
    else:
        print("Ошибка аутентификации:", response.status_code)
        print("Текст ошибки:", response.text)

    url ='http://192.168.0.43:8080/rest/v1/contexts/users.admin.models.generalTable/variables/changeDowntime1'
    headers = {
        "Content-Type": "application/json",
        "Authorization":f'Bearer {token}'

    }

    payload = {'id':id}

    response = requests.patch(url, headers=headers, json=payload)#, json=payload, headers=headers)
    if antoher_id != 0:
        payload = {'id':antoher_id}
        response = requests.patch(url, headers=headers, json=payload)
    return ")"
