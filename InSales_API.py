import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import json
from datetime import date, timedelta, datetime

upd_since = (datetime.now() - timedelta(0)).strftime('%Y-%m-%d')
print(upd_since)

def kresla_kachalki():
    # запрос к API INSALES
    r = requests.get(
        'https://***LOGIN***:***PASS***@***SHOPNAME***.myinsales.ru/admin/orders.json'
        '?updated_since=2022-03-06T00%3A00%3A00+%2B03%3A00&per_page=100&page=1')

    if r.status_code == 200:
        print('Подключение успешно к kresla_kachalki:', r.status_code)
    else:
        print('Что-то пошло не так:', r.status_code)

    # ответ от АПИ и создание большого датафрейма для разбора
    data = r.json()
    df_data_all = pd.DataFrame(data)
    with open('data_kresla.json', 'w') as convert_file:
        convert_file.write(json.dumps(r.json(), indent=4))

    # разбор датафрейма на колонки
    # dataframe с колонкой Номер заказа - order_lines / order_id
    order_id = []
    for i in data:
        for k in i['order_lines']:
            order_id.append(k['order_id'])
    df_order_id = pd.DataFrame(order_id, columns=['Номер заказа'])

    # dataframe с колонкой ID заказа - order_lines / id
    id_titel = []
    for i in data:
        for k in i['order_lines']:
            id_titel.append(k['id'])
    df_id_titel = pd.DataFrame(id_titel, columns=['ID заказа'])

    # dataframe с колонкой Дата заказа
    created_at = []
    for i in data:
        for k in i['order_lines']:
            created_at.append(k['created_at'])
    df_created_at = pd.DataFrame(created_at, columns=['Дата заказа'])

    # dataframe с колонкой Аккаунт
    account_id = []
    for i in df_data_all['account_id'].items():
        k = i[1:]
        account_id.append(k)
    df_account_id = pd.DataFrame(account_id, columns=['Аккаунт'])

    # dataframe с колонками ФИО Телефон Емаил
    df_client = pd.DataFrame(columns=['ФИО', 'Телефон', 'Email'])
    for i in range(0, len(data)):
        df_client.loc[i] = [data[i]['client']['name'], data[i]['client']['phone'],
                            data[i]['client']['email']]

    # dataframe с колонкой Наименование - order_lines / title
    title = []
    for i in data:
        for k in i['order_lines']:
            title.append(k['title'])
    df_title = pd.DataFrame(title, columns=['Наименование'])

    # dataframe с колонкой ЦЕНА - full_sale_price
    price = []
    for i in data:
        for k in i['order_lines']:
            price.append(k['full_sale_price'])
    df_price = pd.DataFrame(price, columns=['Цена'])

    # dataframe с колонкой Коммент -
    comment = []
    for i in df_data_all['comment'].items():
        k = i[1:]
        comment.append(k)
    df_comment = pd.DataFrame(comment, columns=['Коммент'])

    # dataframe с колонкой Размеры - order-lines/0/dimensions
    dimensions = []
    for i in data:
        for k in i['order_lines']:
            dimensions.append(k['dimensions'])
    df_dimensions = pd.DataFrame(dimensions, columns=['Размеры'])

    # dataframe с колонкой Кол-во - order-lines/0/quantity
    quantity = []
    for i in data:
        for k in i['order_lines']:
            quantity.append(k['quantity'])
    df_quantity = pd.DataFrame(quantity, columns=['Кол-во'])

    # dataframe с колонкой Способ доставки - delivery_description
    delivery_description = []
    for i in df_data_all['delivery_description'].items():
        k = i[1:]
        delivery_description.append(k)
    df_delivery_description = pd.DataFrame(delivery_description, columns=['Способ доставки'])

    # dataframe с колонкой Доставка - full_delivery_price
    full_delivery_price = []
    for i in df_data_all['full_delivery_price'].items():
        k = i[1:]
        full_delivery_price.append(k)
    df_full_delivery_price = pd.DataFrame(full_delivery_price, columns=['Доставка'])

    # dataframe с колонкой Оплата - payment_title
    payment_title = []
    for i in df_data_all['payment_title'].items():
        k = i[1:]
        payment_title.append(k)
    df_payment_title = pd.DataFrame(payment_title, columns=['Оплата'])

    # dataframe с колонкой Номер № - number
    number = []
    for i in df_data_all['number'].items():
        k = i[1:]
        number.append(k)
    df_number = pd.DataFrame(number, columns=['Номер №'])

    # dataframe с колонкой Каналы -  first_source
    first_source = []
    for i in df_data_all['first_source'].items():
        k = i[1:]
        first_source.append(k)
    df_first_source = pd.DataFrame(first_source, columns=['Каналы'])

    df_shipping_address = pd.DataFrame(columns=['Адрес доставки'])
    for i in range(0, len(data)):
        df_shipping_address.loc[i] = [data[i]['shipping_address']['full_delivery_address']]

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------
    # Обработка датафрема для вставки пустых строк по индексу, для разных позициы в ОДНОМ заказе
    res_indx = {}
    index = 0
    res_ind_h = []
    for x in order_id:
        if x in res_indx:
            res_indx[x].append(index)
        else:
            res_indx[x] = [index]
        index += 1

    # находим ключи из списка, добавляем пустые строки (ячейки) в датафрейм
    # в зависимости от индекса, если у нас в заказе несколько разных позицый
    # получается индексы строк в датафрейме (переменная res_indx[h])
    for h in res_indx.keys():
        if len(res_indx[h]) > 1:
            res_ind_h.append(res_indx[h])
    for r in res_ind_h:
        for z in r[1:]:
            df_client.loc[z + (-0.9), :] = ['', '', '']
            df_client = df_client.sort_index().reset_index(drop=True)
            df_account_id.loc[z + (-0.9), :] = ['']
            df_account_id = df_account_id.sort_index().reset_index(drop=True)
            df_delivery_description.loc[z + (-0.9), :] = ['']
            df_delivery_description = df_delivery_description.sort_index().reset_index(drop=True)
            df_full_delivery_price.loc[z + (-0.9), :] = ['']
            df_full_delivery_price = df_full_delivery_price.sort_index().reset_index(drop=True)
            df_comment.loc[z + (-0.9), :] = ['']
            df_comment = df_comment.sort_index().reset_index(drop=True)
            df_payment_title.loc[z + (-0.9), :] = ['']
            df_payment_title = df_payment_title.sort_index().reset_index(drop=True)
            df_number.loc[z + (-0.9), :] = ['']
            df_number = df_number.sort_index().reset_index(drop=True)
            df_first_source.loc[z + (-0.9), :] = ['']
            df_first_source = df_first_source.sort_index().reset_index(drop=True)
            df_shipping_address.loc[z + (-0.9), :] = ['']
            df_shipping_address = df_shipping_address.sort_index().reset_index(drop=True)

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------

    # ОСНОВНОЙ ДАТАФРЕЙМ!
    df_kresla_kachalki = pd.concat([df_order_id, df_created_at, df_account_id, df_client,
                                    df_delivery_description, df_title,
                                    df_dimensions, df_quantity, df_price,
                                    df_full_delivery_price, df_comment, df_payment_title,
                                    df_number, df_first_source, df_id_titel,df_shipping_address], axis=1)
    return df_kresla_kachalki


def leset_mabel():
    # запрос к API INSALES
    r = requests.get(
        'https://***LOGIN***:***PASS***@***SHOPNAME***.myinsales.ru/admin/orders.json'
        '?updated_since=2022-03-06T00%3A00%3A00+%2B03%3A00&per_page=100&page=1')

    if r.status_code == 200:
        print('Подключение успешно к leset_mabel:', r.status_code)
    else:
        print('Что-то пошло не так:', r.status_code)
    # ответ от АПИ и создание большого датафрейма для разбора
    data = r.json()
    df_data_all = pd.DataFrame(data)
    with open('data_leset.json', 'w') as convert_file:
        convert_file.write(json.dumps(r.json(), indent=4))
    # разбор датафрейма на колонки
    # dataframe с колонкой Номер заказа - order_lines / order_id
    order_id = []
    for i in data:
        for k in i['order_lines']:
            order_id.append(k['order_id'])
    df_order_id = pd.DataFrame(order_id, columns=['Номер заказа'])

    # dataframe с колонкой ID заказа - order_lines / id
    id_titel = []
    for i in data:
        for k in i['order_lines']:
            id_titel.append(k['id'])
    df_id_titel = pd.DataFrame(id_titel, columns=['ID заказа'])

    # dataframe с колонкой Дата заказа
    created_at = []
    for i in data:
        for k in i['order_lines']:
            created_at.append(k['created_at'])
    df_created_at = pd.DataFrame(created_at, columns=['Дата заказа'])

    # dataframe с колонкой Аккаунт
    account_id = []
    for i in df_data_all['account_id'].items():
        k = i[1:]
        account_id.append(k)
    df_account_id = pd.DataFrame(account_id, columns=['Аккаунт'])

    # dataframe с колонками ФИО Телефон Емаил
    df_client = pd.DataFrame(columns=['ФИО', 'Телефон', 'Email'])
    for i in range(0, len(data)):
        df_client.loc[i] = [data[i]['client']['name'], data[i]['client']['phone'],
                            data[i]['client']['email']]

    # dataframe с колонкой Наименование - order_lines / title
    title = []
    for i in data:
        for k in i['order_lines']:
            title.append(k['title'])
    df_title = pd.DataFrame(title, columns=['Наименование'])

    # dataframe с колонкой ЦЕНА - full_sale_price
    price = []
    for i in data:
        for k in i['order_lines']:
            price.append(k['full_sale_price'])
    df_price = pd.DataFrame(price, columns=['Цена'])

    # dataframe с колонкой Коммент -
    comment = []
    for i in df_data_all['comment'].items():
        k = i[1:]
        comment.append(k)
    df_comment = pd.DataFrame(comment, columns=['Коммент'])

    # dataframe с колонкой Размеры - order-lines/0/dimensions
    dimensions = []
    for i in data:
        for k in i['order_lines']:
            dimensions.append(k['dimensions'])
    df_dimensions = pd.DataFrame(dimensions, columns=['Размеры'])

    # dataframe с колонкой Кол-во - order-lines/0/quantity
    quantity = []
    for i in data:
        for k in i['order_lines']:
            quantity.append(k['quantity'])
    df_quantity = pd.DataFrame(quantity, columns=['Кол-во'])

    # dataframe с колонкой Способ доставки - delivery_description
    delivery_description = []
    for i in df_data_all['delivery_description'].items():
        k = i[1:]
        delivery_description.append(k)
    df_delivery_description = pd.DataFrame(delivery_description, columns=['Способ доставки'])

    # dataframe с колонкой Доставка - full_delivery_price
    full_delivery_price = []
    for i in df_data_all['full_delivery_price'].items():
        k = i[1:]
        full_delivery_price.append(k)
    df_full_delivery_price = pd.DataFrame(full_delivery_price, columns=['Доставка'])

    # dataframe с колонкой Оплата - payment_title
    payment_title = []
    for i in df_data_all['payment_title'].items():
        k = i[1:]
        payment_title.append(k)
    df_payment_title = pd.DataFrame(payment_title, columns=['Оплата'])

    # dataframe с колонкой Номер № - number
    number = []
    for i in df_data_all['number'].items():
        k = i[1:]
        number.append(k)
    df_number = pd.DataFrame(number, columns=['Номер №'])

    # dataframe с колонкой Каналы -  first_source
    first_source = []
    for i in df_data_all['first_source'].items():
        k = i[1:]
        first_source.append(k)
    df_first_source = pd.DataFrame(first_source, columns=['Каналы'])

    df_shipping_address = pd.DataFrame(columns=['Адрес доставки'])
    for i in range(0, len(data)):
        df_shipping_address.loc[i] = [data[i]['shipping_address']['full_delivery_address']]

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------
    # Обработка датафрема для вставки пустых строк по индексу, для разных позициы в ОДНОМ заказе
    res_indx = {}
    index = 0
    res_ind_h = []
    for x in order_id:
        if x in res_indx:
            res_indx[x].append(index)
        else:
            res_indx[x] = [index]
        index += 1

    # находим ключи из списка, добавляем пустые строки (ячейки) в датафрейм
    # в зависимости от индекса, если у нас в заказе несколько разных позицый
    # получается индексы строк в датафрейме (переменная res_indx[h])
    for h in res_indx.keys():
        if len(res_indx[h]) > 1:
            res_ind_h.append(res_indx[h])
    for r in res_ind_h:
        for z in r[1:]:
            df_client.loc[z + (-0.9), :] = ['', '', '']
            df_client = df_client.sort_index().reset_index(drop=True)
            df_account_id.loc[z + (-0.9), :] = ['']
            df_account_id = df_account_id.sort_index().reset_index(drop=True)
            df_delivery_description.loc[z + (-0.9), :] = ['']
            df_delivery_description = df_delivery_description.sort_index().reset_index(drop=True)
            df_full_delivery_price.loc[z + (-0.9), :] = ['']
            df_full_delivery_price = df_full_delivery_price.sort_index().reset_index(drop=True)
            df_comment.loc[z + (-0.9), :] = ['']
            df_comment = df_comment.sort_index().reset_index(drop=True)
            df_payment_title.loc[z + (-0.9), :] = ['']
            df_payment_title = df_payment_title.sort_index().reset_index(drop=True)
            df_number.loc[z + (-0.9), :] = ['']
            df_number = df_number.sort_index().reset_index(drop=True)
            df_first_source.loc[z + (-0.9), :] = ['']
            df_first_source = df_first_source.sort_index().reset_index(drop=True)
            df_shipping_address.loc[z + (-0.9), :] = ['']
            df_shipping_address = df_shipping_address.sort_index().reset_index(drop=True)

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------

    # ОСНОВНОЙ ДАТАФРЕЙМ!
    df_leset_mabel = pd.concat([df_order_id, df_created_at, df_account_id, df_client,
                                df_delivery_description, df_title,
                                df_dimensions, df_quantity, df_price,
                                df_full_delivery_price, df_comment, df_payment_title,
                                df_number, df_first_source, df_id_titel, df_shipping_address], axis=1)
    return df_leset_mabel


def stol_stul():
    # запрос к API INSALES
    r = requests.get(
        'https://***LOGIN***:***PASS***@***SHOPNAME***.myinsales.ru/admin/orders.json'
        '?updated_since=2022-03-06T00%3A00%3A00+%2B03%3A00&per_page=100&page=1')

    if r.status_code == 200:
        print('Подключение успешно к stol_stul:', r.status_code)
    else:
        print('Что-то пошло не так:', r.status_code)
    # ответ от АПИ и создание большого датафрейма для разбора
    data = r.json()
    df_data_all = pd.DataFrame(data)
    with open('data_stol_stul.json', 'w') as convert_file:
        convert_file.write(json.dumps(r.json(), indent=4))
    # разбор датафрейма на колонки с нужной информацией
    # dataframe с колонкой Номер заказа - order_lines / order_id
    order_id = []
    for i in data:
        for k in i['order_lines']:
            order_id.append(k['order_id'])
    df_order_id = pd.DataFrame(order_id, columns=['Номер заказа'])

    # dataframe с колонкой ID заказа - order_lines / id
    id_titel = []
    for i in data:
        for k in i['order_lines']:
            id_titel.append(k['id'])
    df_id_titel = pd.DataFrame(id_titel, columns=['ID заказа'])

    # dataframe с колонкой Дата заказа
    created_at = []
    for i in data:
        for k in i['order_lines']:
            created_at.append(k['created_at'])
    df_created_at = pd.DataFrame(created_at, columns=['Дата заказа'])

    # dataframe с колонкой Аккаунт
    account_id = []
    for i in df_data_all['account_id'].items():
        k = i[1:]
        account_id.append(k)
    df_account_id = pd.DataFrame(account_id, columns=['Аккаунт'])

    # dataframe с колонками ФИО Телефон Емаил
    df_client = pd.DataFrame(columns=['ФИО', 'Телефон', 'Email'])
    for i in range(0, len(data)):
        df_client.loc[i] = [data[i]['client']['name'], data[i]['client']['phone'],
                            data[i]['client']['email']]

    # dataframe с колонкой Наименование - order_lines / title
    title = []
    for i in data:
        for k in i['order_lines']:
            title.append(k['title'])
    df_title = pd.DataFrame(title, columns=['Наименование'])

    # dataframe с колонкой ЦЕНА - full_sale_price
    price = []
    for i in data:
        for k in i['order_lines']:
            price.append(k['full_sale_price'])
    df_price = pd.DataFrame(price, columns=['Цена'])

    # dataframe с колонкой Коммент -
    comment = []
    for i in df_data_all['comment'].items():
        k = i[1:]
        comment.append(k)
    df_comment = pd.DataFrame(comment, columns=['Коммент'])

    # dataframe с колонкой Размеры - order-lines/0/dimensions
    dimensions = []
    for i in data:
        for k in i['order_lines']:
            dimensions.append(k['dimensions'])
    df_dimensions = pd.DataFrame(dimensions, columns=['Размеры'])

    # dataframe с колонкой Кол-во - order-lines/0/quantity
    quantity = []
    for i in data:
        for k in i['order_lines']:
            quantity.append(k['quantity'])
    df_quantity = pd.DataFrame(quantity, columns=['Кол-во'])

    # dataframe с колонкой Способ доставки - delivery_description
    delivery_description = []
    for i in df_data_all['delivery_description'].items():
        k = i[1:]
        delivery_description.append(k)
    df_delivery_description = pd.DataFrame(delivery_description, columns=['Способ доставки'])

    # dataframe с колонкой Доставка - full_delivery_price
    full_delivery_price = []
    for i in df_data_all['full_delivery_price'].items():
        k = i[1:]
        full_delivery_price.append(k)
    df_full_delivery_price = pd.DataFrame(full_delivery_price, columns=['Доставка'])

    # dataframe с колонкой Оплата - payment_title
    payment_title = []
    for i in df_data_all['payment_title'].items():
        k = i[1:]
        payment_title.append(k)
    df_payment_title = pd.DataFrame(payment_title, columns=['Оплата'])

    # dataframe с колонкой Номер № - number
    number = []
    for i in df_data_all['number'].items():
        k = i[1:]
        number.append(k)
    df_number = pd.DataFrame(number, columns=['Номер №'])

    # dataframe с колонкой Каналы -  first_source
    first_source = []
    for i in df_data_all['first_source'].items():
        k = i[1:]
        first_source.append(k)
    df_first_source = pd.DataFrame(first_source, columns=['Каналы'])

    df_shipping_address = pd.DataFrame(columns=['Адрес доставки'])
    for i in range(0, len(data)):
        df_shipping_address.loc[i] = [data[i]['shipping_address']['full_delivery_address']]

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------
    # Обработка датафрема для вставки пустых строк по индексу, для разных позициы в ОДНОМ заказе
    res_indx = {}
    index = 0
    res_ind_h = []
    for x in order_id:
        if x in res_indx:
            res_indx[x].append(index)
        else:
            res_indx[x] = [index]
        index += 1

    # находим ключи из списка, добавляем пустые строки (ячейки) в датафрейм
    # в зависимости от индекса, если у нас в заказе несколько разных позицый
    # получается индексы строк в датафрейме (переменная res_indx[h])
    for h in res_indx.keys():
        if len(res_indx[h]) > 1:
            res_ind_h.append(res_indx[h])
    for r in res_ind_h:
        for z in r[1:]:
            df_client.loc[z + (-0.9), :] = ['', '', '']
            df_client = df_client.sort_index().reset_index(drop=True)
            df_account_id.loc[z + (-0.9), :] = ['']
            df_account_id = df_account_id.sort_index().reset_index(drop=True)
            df_delivery_description.loc[z + (-0.9), :] = ['']
            df_delivery_description = df_delivery_description.sort_index().reset_index(drop=True)
            df_full_delivery_price.loc[z + (-0.9), :] = ['']
            df_full_delivery_price = df_full_delivery_price.sort_index().reset_index(drop=True)
            df_comment.loc[z + (-0.9), :] = ['']
            df_comment = df_comment.sort_index().reset_index(drop=True)
            df_payment_title.loc[z + (-0.9), :] = ['']
            df_payment_title = df_payment_title.sort_index().reset_index(drop=True)
            df_number.loc[z + (-0.9), :] = ['']
            df_number = df_number.sort_index().reset_index(drop=True)
            df_first_source.loc[z + (-0.9), :] = ['']
            df_first_source = df_first_source.sort_index().reset_index(drop=True)
            df_shipping_address.loc[z + (-0.9), :] = ['']
            df_shipping_address = df_shipping_address.sort_index().reset_index(drop=True)

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------

    # ОСНОВНОЙ ДАТАФРЕЙМ!
    df_stol_stul = pd.concat([df_order_id, df_created_at, df_account_id, df_client,
                              df_delivery_description, df_title,
                              df_dimensions, df_quantity, df_price,
                              df_full_delivery_price, df_comment, df_payment_title,
                              df_number, df_first_source, df_id_titel, df_shipping_address], axis=1)
    return df_stol_stul


def visan():
    # запрос к API INSALES
    r = requests.get(
        'https://***LOGIN***:***PASS***@***SHOPNAME***.myinsales.ru/admin/orders.json'
        '?updated_since=2022-03-06T00%3A00%3A00+%2B03%3A00&per_page=100&page=1')

    if r.status_code == 200:
        print('Подключение успешно к visan:', r.status_code)
    else:
        print('Что-то пошло не так:', r.status_code)
    # ответ от АПИ и создание большого датафрейма для разбора
    data = r.json()
    df_data_all = pd.DataFrame(data)
    with open('data_visan.json', 'w') as convert_file:
        convert_file.write(json.dumps(r.json(), indent=4))
    # разбор датафрейма на колонки с нужной информацией
    # dataframe с колонкой Номер заказа - order_lines / order_id
    order_id = []
    for i in data:
        for k in i['order_lines']:
            order_id.append(k['order_id'])
    df_order_id = pd.DataFrame(order_id, columns=['Номер заказа'])

    # dataframe с колонкой ID заказа - order_lines / id
    id_titel = []
    for i in data:
        for k in i['order_lines']:
            id_titel.append(k['id'])
    df_id_titel = pd.DataFrame(id_titel, columns=['ID заказа'])

    # dataframe с колонкой Дата заказа
    created_at = []
    for i in data:
        for k in i['order_lines']:
            created_at.append(k['created_at'])
    df_created_at = pd.DataFrame(created_at, columns=['Дата заказа'])

    # dataframe с колонкой Аккаунт
    account_id = []
    for i in df_data_all['account_id'].items():
        k = i[1:]
        account_id.append(k)
    df_account_id = pd.DataFrame(account_id, columns=['Аккаунт'])

    # dataframe с колонками ФИО Телефон Емаил
    df_client = pd.DataFrame(columns=['ФИО', 'Телефон', 'Email'])
    for i in range(0, len(data)):
        df_client.loc[i] = [data[i]['client']['name'], data[i]['client']['phone'],
                            data[i]['client']['email']]

    # dataframe с колонкой Наименование - order_lines / title
    title = []
    for i in data:
        for k in i['order_lines']:
            title.append(k['title'])
    df_title = pd.DataFrame(title, columns=['Наименование'])

    # dataframe с колонкой ЦЕНА - full_sale_price
    price = []
    for i in data:
        for k in i['order_lines']:
            price.append(k['full_sale_price'])
    df_price = pd.DataFrame(price, columns=['Цена'])

    # dataframe с колонкой Коммент -
    comment = []
    for i in df_data_all['comment'].items():
        k = i[1:]
        comment.append(k)
    df_comment = pd.DataFrame(comment, columns=['Коммент'])

    # dataframe с колонкой Размеры - order-lines/0/dimensions
    dimensions = []
    for i in data:
        for k in i['order_lines']:
            dimensions.append(k['dimensions'])
    df_dimensions = pd.DataFrame(dimensions, columns=['Размеры'])

    # dataframe с колонкой Кол-во - order-lines/0/quantity
    quantity = []
    for i in data:
        for k in i['order_lines']:
            quantity.append(k['quantity'])
    df_quantity = pd.DataFrame(quantity, columns=['Кол-во'])

    # dataframe с колонкой Способ доставки - delivery_description
    delivery_description = []
    for i in df_data_all['delivery_description'].items():
        k = i[1:]
        delivery_description.append(k)
    df_delivery_description = pd.DataFrame(delivery_description, columns=['Способ доставки'])

    # dataframe с колонкой Доставка - full_delivery_price
    full_delivery_price = []
    for i in df_data_all['full_delivery_price'].items():
        k = i[1:]
        full_delivery_price.append(k)
    df_full_delivery_price = pd.DataFrame(full_delivery_price, columns=['Доставка'])

    # dataframe с колонкой Оплата - payment_title
    payment_title = []
    for i in df_data_all['payment_title'].items():
        k = i[1:]
        payment_title.append(k)
    df_payment_title = pd.DataFrame(payment_title, columns=['Оплата'])

    # dataframe с колонкой Номер № - number
    number = []
    for i in df_data_all['number'].items():
        k = i[1:]
        number.append(k)
    df_number = pd.DataFrame(number, columns=['Номер №'])

    # dataframe с колонкой Каналы -  first_source
    first_source = []
    for i in df_data_all['first_source'].items():
        k = i[1:]
        first_source.append(k)
    df_first_source = pd.DataFrame(first_source, columns=['Каналы'])

    df_shipping_address = pd.DataFrame(columns=['Адрес доставки'])
    for i in range(0, len(data)):
        df_shipping_address.loc[i] = [data[i]['shipping_address']['full_delivery_address']]

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------
    # Обработка датафрема для вставки пустых строк по индексу, для разных позициы в ОДНОМ заказе
    res_indx = {}
    index = 0
    res_ind_h = []
    for x in order_id:
        if x in res_indx:
            res_indx[x].append(index)
        else:
            res_indx[x] = [index]
        index += 1

    # находим ключи из списка, добавляем пустые строки (ячейки) в датафрейм
    # в зависимости от индекса, если у нас в заказе несколько разных позицый
    # получается индексы строк в датафрейме (переменная res_indx[h])
    for h in res_indx.keys():
        if len(res_indx[h]) > 1:
            res_ind_h.append(res_indx[h])
    for r in res_ind_h:
        for z in r[1:]:
            df_client.loc[z + (-0.9), :] = ['', '', '']
            df_client = df_client.sort_index().reset_index(drop=True)
            df_account_id.loc[z + (-0.9), :] = ['']
            df_account_id = df_account_id.sort_index().reset_index(drop=True)
            df_delivery_description.loc[z + (-0.9), :] = ['']
            df_delivery_description = df_delivery_description.sort_index().reset_index(drop=True)
            df_full_delivery_price.loc[z + (-0.9), :] = ['']
            df_full_delivery_price = df_full_delivery_price.sort_index().reset_index(drop=True)
            df_comment.loc[z + (-0.9), :] = ['']
            df_comment = df_comment.sort_index().reset_index(drop=True)
            df_payment_title.loc[z + (-0.9), :] = ['']
            df_payment_title = df_payment_title.sort_index().reset_index(drop=True)
            df_number.loc[z + (-0.9), :] = ['']
            df_number = df_number.sort_index().reset_index(drop=True)
            df_first_source.loc[z + (-0.9), :] = ['']
            df_first_source = df_first_source.sort_index().reset_index(drop=True)
            df_shipping_address.loc[z + (-0.9), :] = ['']
            df_shipping_address = df_shipping_address.sort_index().reset_index(drop=True)

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------

    # ОСНОВНОЙ ДАТАФРЕЙМ!
    df_visan = pd.concat([df_order_id, df_created_at, df_account_id, df_client,
                          df_delivery_description, df_title,
                          df_dimensions, df_quantity, df_price,
                          df_full_delivery_price, df_comment, df_payment_title,
                          df_number, df_first_source, df_id_titel, df_shipping_address], axis=1)
    return df_visan


def papasan():
    # запрос к API INSALES
    r = requests.get(
        'https://***LOGIN***:***PASS***@***SHOPNAME***.myinsales.ru/admin/orders.json'
        '?updated_since=2022-03-06T00%3A00%3A00+%2B03%3A00&per_page=100&page=1')

    if r.status_code == 200:
        print('Подключение успешно к papasan:', r.status_code)
    else:
        print('Что-то пошло не так:', r.status_code)
    # ответ от АПИ и создание большого датафрейма для разбора
    data = r.json()
    df_data_all = pd.DataFrame(data)
    with open('data_papasan.json', 'w') as convert_file:
        convert_file.write(json.dumps(r.json(), indent=4))
    # разбор датафрейма на колонки с нужной информацией
    # dataframe с колонкой Номер заказа - order_lines / order_id
    order_id = []
    for i in data:
        for k in i['order_lines']:
            order_id.append(k['order_id'])
    df_order_id = pd.DataFrame(order_id, columns=['Номер заказа'])

    # dataframe с колонкой ID заказа - order_lines / id
    id_titel = []
    for i in data:
        for k in i['order_lines']:
            id_titel.append(k['id'])
    df_id_titel = pd.DataFrame(id_titel, columns=['ID заказа'])

    # dataframe с колонкой Дата заказа
    created_at = []
    for i in data:
        for k in i['order_lines']:
            created_at.append(k['created_at'])
    df_created_at = pd.DataFrame(created_at, columns=['Дата заказа'])

    # dataframe с колонкой Аккаунт
    account_id = []
    for i in df_data_all['account_id'].items():
        k = i[1:]
        account_id.append(k)
    df_account_id = pd.DataFrame(account_id, columns=['Аккаунт'])

    # dataframe с колонками ФИО Телефон Емаил
    df_client = pd.DataFrame(columns=['ФИО', 'Телефон', 'Email'])
    for i in range(0, len(data)):
        df_client.loc[i] = [data[i]['client']['name'], data[i]['client']['phone'],
                            data[i]['client']['email']]

    # dataframe с колонкой Наименование - order_lines / title
    title = []
    for i in data:
        for k in i['order_lines']:
            title.append(k['title'])
    df_title = pd.DataFrame(title, columns=['Наименование'])

    # dataframe с колонкой ЦЕНА - full_sale_price
    price = []
    for i in data:
        for k in i['order_lines']:
            price.append(k['full_sale_price'])
    df_price = pd.DataFrame(price, columns=['Цена'])

    # dataframe с колонкой Коммент -
    comment = []
    for i in df_data_all['comment'].items():
        k = i[1:]
        comment.append(k)
    df_comment = pd.DataFrame(comment, columns=['Коммент'])

    # dataframe с колонкой Размеры - order-lines/0/dimensions
    dimensions = []
    for i in data:
        for k in i['order_lines']:
            dimensions.append(k['dimensions'])
    df_dimensions = pd.DataFrame(dimensions, columns=['Размеры'])

    # dataframe с колонкой Кол-во - order-lines/0/quantity
    quantity = []
    for i in data:
        for k in i['order_lines']:
            quantity.append(k['quantity'])
    df_quantity = pd.DataFrame(quantity, columns=['Кол-во'])

    # dataframe с колонкой Способ доставки - delivery_description
    delivery_description = []
    for i in df_data_all['delivery_description'].items():
        k = i[1:]
        delivery_description.append(k)
    df_delivery_description = pd.DataFrame(delivery_description, columns=['Способ доставки'])

    # dataframe с колонкой Доставка - full_delivery_price
    full_delivery_price = []
    for i in df_data_all['full_delivery_price'].items():
        k = i[1:]
        full_delivery_price.append(k)
    df_full_delivery_price = pd.DataFrame(full_delivery_price, columns=['Доставка'])

    # dataframe с колонкой Оплата - payment_title
    payment_title = []
    for i in df_data_all['payment_title'].items():
        k = i[1:]
        payment_title.append(k)
    df_payment_title = pd.DataFrame(payment_title, columns=['Оплата'])

    # dataframe с колонкой Номер № - number
    number = []
    for i in df_data_all['number'].items():
        k = i[1:]
        number.append(k)
    df_number = pd.DataFrame(number, columns=['Номер №'])

    # dataframe с колонкой Каналы -  first_source
    first_source = []
    for i in df_data_all['first_source'].items():
        k = i[1:]
        first_source.append(k)
    df_first_source = pd.DataFrame(first_source, columns=['Каналы'])

    df_shipping_address = pd.DataFrame(columns=['Адрес доставки'])
    for i in range(0, len(data)):
        df_shipping_address.loc[i] = [data[i]['shipping_address']['full_delivery_address']]

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------
    # Обработка датафрема для вставки пустых строк по индексу, для разных позициы в ОДНОМ заказе
    res_indx = {}
    index = 0
    res_ind_h = []
    for x in order_id:
        if x in res_indx:
            res_indx[x].append(index)
        else:
            res_indx[x] = [index]
        index += 1

    # находим ключи из списка, добавляем пустые строки (ячейки) в датафрейм
    # в зависимости от индекса, если у нас в заказе несколько разных позицый
    # получается индексы строк в датафрейме (переменная res_indx[h])
    for h in res_indx.keys():
        if len(res_indx[h]) > 1:
            res_ind_h.append(res_indx[h])
    for r in res_ind_h:
        for z in r[1:]:
            df_client.loc[z + (-0.9), :] = ['', '', '']
            df_client = df_client.sort_index().reset_index(drop=True)
            df_account_id.loc[z + (-0.9), :] = ['']
            df_account_id = df_account_id.sort_index().reset_index(drop=True)
            df_delivery_description.loc[z + (-0.9), :] = ['']
            df_delivery_description = df_delivery_description.sort_index().reset_index(drop=True)
            df_full_delivery_price.loc[z + (-0.9), :] = ['']
            df_full_delivery_price = df_full_delivery_price.sort_index().reset_index(drop=True)
            df_comment.loc[z + (-0.9), :] = ['']
            df_comment = df_comment.sort_index().reset_index(drop=True)
            df_payment_title.loc[z + (-0.9), :] = ['']
            df_payment_title = df_payment_title.sort_index().reset_index(drop=True)
            df_number.loc[z + (-0.9), :] = ['']
            df_number = df_number.sort_index().reset_index(drop=True)
            df_first_source.loc[z + (-0.9), :] = ['']
            df_first_source = df_first_source.sort_index().reset_index(drop=True)
            df_shipping_address.loc[z + (-0.9), :] = ['']
            df_shipping_address = df_shipping_address.sort_index().reset_index(drop=True)

    # ------------------!!!!!!!!!!!!!!!---------------------------------------------------------

    # ОСНОВНОЙ ДАТАФРЕЙМ!
    df_papasan = pd.concat([df_order_id, df_created_at, df_account_id, df_client,
                            df_delivery_description, df_title,
                            df_dimensions, df_quantity, df_price,
                            df_full_delivery_price, df_comment, df_payment_title,
                            df_number, df_first_source, df_id_titel, df_shipping_address], axis=1)
    return df_papasan


# Все магазины в одной таблице
def concat_all_site():
    df_all_site = pd.concat([kresla_kachalki(), leset_mabel(), stol_stul(), visan(), papasan()], ignore_index=True)
    df_all_site.dropna(subset=['Номер заказа'], inplace=True)
    final_df_all_site = df_all_site
    return final_df_all_site


papasan_data = pd.ExcelWriter('Papasan_data.xlsx')
papasan().to_excel(papasan_data)
papasan_data.save()

visan_data = pd.ExcelWriter('Visan_data.xlsx')
visan().to_excel(visan_data)
visan_data.save()

stol_stul_data = pd.ExcelWriter('Stol_stul_data.xlsx')
stol_stul().to_excel(stol_stul_data)
stol_stul_data.save()

leset_mabel_data = pd.ExcelWriter('Leset_mabel_data.xlsx')
leset_mabel().to_excel(leset_mabel_data)
leset_mabel_data.save()

kresla_data = pd.ExcelWriter('Kresla_data.xlsx')
kresla_kachalki().to_excel(kresla_data)
kresla_data.save()

all_site_data = pd.ExcelWriter('all_site_data.xlsx')
concat_all_site().to_excel(all_site_data)
all_site_data.save()


# функция для сохранения CSV для Google Drive
def xlsx_to_csv_pd():
    data_xls = pd.read_excel('all_site_data.xlsx')
    data_xls.to_csv('all_site_data_CSV.csv')


# создание GOOGLE SHEETS на google drive с данными из таблицы и
# # обновление нового файла google sheets MEBEL_all_site_data_CSV на основе CSV
def upload_to_google_drive_serv_acc():
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) #client_secret.json берем в настройках сервис аккаунта Google
    client = gspread.authorize(credentials)

    spreadsheet = client.open('all_site_data_CSV')

    with open(r'all_site_data_CSV.csv', encoding='latin1') as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet.id, data=content)


# Выполнение функций записи CVS и загрузки его в Google Drive, также записи и
# обновления нового файла google sheets MEBEL_all_site_data_CSV на основе CSV
xlsx_to_csv_pd()
# upload_to_google_drive()
upload_to_google_drive_serv_acc()
print('CSV УСПЕШНО ЗАПИСАН В ГУГЛ ТАБЛИЦЫ!!!')
