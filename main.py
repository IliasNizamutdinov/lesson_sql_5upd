import json

import sqlalchemy as sq

from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Shop, Book, Stock, Sale


def main():

    login = 'postgres'
    password = 'postgres'
    name_base = 'lesson_4'
    server = 'localhost'
    port = '5432'

    DSN = f'postgresql://{login}:{password}@{server}:{port}/{name_base}'
    engine = sq.create_engine(DSN)

    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    list = []

    #читаем json файл

    with open('files/tests_data.json', 'r', encoding='utf-8') as file:
        json_data = json.load(file)
    for date in json_data:
        model = date['model']
        fields = date['fields']
        id = date['pk']

        if model == 'publisher':
            list.append(Publisher(name=fields['name'], id=id))
        elif model == 'book':
            list.append(Book(title=fields['title'], id_publisher=fields['id_publisher'], id=id))
        elif model == 'shop':
            list.append(Shop(name=fields['name'], id=id))
        elif model == 'stock':
            list.append(Stock(id_shop=fields['id_shop'], id_book=fields['id_book'], count=fields['count'], id=id))
        elif model == 'sale':
            list.append(Sale(price=fields['price'], date_sale=fields['date_sale'], count=fields['count'], id_stock=fields['id_stock']))

    session.add_all(list)
    session.commit()

    name_publisher = input("Введите имя издателя: ")
    for c in session.query(Publisher).filter(Publisher.name == name_publisher).all():
        print(c)

    print("Магазины издателя: ")
    for d in session.query(Shop).join(Stock, Shop.id == Stock.id_shop).join(Book, Stock.id_book == Book.id).join(
            Publisher, Book.id_publisher == Publisher.id).filter(Publisher.name == name_publisher).all():
        print(d)

    session.close()

if __name__ == '__main__':
    main()

