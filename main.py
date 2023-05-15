# -*- coding: cp1251 -*-
import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
import json
from models import create_tables, Publisher, Book, Stock, Shop, Sale
import os

DB_LOGIN = os.getenv("postgres")
DB_PASS = os.getenv("postgres")
DB_NAME = os.getenv("netology_db")

#DSN = f'postgresql://{PG_LOGIN}:{PG_PASS}@localhost:5432/{DB_NAME}' должно работать, но выдаёт ошибку
DSN = f'postgresql://postgres:postgres@localhost:5432/netology_db'
engine = sq.create_engine(DSN)
Session = sessionmaker(bind=engine)
session = Session()

def upload_to_db(session):
    with open('test_base.json') as file:
        data = json.load(file)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]

        session.add(model(id=record.get('pk'), **record.get('fields')))
        session.commit()

if __name__ == '__main__':

    create_tables(engine)
    upload_to_db(session)

    publisher = input('Введите имя издателя: ')

    result = session.query(Publisher.name, Book.title, Shop.name, Sale.price * Sale.count, Sale.date_sale). \
        join(Book).join(Stock).join(Sale).join(Shop)

    if publisher.isdigit():
        result = result.filter(Publisher.id == publisher).all()
    else:
        result = result.filter(Publisher.name == publisher).all()

    if result:
        for row in result:
            print(*row, sep=' | ')
    else:
        print(f'Издатель {publisher} не найден')