import os, sys, time, random
from datetime import datetime,timedelta
import pymongo

from pymongo.auth import _parse_scram_response


def gen_name(first_letter=''):
    fullname = {}
    for field in ['surname','firstname','middlename']:
        name = f"{first_letter}"
        for j in range(random.randrange(5, (10, 9)[bool(first_letter)])):
            l = chr(random.randrange(97, 122))
            if not name:
                l = l.capitalize()
            name += l
        fullname[field] = name
    return fullname

def gen_birth(start_date=datetime(1970, 1, 1),end_date=datetime(2000, 1, 1)):
    delta = end_date - start_date
    return start_date + timedelta(days=random.randrange(delta.days))


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('%2.2f ms' % ((te - ts) * 1000))
        return result

    return timed

from pymongo import MongoClient
import re
# import pymongo

# conn = pymongo.Connection('192.168.99.100', 27017)
# db = conn.mydb
# client = MongoClient('192.168.99.100', 27017)
# client.prod_db.authenticate('root', 'example', mechanism='MONGODB-CR')
client = MongoClient('mongodb://root:example@192.168.99.100:27017')

db = client.test_database

# posts = db.posts
# post = {"author": "Mike",
#              "text": "My first blog post!",
#              "tags": ["mongodb", "python", "pymongo"],
#              "date": datetime.datetime.utcnow()}

# post_id = posts.insert_one(post).inserted_id

# print(post_id)

# for post in posts.find({}):
#     print(post)

@timeit
def timed_select():
    records = db.records
    res = records.find(filter=filter)
    for record in res:
        # print(record)
        pass

filter={
    '$and': [
        {
            'name.firstname': {
                '$regex': re.compile(r"^F+")
            }
        }, {
            'name.middlename': {
                '$regex': re.compile(r"^F+")
            }
        }, {
            'name.surname': {
                '$regex': re.compile(r"^F+")
            }
        }
    ]
}
if len(sys.argv)>1:
    if sys.argv[1] == '1':
        records = db.create_collection('records')
    elif sys.argv[1] == '2':
        rec = {'name':sys.argv[2],
                'birthday':sys.argv[3],
                'sex':sys.argv[4]}

        records = db.records
        print(records.insert_one(rec).inserted_id)

    elif sys.argv[1] == '3':
        records = db.records
        res = records.aggregate([
    {'$group': {
            '_id': {
                'name': {
                    'surname': '$name.surname', 
                    'firstname': '$name.firstname', 
                    'middlename': '$name.middlename'
                }, 
                'birthday': '$birthday'
            }, 
            'sex': {
                '$first': '$sex'
            }
        }
    }, {'$project': {
            'age': {
                '$floor': {
                    '$divide': [
                        {
                            '$subtract': [
                                '$$NOW', '$_id.birthday'
                            ]
                        }, 365.25 * 24 * 60 * 60 * 1000
                    ]
                }
            }, 
            'sex': 1
        }
    }, {'$sort': {
            '_id.name': 1
        }
    }, # { '$limit': 10
       # }
],allowDiskUse=True)#
        for record in res:
            print(record)
        # for record in records.find({},{'_id':0}).sort('name',1).allow_disk_use(True):#
        #     print(record)

    elif sys.argv[1] == '4':
        data = []
        for _ in range(1000000):
            data.append({'name':gen_name(),
                        'birthday':gen_birth(),
                        'sex':('female','male')[random.randint(0, 1)]})
        for _ in range(100):
            data.append({'name':gen_name('F'),
                        'birthday':gen_birth(),
                        'sex': 'male'})
        records = db.records
        records.insert_many(data)
    elif sys.argv[1] == '5':
        

        # for record in records.find({'name':{'$regex':'F[a-z]+ F[a-z]+ F[a-z]+'},'sex':1}):
        #     print(record)
        timed_select()
    elif sys.argv[1] == '6':
        [db.records.drop_index('idx') for index in db.records.list_indexes() if index['name'] == 'idx' ]

        timed_select()
        db.records.create_index(
            [( 'name.surname', pymongo.ASCENDING), 
            ('name.firstname',  pymongo.ASCENDING),
            ('name.middlename',  pymongo.ASCENDING),
            ('sex',  pymongo.ASCENDING )           
        ],name="idx")
        print('index created')
        timed_select()

# records = db.records
# data = {'name':'Qwerty Asdfg Zxcvb',
#         'birthday':datetime.datetime(1999,10,12),
#         'sex':1}
# records.insert_one(data)

# for record in records.aggregate([
#     {'$match':{'name':'Qwerty Asdfg Zxcvb'}},
#     {'$set': {"age":'$$NOW' }}

#     ]):
#     print(record)


# print((datetime.datetime.today()-datetime.datetime(2000,10,11)).days/365.25)



"""
1. Создание таблицы с полями представляющими ФИО, дату рождения, пол. 

2. Создание записи. Использовать следующий формат:
myApp 2 ФИО ДатаРождения Пол

3. Вывод всех строк с уникальным значением ФИО+дата, отсортированным по ФИО , вывести ФИО, Дату рождения, пол, кол-во полных лет.
Пример запуска приложения:
myApp 3

4. Заполнение автоматически 1000000 строк. Распределение пола в них должно быть относительно равномерным, начальной буквы ФИО также. Заполнение автоматически  100 строк в которых пол мужской и ФИО начинается с "F".
Пример запуска приложения:
myApp 4

5.  Результат выборки из таблицы по критерию: пол мужской, ФИО  начинается с "F". Сделать замер времени выполнения.
Пример запуска приложения:
myApp 5
Вывод приложения должен содержать время.

6. Произвести определенные манипуляции над базой данных для ускорения запроса из пункта 5. Убедиться, что время исполнения уменьшилось. Объяснить смысл произведенных действий. Предоставить результаты замера до и после.


"""