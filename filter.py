import pymongo

client = pymongo.MongoClient(host="117.119.77.139",
                    port=30019,
                    username="gong",
                    password="tipsikeg2012",
                    authSource="ysu",
                    authMechanism="SCRAM-SHA-1",
                    )
# self.mongo_client = pymongo.MongoClient('mongodb://localhost:27017')
publisher_journal_db = client['ysu']
collection = publisher_journal_db['ams_alldata']


for i, item in enumerate(collection.find().batch_size(2)):
    print(i)
    if 'home.html' in item['url'][0]:
        print(item['url'][0])