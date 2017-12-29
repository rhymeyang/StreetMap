from pymongo import MongoClient
import os
import re

LogDir = os.path.join(os.getcwd(),'log')


ChineseFilter = re.compile('[\u4e00-\u9fff]+')

# if find pingyin, only show there is pinyin, can not get whole pinyin
# eg: Xuéqián Jiē can only get ['é', 'á', 'ē']
# http://www.unicode.org/Public/UCD/latest/charts/CodeCharts.pdf
CodeCharFilter = re.compile('[\u00e0-\u00ff\u0100-\u017f]+')
NumberFilter = re.compile('[\u0030-\u0039]+$')
# unicode 英文字母
UnicodeAlphabetFilter = re.compile('[\u0041-\u005a\u0061-\u007a]+')


OriginDataBase = 'org_osm_db'
OriginCollection = 'org_osm'

FinalDataBase = 'osm_db'
FinalCollection = 'osm_collection'

def get_mongo_final_collection():
    client = MongoClient('localhost:27017')
    db = client[FinalDataBase]
    collection = db[FinalCollection]
    return collection

def get_mongo_org_collection():    
    client = MongoClient('localhost:27017')
    db = client[OriginDataBase]
    collection = db[OriginCollection]
    return collection

mongoCollection = get_mongo_final_collection()

mongoOrgCollection = get_mongo_org_collection()



