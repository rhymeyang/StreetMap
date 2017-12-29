from lib.osmCommon import (
                                mongoCollection,
                                CodeCharFilter,
                                ChineseFilter,
                                UnicodeAlphabetFilter,
                                NumberFilter
                            )
from lib.osmBase import OsmBase
import pprint

import re
import json

class AddressClean(OsmBase):
    """修改后address相关值都在 address下"""
    def __init__(self):
        super(AddressClean, self).__init__("address_clean", 'address_clean.log')
        
        self._city_en_cn_dict = {
                        'shanghai': '上海市',
                        'hangzhou': '杭州市',
                        'kunshan': '昆山市',
                        'ningbo': '宁波市',
                        'qidong': '启东市',
                        'suzhou':'苏州市',
                        'shaoxing': '绍兴市',
                        'huzhou': '湖州市', # 'nanxun' :'南浔',
                        'zhoushan': '舟山市',
                        'nantong': '南通市',
                        'jiading': '嘉定区', 
                        'wuxi' : '无锡市',                
                        'zhenjiang': '镇江市',
                        'fenghua':'奉化市',
                        'jiaxing':'嘉兴市'}

        self._city_cn_list = list(set(self._city_en_cn_dict.values()))
        self._city_en_list = list(self._city_en_cn_dict.keys())

        self._road_en_cn_dict = {
                    'pudong avenue': '浦东大道',
                    'tomson golf garden': '汤臣高尔夫花园',
                    'yuyuan road' : '豫园路',
                    'nanjing road' : '南京路',
                    'middle yincheng road' : '银城中路',
                    'dapu road' : '打浦路',
                    'wensan west rode': '文三西路',
                    'xue yuan road' : '学苑路',
                    'zhongshangnaner road': '中山南二路',
                    'yongfu road': '永福路',
                    'fumin road': '富民路',
                    'wukang road': '武康路',
                    'wukang': '武康路',
                    'zuchongzhi road': '祖冲之路',
                    'garden road': '花园路',
                    'west nanjing road': '南京西路',
                    'hongqiao road': '虹桥路',
                    'chuansha road' : '川沙路',
                    'loushan guan road': '娄山关路'

                    }

        self._district_cn_set = set([ 
                                        '卢湾区', '嘉定区', '浦东新区', 
                                        '虹口区', '普陀区', '南浔区', 
                                        '青浦区', '闵行区', '镇海区', 
                                        '金山区', '松江区', '吴江区', 
                                        '徐汇区', '静安区', '余杭区',
                                        ])
        self._subdistrict_cn_set = set([ 
                                        '南汇新城镇', '枫泾镇', '白马镇', 
                                        '月浦镇', '低塘街道', '临港新城', 
                                        '泗门镇', '新埭镇', '长兴县', 
                                        '洋泾街道', '周巷镇', '新市镇', 
                                        '友谊路街道', '金桥', '坎山镇'
                                        ])
        
    def clean_simple_address(self):
        '''
        only rename key
        '''
        rename_dict = {
            'tags.addr:flats' : 'address.flats',
            'tags.addr:door': 'address.door',
            'tags.addr:full': 'address.full',
            'tags.addr:neighbourhood' : 'address.neighbourhood',
            'tags.addr:street:alley' : 'address.street_alley',
            'tags.addr:housenumber:en': 'address.housenumber_en',
            'tags.tags.addr:street:en': 'address.street_en',
            
        }

        for org_key, tar_key in rename_dict.items():
            self._rename_only(org_key, tar_key)
                

    def clean_postcode(self):
        '''
        postcode 有两个标签：
            addr:postcode
            postal_code
        修改后标签:
            address.postcode
        '''
        # clean postal_code
        self._check_postal_code()

        # clean addr:postcode
        mongoCollection.update_many({ 'tags.addr:postcode' : {'$exists': True}}, {'$rename': {'tags.addr:postcode':'address.postcode'}})
        self._logger.info("rename tags.addr:postcode")

        # check postcode 6 numbers
        self._check_postcode()

    def clean_city(self):
        '''
        city 原始标签：
            addr:city
            addr:city_1
            addr:city:en
        修改后标签:
            address.city
            address.city_en

        mongo shell
            collection.find({ '$or': [{'tags.addr:city_1' : {'$exists': true}},{'tags.addr:city' : {'$exists': true}},{'addr:city:en' : {'$exists': true}}]}).count()
            collection.find({ 'address.city': {'$exists': true}}).count()
            collection.find({ 'address.city_en': {'$exists': true}}).count()            
        '''
        
        self._check_city_only_num()

        self._combine_address_city()

        self._check_city_cn_district()
        self._check_city_cn_subdistrict()
        
        # must invoke after _combine_address_city
        self._check_city_cn_content()

    def clean_special_address(self):
        self._logger.info("enter clean_special_address")
        try:
            # 'tags.描述',
            mongoCollection.update_one({'id': '481554605', 'tags.描述': '仅2B座第四层'},
                                            {'$set': {'address.interpolation': '2B座',
                                                    'address.floor': '4楼'},
                                            '$unset': {'tags.描述':''}})
        except Exception as e:
            self._logger.error("clean_special_address {}".format(e))
        finally:
            
            self._logger.info("leave clean_special_address")



    def clean_special_city(self):
        ''' should put before actual work'''
        self._logger.info("enter clean_special_city")
        try:
            # remove special value
            mongoCollection.update_one({'tags.addr:city': '碧桂园凤凰城大凤凰'}, {'$unset': {'tags.addr:city': ''}})
            
            mongoCollection.update_one({"id": "4471354725","tags.addr:city": "南汇路"},
                                            {"$unset":{"tags.addr:city":''}})

            mongoCollection.update_one({"id": "515530977","tags.addr:city": "松江区","tags.addr:district":"车墩镇"},
                                            {
                                                "$set":{"tags.addr:district":"松江区",
                                                    "address.subdistrict":"车墩镇"},
                                                "$unset":{"tags.addr:city":''}})

            mongoCollection.update_one({"id": "407734300","tags.addr:city": "浙江省杭州市"},
                                            {"$set":{"tags.addr:province":"浙江省",
                                                    "tags.addr:city":"杭州市"}})
            mongoCollection.update_one({"id": "514675513","tags.addr:city": "上海市嘉定区","tags.addr:district":"马陆镇"},
                                            {"$set":{"tags.addr:city":"上海市",
                                                    "tags.addr:district":"嘉定区",
                                                    "address.subdistrict":"马陆镇"}})
            mongoCollection.update_one({"id": "305511037","tags.addr:city": "浦江漕河泾高科技园区新骏环路800号"},
                                            {"$set":{"address.subdistrict":"浦江漕河泾高科技园区",
                                                    "tags.addr:street":"新骏环路",
                                                    "tags.addr:housenumber":"800号"},
                                            "$unset":{"tags.addr:city":''}})
            mongoCollection.update_one({"id": "407734302","tags.addr:city": "浙江·杭州"},
                                            {"$set":{"tags.addr:province":"浙江省",
                                                    "tags.addr:city":"杭州市"}})

            mongoCollection.update_one({"id": "440145256","tags.addr:city": "上海静安区昌平路68号"},
                                            {"$set":{"address.city":"上海市",
                                                     "address.district":"静安区"},
                                             '$unset': {'tags.addr:city':''}})
            
            # two record
            mongoCollection.update_many({'$and' :[
                        {"tags.addr:city": "浦东新区"},
                        {'tags.addr:district':'高行镇'},
                        {'$or': [{'id': '511932164'}, {'id': '511929127'}]}
                    ]},
                    {"$set":{"tags.addr:district":"浦东新区",
                             "address.subdistrict":"车墩镇"},
                     "$unset":{"tags.addr:city":''}})
            
            mongoCollection.update_many({'$and' :[
                        {"tags.addr:city": "宜兴市湖父镇"},
                        {'$or': [{'id': '4698565580'}, {'id': '4698577398'}]}
                    ]},
                    {"$set":{"tags.addr:city":"宜兴市",
                             "address.subdistrict":"湖父镇"}})
            # rename
            mongoCollection.update_many({'$and' :[
                        {"tags.addr:city": "宝山城市工业园区"},
                        {'$or': [{'id': '467382514'}, {'id': '467382515'}]}
                    ]},
                    {"$rename":{"tags.addr:city":"address.subdistrict"}})
            
        except Exception as e:
            self._logger.error("clean_special_city {}".format(e))
        finally:
            self._logger.info("leave clean_special_city")

    def clean_country(self):
        '''
        country 原始标签：
            addr:country
        修改后标签:
            address.country

        shell
            grep 'k="addr:country' shanghai_china.osm | cut -d'"' -f4 |sort|uniq
            得到三种可能值：CH,CN,ZH
        '''
        self._logger.info("enter clean_country")
        self._log_multi_count(['tags.addr:country', 'address.country'], True)
        try:
            
            cursor = mongoCollection.find({'tags.addr:country': {'$exists': True}})
            for doc in cursor:
                if doc['tags']['addr:country'] in ['CH', 'CN', 'ZH']:
                    mongoCollection.update_one({'_id': doc['_id']},
                                            {'$set': {'address.country':'CN'}, 
                                            '$unset': {'tags.addr:country': ''}})
                    self._logger.info('update country {}'.format(doc['_id']))
        except Exception as e:
            self._logger.error("clean_country {}".format(e))
        finally:
            self._log_multi_count(['tags.addr:country', 'address.country'], False)
            self._logger.info("leave clean_country")
            
                  
    def clean_district(self):
        '''
        district 原始标签：
            addr:district
        修改后标签:
            address.district
            address.district_en

        shell
            grep 'k="addr:district' shanghai_china.osm | cut -d'"' -f4 |sort|uniq
            grep 'k="addr:district' shanghai_china.osm | cut -d'"' -f2 |sort|uniq
            中英文混杂
        '''
        self._logger.info("enter clean_district")
        try:
            self._log_multi_count(['tags.addr:district', 'address.district', 'address.district_en'], True)

            # 修改特殊值
            mongoCollection.update_many({ 'tags.addr:district' : 'Jiading Industrial Zone'}, 
                {'$set': {'tags.addr:district':'Jiading'}})

            mongoCollection.update_many({ 'tags.addr:district' : 'Pudong New District'}, 
                {'$set': {'tags.addr:district':'pudong'}})
    
            self._clean_district()
            self._check_district_contain_city()
        except Exception as e:
            self._logger.error('clean_district {}'.format(e))        
        finally:
            self._log_multi_count(['tags.addr:district', 'address.district', 'address.district_en'], False)
            self._logger.info("leave clean_district")
        
    def clean_interpolation(self):
        self._logger.info("enter clean_interpolation")
        try:
            cursor = mongoCollection.find({'tags.addr:interpolation': {'$exists': True},
                                               'address.interpolation': {'$exists': False}})

            for doc in cursor:
                org_interpolation = doc['tags']['addr:interpolation']
                new_interpolation = org_interpolation.replace('~', '-').replace('～', '-')

                self._rename_one_with_content_change(doc['_id'], 
                                                     'tags.addr:interpolation',
                                                     'address.interpolation',
                                                     new_interpolation)

        except Exception as e:
            self._logger.error('clean_interpolation {}'.format(e))        
        finally:
            
            self._logger.info("leave clean_interpolation")

    def clean_housenumber(self):
        '''
        housenumber 原始标签：
            addr:housenumber:en 
            addr:housenumber
        修改后标签:
            address.housenumber
            address.housenumber_en
        '''
        self._logger.info("enter clean_housenumber")

        try:            
            self._log_multi_count(['tags.addr:housenumber', 
                                      'address.housenumber', 
                                      'address.housenumber_en'],
                                      True)
            
            # rename tag
            mongoCollection.update_many({ 'tags.addr:housenumber' : {'$exists': True}}, {'$rename': {'tags.addr:housenumber':'address.housenumber'}})
        
            self._clean_housenumber_charactor_clean()

            self._clean_housenumber_without_number()

            self._check_housenumber_is_phone()

            self._clean_housenumber_remove_character()

            self._clean_housenumber_with_street_alley()

            self._clean_housenumber_with_street_org_street()
            self._clean_housenumber_with_street_no_org_street()


            self._clean_housenumber_with_alley()
            # should invoke at last
            self._clean_housenumber_with_interpolation()


        except Exception as e:
            self._logger.error("clean_housenumber: {}".format(e))
        finally:
            self._log_multi_count(['tags.addr:housenumber', 'address.housenumber'], False)
            self._logger.info("leave clean_housenumber")

    
    def clean_street(self):
        '''
        street 原始标签：
            addr:street
        修改后标签:
            address.street
            address.street_en
        '''
        # invoke first
        self._clean_street_character_clean()
        
        # chinese only
        mongoCollection.update_many({'tags.addr:street' :{'$regex': '^[\u4e00-\u9fff]+$'},
                                        'address.street': {'$exists': False}},
                                        {'$rename': {'tags.addr:street': 'address.street'}})
        # 黄渡 · 绿苑路
        mongoCollection.update_many({'tags.addr:street' :{'$regex': '^[\u4e00-\u9fff]+ {0,1}· {0,1}[\u4e00-\u9fff]+$'},
                                        'address.street': {'$exists': False}},
                                        {'$rename': {'tags.addr:street': 'address.street'}})
        # 淮海中路,近茂名路
        mongoCollection.update_many({'tags.addr:street' :{'$regex': '^[\u4e00-\u9fff]+,[\u4e00-\u9fff]+$'},
                                        'address.street': {'$exists': False}},
                                        {'$rename': {'tags.addr:street': 'address.street'}})

        # S308
        mongoCollection.update_many({'tags.addr:street' :{'$regex': '^S\d+$'},
                                        'address.street': {'$exists': False}},
                                        {'$rename': {'tags.addr:street': 'address.street'}})

        # self._clean_street_only_chinese()
        
        self._clean_street_with_alley()

        self._clean_street_with_housenumber_interpolation()        
        self._clean_street_with_housenumber_no_interpolation()
                
        self._clean_street_split_cn_en()
        
        self._clean_street_en_only_rename_only()

        # invoke after _clean_street_en_only_rename_only
        self._clean_street_check_en_content()

        # invoke after _clean_street_check_en_content
        self._clean_street_en_with_housenumber()
        # invoke after _clean_street_en_with_housenumber
        self._clean_street_en_only_to_cn()

    def clean_province(self):
        self._logger.info("enter clean_province")

        shanghai_list = ['shnghai', 'Shanghai', '上海市', 's上海', '上海', 'Shanghai Shi', 'shanghai']
        jiangsu_list = ['江蘇省', '江苏省', 'Jiangsu', '江苏', 'j江苏省']
        zhejiang_list = ['浙江省', 'ZheJiang', 'Zhejiang', '浙江']
        try:
            db_org_key = 'tags.addr:province'
            db_tar_key = 'address.province'

            cursor = mongoCollection.find({db_org_key: {'$exists': True}, db_tar_key: {'$exists': False}})

            for doc in cursor:
                org_provience = doc['tags']['addr:province']

                if org_provience in shanghai_list:
                    mongoCollection.update_one({'_id': doc['_id']},
                        {'$set': {'address.province': '上海市'},
                         '$unset': {'tags.addr:province':''}})
                elif org_provience in jiangsu_list:
                    mongoCollection.update_one({'_id': doc['_id']},
                        {'$set': {'address.province': '江苏省'},
                         '$unset': {'tags.addr:province':''}})
                elif org_provience in zhejiang_list:
                    mongoCollection.update_one({'_id': doc['_id']},
                        {'$set': {'address.province': '浙江省'},
                         '$unset': {'tags.addr:province':''}})

        except Exception as e:
            self._logger.error('clean_province {}'.format(e))        
        finally:
            self._logger.info("leave clean_province")

    def clean_special_province(self):
        self._logger.info("enter clean_special_province")
        try:
            mongoCollection.update_one({'id':'497378391', 'tags.addr:province': '浙江省杭州市余杭区文一西路1500号'},
                                           {'$set': {'address.province': '浙江省',
                                                   'address.street': '文一西路',
                                                   'address.housenumber': '1500号'},
                                            '$unset': {'tags.addr:province':'',
                                                    'tags.addr:housenumber': '',
                                                    'tags.addr:street':''}})

            mongoCollection.update_one({'id':'4886658847', 'tags.addr:province': 'Zhangjiang'},
                                           {'$set': {'address.subdistrict': '张江镇',
                                                   'address.subdistrict_en': 'Zhangjiang Town'},
                                            '$unset': {'tags.addr:province':''}})

            mongoCollection.update_one({'id':'501089510', 'tags.addr:province': '200332'},
                                           {'$rename': {'tags.addr:province' : 'address.postcode'}})

            
            mongoCollection.update_many(
                                            {'$and': [{'tags.addr:province': 'Jinqiao'},
                                                    {'$or': [
                                                            {'id':'4886670572'},
                                                            {'id':'4886667081'},
                                                            ]}
                                                    ]},                
                                           {'$set': {'address.subdistrict': '金桥',
                                                   'address.subdistrict_en': 'Jinqiao'},
                                            '$unset': {'tags.addr:province':''}})

            mongoCollection.update_many(
                                            {'$and': [{'tags.addr:province': '江苏苏州'},
                                                    {'$or': [
                                                            {'id':'538713492'},
                                                            {'id':'538713832'},
                                                            ]}
                                                    ]},                
                                           {'$set': {'address.province': '江苏省',
                                                   'address.city': '苏州市'},
                                            '$unset': {'tags.addr:province':''}})
                
        except Exception as e:
            self._logger.error('clean_special_province {}'.format(e))        
        finally:
            self._logger.info("leave clean_special_province")            

    def _clean_street_en_only_to_cn(self):
        ''' englisht to chinese'''
        self._logger.info("enter _clean_street_en_only_to_cn")
        en_key = 'address.street_en'
        cn_key = 'address.street'


        try:
            self._log_count(cn_key, True)
            
            cursor = mongoCollection.find({en_key: {'$exists': True}, cn_key: {'$exists': False}})
            for doc in cursor:                
                en_name = doc['address']['street_en'].lower()
                
                if en_name in self._road_en_cn_dict.keys():
                    self._set_unset_one_record(doc['_id'],{cn_key:self._road_en_cn_dict[en_name]})
                    self._logger.info("update {}".format(doc['_id']))
        except Exception as e:
            self._logger.error("_clean_street_en_only_to_cn {}".format(e))
            
        finally:
            self._log_count(cn_key, False)
            self._logger.info("leave _clean_street_en_only_to_cn")

    def _clean_street_split_cn_en(self):
        self._logger.info("enter _clean_street_split_cn_en")
        org_key = 'tags.addr:street'
        tar_key = 'address.street'
        try:
            self._log_count(org_key, True)
            self._log_count(tar_key, True)
            
            cursor = mongoCollection.find({org_key: {'$exists': True}})
            for doc in cursor:
                street_name = doc['tags']['addr:street']
                if ChineseFilter.search(street_name):
                    cn_street_name = ChineseFilter.findall(street_name)
                    if len(cn_street_name) != 1:
                        self._logger.error("_clean_street_split_cn_en check {}".format(doc['_id']))
                        continue
                    else:
                        cn_street_name = cn_street_name[0]
                        en_street_name = ChineseFilter.sub('', street_name).strip()

                        set_dict={tar_key: cn_street_name}
                        if len(en_street_name)>0 and en_street_name[0] == '(' and en_street_name[-1]==')':
                            en_street_name = en_street_name[1:-1]
                            set_dict["{}_en".format(tar_key)] = en_street_name
                    unset_dict = {org_key: ''}
                    mongoCollection.update_one({'_id': doc['_id']},{'$set': set_dict,'$unset': unset_dict})
                    self._logger.info("update {}".format(doc['_id']))
        except Exception as e:
            self._logger.error("_clean_street_split_cn_en {}".format(e))
            raise e
            
        finally:
            self._log_count(org_key, False)
            self._log_count(tar_key, False)
            self._logger.info("leave _clean_street_split_cn_en")

    def _clean_street_with_alley(self):
        self._logger.info("leave _clean_street_with_alley")

        org_key = 'tags.addr:street'
        tar_key = 'address.street'
        alley_key = 'address.street_alley'

        try:
            self._log_count(org_key, True)
            self._log_count(tar_key, True)
            self._log_count(alley_key, True)

            # 泰安路115弄
            # 胶州路 319 弄
            streetname_alley_filter =  '^([\u4e00-\u9fff]+路)(\d+弄)$'

            
            cursor = mongoCollection.find({org_key: {'$regex': '^[\u4e00-\u9fff]+路\d+弄$'},
                                            alley_key: {'$exists': False},
                                            'tags.addr:street:alley' : {'$exists': False}})
            
            for doc in cursor:
                org_street_name = doc['tags']['addr:street']

                # if re.search(streetname_alley_filter,org_street_name):
                (street, alley) = re.findall(streetname_alley_filter,org_street_name)[0]
            
                self._set_unset_one_record(doc['_id'],
                                                        {tar_key: street,alley_key : alley},
                                                        {org_key: ''})
                self._logger.info("update {}".format(doc['_id']))

        except Exception as e:
            self._logger.info("_clean_street_with_alley {}".format(e))
        finally:
            self._log_count(org_key, False)
            self._log_count(tar_key, False)
            self._log_count(alley_key, False)
            self._logger.info("leave _clean_street_with_alley")

    def _clean_street_with_housenumber_interpolation(self):
        self._logger.info("enter _clean_street_with_housenumber_interpolation")
        org_key = 'tags.addr:street'
        tar_key = 'address.street'
        housenumber_key = 'tags.addr:housenumber'
        new_housenumber_key = 'address.housenumber'
        interpolation_key = 'tags.addr:interpolation'
        new_interpolation_key = 'address.interpolation'


        try:

            self._log_count(org_key, True)
            self._log_count(tar_key, True)
            self._log_count(new_housenumber_key, True)
            self._log_count(new_interpolation_key, True)

            # 杨高南路5678号
            streetname_filter =  '^([\u4e00-\u9fff]+路)(\d+号{0,1})$'

            # 杨高南路5678号
            # B13B
            cursor = mongoCollection.find({org_key: {'$regex': '^[\u4e00-\u9fff]+路\d+号'},
                                            housenumber_key : {'$exists': True},
                                            new_housenumber_key : {'$exists': False},
                                            interpolation_key : {'$exists': False},
                                            new_interpolation_key : {'$exists': False}})

            for doc in cursor:
                street_name = doc['tags']['addr:street']
                org_housenumber = doc['tags']['addr:housenumber']

                (new_street_name,housenumber) = re.findall(streetname_filter, street_name)[0]                

                self._set_unset_one_record(doc['_id'],
                                                {tar_key: new_street_name,
                                                new_housenumber_key : housenumber,
                                                new_interpolation_key: org_housenumber},
                                                {org_key: '', housenumber_key: ''})

                
                self._logger.info("update {}".format(doc['_id']))
                # db.final.find({'tags.addr:street': {$exists: true}}, {'tags.addr:street':1})
        except Exception as e:
            self._logger.error("_clean_street_with_housenumber_interpolation {}".format(e))
            
        finally:
            self._log_count(org_key, False)
            self._log_count(tar_key, False)
            self._log_count(new_housenumber_key, False)
            self._log_count(new_interpolation_key, False)
            self._logger.info("leave _clean_street_with_housenumber_interpolation")

    def _clean_street_with_housenumber_no_interpolation(self):
        self._logger.info("enter _clean_street_with_housenumber_no_interpolation")
        org_key = 'tags.addr:street'
        tar_key = 'address.street'
        housenumber_key = 'tags.addr:housenumber'
        new_housenumber_key = 'address.housenumber'


        try:

            self._log_count(org_key, True)
            self._log_count(tar_key, True)
            self._log_count(new_housenumber_key, True)

            # 中山北路3736号
            streetname_filter =  '^([\u4e00-\u9fff]+路)(\d+号{0,1})$'

            cursor = mongoCollection.find({org_key: {'$regex': '^[\u4e00-\u9fff]+路\d+号'},
                                            housenumber_key : {'$exists': False},
                                            new_housenumber_key : {'$exists': False}})

            for doc in cursor:
                street_name = doc['tags']['addr:street']

                (new_street_name,housenumber) = re.findall(streetname_filter, street_name)[0]                

                self._set_unset_one_record(doc['_id'],
                                                {tar_key: new_street_name,
                                                new_housenumber_key : housenumber},
                                                {org_key: ''})

                
                self._logger.info("update {}".format(doc['_id']))
                # db.final.find({'tags.addr:street': {$exists: true}}, {'tags.addr:street':1})
        except Exception as e:
            self._logger.error("_clean_street_with_housenumber_no_interpolation {}".format(e))
            
        finally:
            self._log_count(org_key, False)
            self._log_count(tar_key, False)
            self._log_count(new_housenumber_key, False)
            self._logger.info("leave _clean_street_with_housenumber_no_interpolation")

    def _clean_street_check_en_content(self):
        self._logger.info("enter _clean_street_check_en_content")
        db_key = 'address.street_en'
        
        try:
            cursor = mongoCollection.find({db_key: {'$exists': True}})
            for doc in cursor:
                street_en_name = doc['address']['street_en']

                new_street_name = re.sub(' *[Rr][Dd][\.|,]{0,1}$', ' Road', street_en_name)             
                new_street_name = re.sub(' +[Ll][Uu]$', ' Road', new_street_name)
                new_street_name = re.sub(' *[A|a]ve\.$', ' Avenue', new_street_name)
                
                new_street_name = re.sub('[\n ]+', ' ', new_street_name)
                new_street_name = re.sub('\( *\)', '', new_street_name)
                new_street_name = re.sub('Rd\.{0,1} *\(', 'Road (', new_street_name)
                
                new_street_name = new_street_name.strip()

                if re.search('[\u0061-\u007a](Rd)$', new_street_name):
                    new_street_name = re.sub("Rd$", ' Road', new_street_name)
                if new_street_name != street_en_name:                    
                    mongoCollection.update_one({'_id': doc['_id']}, 
                                                  {'$set': {db_key: new_street_name}})
                    self._logger.info("update {}".format(doc['_id']))
        except Exception as e:
            self._logger.error("_clean_street_check_en_content {}".format(e))
            raise e
            
        finally:            
            self._logger.info("leave _clean_street_check_en_content")

    def _clean_street_en_with_housenumber(self):
        self._logger.info("enter _clean_street_en_with_housenumber")
        db_key = 'address.street_en'
        
        try:
            cursor = mongoCollection.find({db_key: {'$exists': True}})
            for doc in cursor:

                street_en_name = doc['address']['street_en']
                check_patten = '^([#]|NO.|^)*(\d+)[a-zA-Z ]+[Rr]oad$'
                
                if not re.search(check_patten,street_en_name):
                    continue
                number = re.findall(check_patten,street_en_name)[0][1]
                
                street_en = re.sub('^([#]|NO.|^])*\d+ +', '', street_en_name)

                set_dict = {db_key: street_en}
                
                if 'addr:housenumber' not in doc['tags'] and \
                    ('address' not in doc or 'housenumber' not in doc['address']):
                    set_dict['address.housenumber'] = number
                                 
                mongoCollection.update_one({'_id': doc['_id']}, {'$set': set_dict})
                self._logger.info("update {}".format(doc['_id']))
        except Exception as e:
            self._logger.error("_clean_street_en_with_housenumber {}".format(e))
            
        finally:            
            self._logger.info("leave _clean_street_en_with_housenumber")

    def _clean_street_en_only_rename_only(self):
        ''' after special changed'''
        self._logger.info("enter _clean_street_en_only_rename_only")
        org_key = 'tags.addr:street'
        tar_key = 'address.street_en'
        try:
            cursor = mongoCollection.find({org_key: {'$exists': True}})
            for doc in cursor: 
                street_name = doc['tags']['addr:street']

                
                if ChineseFilter.search(street_name) or NumberFilter.search(street_name):
                    continue
                                
                self._rename_one_record(doc['_id'],org_key, tar_key)
                    
        except Exception as e:
            self._logger.error("_clean_street_en_only_rename_only {}".format(e))
            
        finally:
            self._log_count(org_key, False)
            self._log_count(tar_key, False)
            self._logger.info("leave _clean_street_en_only_rename_only")

    def clean_special_street(self):
        self._logger.info("enter clean_special_street")
        def town_in_street(outer, town_name):
            outer._logger.info("enter town_in_street")
            try:
                cursor = mongoCollection.find({'tags.addr:street' : town_name, 
                                            'tags.addr:housenumber': {'$exists': True},
                                            'address.subdistrict': {'$exists': False},
                                            'tags.addr:subdistrict': {'$exists': False}})
                for doc in cursor:
                    housenumber = doc['tags']['addr:housenumber']

                    street_housenumber_filter = '^([\u4e00-\u9fff]+路)(\d+ *号)$'
                    street_alley_filter = '^([\u4e00-\u9fff]+路)(\d+ *弄)$'
                    if re.search(street_housenumber_filter, housenumber):
                        (street, housenumber) = re.findall(street_housenumber_filter, housenumber)[0]
                        housenumber = housenumber.replace(' ', '')
                        set_dict = {
                            'address.housenumber': housenumber,
                            'address.street': street,
                            'address.subdistrict': town_name
                        }
                        unset_dict = {'tags.addr:street':'', 'tags.addr:housenumber':''}
                        outer._set_unset_one_record(doc['_id'], set_dict, unset_dict)
                    elif re.search(street_alley_filter, housenumber):
                        (street, alley) = re.findall(street_alley_filter, housenumber)[0]
                        alley = alley.replace(' ', '')
                        set_dict = {
                            'address.street_alley': alley,
                            'address.street': street,
                            'address.subdistrict': town_name
                        }
                        unset_dict = {'tags.addr:street':'', 'tags.addr:housenumber':''}
                        outer._set_unset_one_record(doc['_id'], set_dict, unset_dict)
                    else:
                        outer._logger.error("hangtou_town should not go here {}".format(doc['_id']))
            except Exception as e:
                outer._logger.error("town_in_street {}".format(e))
            finally:
                outer._logger.info("leave town_in_street")
        
        def road_lane(outer):
            outer._logger.info("enter road_lane")
            try:
                cursor = mongoCollection.find({'tags.addr:street': {'$exists' : True},
                                                  'tags.addr:housenumber': {'$exists' : True}})

                street_lane_filter = '([\u4e00-\u9fff]+)(\d+[弄]) +[Ll]ane +(\d+) +of +([a-zA-Z ]+)$'

                for doc in cursor:
                    street_name = doc['tags']['addr:street']
                    
                    if re.search(street_lane_filter, street_name):
                        # outer._logger.info(street_name)
                        # '密云路454弄 Lane 454 of Miyun Road'
                        (cn_name, cn_number, en_number, en_name) = re.findall(street_lane_filter, street_name)[0]
                        set_dict = {
                            'address.street': cn_name,
                            "address.street_en": en_name,
                            'address.street_alley': cn_number,
                        }
                        unset_dict = {
                            'tags.addr:housenumber': '',
                            'tags.addr:street': ''
                        }
                        
                        outer._set_unset_one_record(doc['_id'], set_dict, unset_dict)
            except Exception as e:
                outer._logger.error("road_lane {}".format(e))
            finally:
                outer._logger.info("leave road_lane")
               
        def alley_number(outer) :
            # Alley 2500, Xiupu Road
            # Alley 3668, Xiuyan Road
            
            outer._logger.info("enter alley_number")
            try:
                cursor = mongoCollection.find({'tags.addr:street': {'$exists' : True}, 
                                              'tags.addr:street:alley': {'$exists' : False},
                                              'address.street_alley': {'$exists' : False}})

                alley_number_filter = 'Alley +(\d+), +([a-zA-Z]+) *Road$'

                road_dict ={
                            'Xiuyan': '秀沿路',
                            'Xiupu': '秀浦路'
                        }
                for doc in cursor:
                    street_name = doc['tags']['addr:street']

                    if re.search(alley_number_filter,street_name):                        
                        (alley, road_name) = re.findall(alley_number_filter,street_name)[0]
                        
                        if road_name not in road_dict:
                            continue

                        set_dict = {
                            'address.street': road_dict[road_name],
                            "address.street_en": "{} Road".format(road_name),
                            'address.street_alley' : "{}弄".format(alley)
                        }
                        unset_dict = {'tags.addr:street': ''}
                        outer._set_unset_one_record(doc['_id'], set_dict, unset_dict)
            except Exception as e:
                outer._logger.error("alley_number {}".format(e))
            finally:
                outer._logger.info("leave alley_number")

        def road_alley(outer) :
            # 镇宁路255-275弄
            # 江苏路46-78弄
            # 河南中路531-541弄
            outer._logger.info("enter road_alley")
            try:
                cursor = mongoCollection.find({'tags.addr:street': {'$exists' : True}, 
                                              'tags.addr:street:alley': {'$exists' : False},
                                              'address.street_alley': {'$exists' : False}})
                road_alley_filter = '^([\u4e00-\u9fff]+路)(\d+-\d+弄)$'

                for doc in cursor:
                    street_name = doc['tags']['addr:street']
                    
                    if re.search(road_alley_filter,street_name):                        
                        (road_name, alley) = re.findall(road_alley_filter,street_name)[0]
                        
                       
                        set_dict = {
                            'address.street': road_name,
                            'address.street_alley' : alley
                        }
                        unset_dict = {'tags.addr:street': ''}
                        outer._set_unset_one_record(doc['_id'], set_dict, unset_dict)
            except Exception as e:
                outer._logger.error("road_alley {}".format(e))
            finally:
                outer._logger.info("leave road_alley")

        def xiuyan_road(outer):
            outer._logger.info("enter xiuyan_road")
            # 秀沿路1028弄2支弄, 78-80
            # 秀沿路1028弄2支弄, 107
            # 28 record


            try:
                cursor = mongoCollection.find({'tags.addr:street': {'$regex': '[\u4e00-\u9fff]+路\d+弄\d+支弄'},
                                                   'tags.addr:housename': {'$regex':'\d+-{0,1}[\d]*'},
                                                   'tags.addr:housenumber' : {'$exists': False}})

                for doc in cursor:
                    org_street = doc['tags']['addr:street']
                    org_housename = doc['tags']['addr:housename']
                    
                    (street, alley) = re.findall('^([\u4e00-\u9fff]+路)(\d+弄\d+支弄)$', org_street)[0]
                    mongoCollection.update_one({'_id': doc['_id']},
                        {'$set': {'address.street': street,
                                'address.street_alley': alley,
                                'address.housenumber': org_housename},
                         '$unset': {'tags.addr:street':'', 'tags.addr:housename':''}})
                    self._logger.info("update {}".format(doc['_id']))

            except Exception as e:
                outer._logger.error("xiuyan_road {}".format(e))
            finally:
                outer._logger.info("leave xiuyan_road")


            
        try:
            # before town_in_street invoke
            mongoCollection.update_one(            
                {'id': '298458681', 'tags.addr:housenumber': '闸航路2550弄39号', 'tags.addr:street' : '浦江镇'},
                {'$set': {'address.street_alley': '2550弄',
                        'address.housenumber': '39号',
                        'address.street': '闸航路',
                        'address.subdistrict': '浦江镇'},
                '$unset': {'tags.addr:housenumber' : '',
                            'tags.addr:street' : ''}})

            # error street name
            for town in ['航头镇', '浦江镇']:
                town_in_street(self, town)

            road_lane(self)
            alley_number(self)
            road_alley(self)
            xiuyan_road(self)

            # let number del with it
            mongoCollection.update_one(            
                {'id': '4484129216', 'tags.addr:street': '1440弄19号'},
                {'$set': {'tags.addr:street': 'tags.addr:housenumber'}})

            # 不能识别的语言
            mongoCollection.delete_one({'id':'4835061722', 'tags.addr:street':"لندینگ هوم 1"})

            # street name only number
            mongoCollection.update_many(
                {'tags.addr:street' : {'$regex': "^\d+$"}},{'$unset': {'tags.addr:street':''}})
            
            mongoCollection.update_one(            
                {'id': '68522234', 'tags.addr:street': 'Dong Zhu An Bang Road'},
                {'$set': {'address.street': '东诸安浜路',
                        'address.street_en': 'Dong Zhu An Bang Road'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '261788415', 'tags.addr:street': '濠锦路15'},
                {'$set': {'tags.name': '锦江苑',
                        'address.street': '濠锦路',
                        'address.housenumber':'15号',
                        'address.interpolation': '1号',
                        'address.housename': '锦江苑'
                        },
                 '$unset': {'tags.addr:street':'', 'tags.addr:housename': ''}})

            mongoCollection.update_many({'tags.addr:street:en': 'East Zhongshan No. 1 Road'},
                                            {'$set': {'address.street_en': 'East Zhongshan Number 1 Road'},
                                            '$unset': {'tags.addr:street:en': ''}})


            mongoCollection.update_one(            
                {'id': '1563153083', 'tags.addr:street': '上海市长宁区中山西路1277号'},
                {'$set': {'address.city': '上海市',
                        'address.street': '中山西路',
                        'address.housenumber':'1277号',
                        'address.district': '长宁区'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '5170281821', 'tags.addr:street': '湖州市金山路', 'tags.name': '湖州市金盖山路66号'},
                {'$set': {'address.city': '湖州市',
                        'address.street': '金盖山路',
                        'address.housenumber':'66号'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_many({'$and':[
                                            {'tags.addr:street': '坎山镇人民路'},
                                            {'$or' : [{'id': '4544265390'}, {'id': '4544745089'},{'id': '4544745189'}]}
                                            ]},
                                        {'$set': {'address.street': '人民路',
                                                'address.subdistrict':'坎山镇'},
                                         '$unset': {'tags.addr:street': ''}})

            mongoCollection.update_many({'$and':[
                                            {'tags.addr:street': '松江人民北路'},
                                            {'$or' : [{'id': '351291678'}, {'id': '351295430'}]}
                                            ]},
                                        {'$set': {'address.street': '人民北路'},
                                         '$rename': {'tags.addr:city': 'address.subdistrict'},
                                         '$unset': {'tags.addr:street': ''}})

            mongoCollection.update_one(            
                {'id': '1667664835', 'tags.addr:street': '#3999 XiuPu Road'},
                {'$set': {'address.street': '秀浦路',
                        'address.street_en': 'XiuPu Road Road',
                        'address.housenumber': '3999号',
                        'address.interpolation': '37栋'},
                 '$unset': {'tags.addr:street':'', 'tags.addr:housenumber':''}})

            mongoCollection.update_one(            
                {'id': '113467395', 'tags.addr:street:en': 'Dongxin Avenue'},
                {'$set': {'address.street': '东信大道',
                        'address.street_en': 'Dongxin Avenue',
                        'address.city_en': 'Hangzhou',
                        'address.city': '杭州市'},
                 '$unset': {'tags.addr:street:en':'', 'tags.addr:city:en':''}})

            mongoCollection.update_one(            
                {'id': '504413799', 'tags.addr:street': '春晓街道港博路'},
                {'$set': {'address.street': '港博路',
                        'address.subdistrict': '春晓街道'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '2697218683', 'tags.addr:street': '青浦区盈港路1002号'},
                {'$set': {'address.street': '盈港路',
                        'address.district': '青浦区',
                        'address.housenumber': '1002号'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '2697216641', 'tags.addr:street': '青浦区盈港路1006号'},
                {'$set': {'address.street': '盈港路',
                        'address.district': '青浦区',
                        'address.housenumber': '1006号'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '1693952706', 'tags.addr:street':'NO.588 binhe road'},
                {'$set': {'address.street': '滨河路',
                        'address.street_en': 'Binhe Road',
                        'address.housenumber': '588号',
                        'address.interpolation': '4E17'},
                 '$unset': {'tags.addr:street':'', 'tags.addr:housenumber':''}})

            mongoCollection.update_one(            
                {'id': '4378607643', 'tags.addr:street':'昌平路', 'tags.addr:city': '上海市静安区昌平路68号601'},
                {'$set': {'address.street': '昌平路',
                        'address.housenumber': '68号',
                        'address.district': '静安区',
                        'address.city': '上海市',
                        'address.interpolation': '601'},
                 '$unset': {'tags.addr:street':'', 'tags.addr:city':'', 'tags.addr:housenumber':''}})

            mongoCollection.update_one(            
                {'id': '3812032396', 'tags.addr:street': '创强1'},
                {'$set': {'address.street': '创强路'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '4954619123', 'tags.addr:street': 'Bao An Gong Lu 3386'},
                {'$set': {'address.street': '宝安公路',                        
                        'address.street_en': 'Bao An Gong Lu',
                        'address.housenumber': '3386号'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '233832136', 'tags.addr:street': 'CaoXi Bei lu 99'},
                {'$set': {'address.street': '漕溪北路',
                        'address.street_en': 'North CaoXi Road',
                        'address.housenumber': '99号',
                        'address.interpolation': '2-3'},
                 '$unset': {'tags.addr:street':'', 'tags.addr:housenumber':''}})
            mongoCollection.update_one(            
                {'id': '233833192', 'tags.addr:street': 'CaoXi Bei lu 99'},
                {'$set': {'address.street': '漕溪北路',
                        'address.street_en': 'North CaoXi Road',
                        'address.housenumber': '99号',
                        'address.interpolation': '1'},
                 '$unset': {'tags.addr:street':'', 'tags.addr:housenumber':''}})
            
            
            mongoCollection.update_one(            
                {'id': '4693381198', 'tags.addr:street': 'fuyou rd 378'},
                {'$set': {'address.street': '福佑路',                        
                        'address.street_en': 'Fuyou Road',
                        'address.housenumber': '378号'},
                 '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '4114670192', 'tags.addr:street': 'Huyi Highway 3101'},
                {'$set': {'address.street': '沪宜公路',                        
                        'address.street_en': 'Huyi Highway',
                        'address.housenumber': '3101号'},
                 '$unset': {'tags.addr:street':'', 'tags.addr:housenumber':''}})

            mongoCollection.update_one(            
                {'id': '430486602', 'tags.addr:street': '独墅湖高教区'},
                {'$set': {'address.subdistrict': '独墅湖高教区'},
                 '$unset': {'tags.addr:street':''}})


            mongoCollection.update_one(            
                {'id': '264386178', 'tags.addr:street': '青浦区盈港路688弄72号'},
                {'$set': {'address.district': '青浦区',
                        'address.street': '盈港路',
                        'address.street_alley': '688弄',
                        'address.housenumber': '72号'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '264441080', 'tags.addr:street': '溧水区永阳镇金蛙路金蛙路'},
                {'$set': {'address.district': '溧水区',
                        'address.street': '金蛙路',
                        'address.subdistrict': '永阳镇'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '290223162', 'tags.addr:street': '滨湖区青祁路和梁溪路交叉西南角'},
                {'$set': {'address.district': '滨湖区',
                        'address.street': '青祁路和梁溪路交叉西南角'},
                '$unset': {'tags.addr:street':''}})
            mongoCollection.update_one(            
                {'id': '96664914', 'tags.addr:street': '上海市徐汇区淮海西路55号'},
                {'$set': {'address.district': '徐汇区',
                        'address.city': '上海市',
                        'address.housenumber': '55号',
                        'address.street': '淮海西路'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '258100653', 'tags.addr:street': '闸北区保德路'},
                {'$set': {'address.district': '闸北区',
                        'address.street': '保德路'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '4984049133', 'tags.addr:street': '仙林街道文苑路'},
                {'$set': {'address.subdistrict': '仙林街道',
                        'address.street': '文苑路'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '374279096', 'tags.addr:street': '筏头乡后坞村'},
                {'$rename': {'tags.addr:street': 'address.subdistrict'}})

            mongoCollection.update_one(            
                {'id': '1566517033', 'tags.addr:street': '西湖区华星路'},
                {'$set': {'address.district': '西湖区',
                        'address.street': '华星路'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '1973418060', 'tags.addr:street': '仙霞路333号10楼'},
                {'$set': {'address.housenumber': '333号',
                        'address.street': '仙霞路',
                        'address.floor': '10楼'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '4452121885', 'tags.addr:street': '吴中路吴中路'},
                {'$set': {'address.street': '吴中路'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '1224175828', 'tags.addr:street': '柏庐南路999号（Bailunan Rd.）'},
                {'$set': {'address.street_en':  'South Bailu Road',
                        'address.street': '柏庐南路',
                        'address.housenumber': '999号',
                        'address.city': '昆山市',
                        'address.country': 'CN',
                        'address.province': '江苏省',
                        'name': '乐购昆山吉田店',
                        'name_cn': "Tesco"},
                '$unset': {'tags.addr:street':'','tags.name':''}})

            mongoCollection.update_one(            
                {'id': '1778854607', 'tags.addr:street': '陕西北路66号科恩国际中心'},
                {'$set': {'address.street': '陕西北路',
                        'address.housenumber': '66号'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '1304348755', 'tags.addr:street': 'Nanjing W Rd, 静安区'},
                {'$set': {'address.district': '静安区',
                        'address.street': '南京西路',
                        'address.street_en': 'West Nanjing Road'},
                '$unset': {'tags.addr:street':''}})


            mongoCollection.update_one(            
                {'id': '4739240522', 'tags.addr:street': 'West of Xihu Lake, Xihu District'},
                {'$set': {'address.district': '西湖区',
                        'address.district_en': 'Xihu District'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '219875738', 'tags.addr:street': 'Xiangyin Road, Yangpu District'},
                {'$set': {'address.street': '翔殷路',
                        'address.street_en': 'Xiangyin Road',
                        'address.district' : '杨浦区',
                        'address.district_en' : 'Yangpu',
                        'address.housename_en': 'USST Science Park',
                        'address.interpolation': '16栋' },
                '$unset': {'tags.addr:street':'', 'tags.addr:housename': ''}})

            mongoCollection.update_one(            
                {'id': '5006114874', 'tags.addr:street': "Lane 500 Wuning Road, Putuo, Shanghai"},
                {'$set': {'address.street': '武宁路',
                        'address.street_en': 'Wuning Road',
                        'address.street_alley': '500弄',
                        'address.district': '普陀区',
                        'address.district_en': 'Putuo District',
                        'address.city': '上海市',
                        'address.city_en': 'Shanghai',
                        'name': '上海市中心招待所',
                        'name_en': 'Shanghai city central hostel'
                        },
                '$unset': {'tags.addr:street':'', 'tags.name': ''}})

            mongoCollection.update_one(            
                {'id': '4685192192', 'tags.addr:street': "Santaishan Rd, Xihu Qu, Hangzhou Shi, Zhejiang Sheng, Китай"},
                {'$set': {'address.street': '三台山路',
                        'address.street_en': 'Santaishan Road',
                        'address.housenumber': '161',
                        'address.district': '西湖区',
                        'address.district_en': 'Xihu District',
                        'address.city': '杭州市',
                        'address.city_en': 'Hangzhou'},
                '$unset': {'tags.addr:street':'', 'tags.addr:housenumber':''}})
            
            mongoCollection.update_one(            
                {'id': '1106099285', 'tags.addr:street': "Qingyang Rd. (M)"},
                {'$set': {'address.street': '庆阳路',
                        'address.street_en': 'Middle Qingyang Road'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '2408433892', 'tags.addr:street': "Lane 1555 Jinshajiang road(west)"},
                {'$set': {'address.street': '金沙江西路',
                        'address.street_en': 'West Jinshajiang Road',
                        'address.street_alley': '1555弄',
                        'address.housename_en': 'West BDC',
                        'address.city': '上海市',
                        'address.city_en': 'Shanghai'},
                '$unset': {'tags.addr:street':'', 'tags.addr:city':''}})

            mongoCollection.update_one(            
                {'id': '490438396', 'tags.addr:street': "1094弄"},
                {'$rename': {'tags.addr:street':'address.street_alley'}})

            mongoCollection.update_one(            
                {'id': '4977698358', 'tags.addr:street': "2 Ping hai Road, Hangzhou 3 10006, china"},
                {'$set': {'address.street': '平海路',
                        'address.street_en': 'Pinghai Road',
                        'address.housenumber': '2',
                        'address.city': '杭州市',
                        'address.city_en': 'Hangzhou',
                        'address.postcode': '310006',
                        'address.country': 'CN'},
                '$unset': {'tags.addr:street':''}})

            mongoCollection.update_one(            
                {'id': '4702556689', 'tags.addr:street': "Xiang yang south road 203"},
                {'$set': {'address.street': '襄阳南路',
                        'address.street_en': 'South Xiangyang Road',
                        'address.housenumber': '203',
                        'name_en': 'Hunan road police station',
                        'name': '湖南路派出所'},
                '$unset': {'tags.addr:street':'', 'tags.name:en':'', 'tags.addr:housenumber':''}})

            mongoCollection.update_one(            
                {'id': '317492503', 'tags.addr:street': "高新区商业街18号2-3楼"},
                {'$set': {'address.street': '高新区商业街',
                        'address.housenumber': '18号',
                        'address.floor': '2-3楼'},
                '$unset': {'tags.addr:street':''}})


        except Exception as e:
            self._logger.error("clean_special_street {}".format(e))
        finally:
            self._logger.info("leave clean_special_street")


    def _clean_street_character_clean(self):
        self._logger.info("enter _clean_street_character_clean")
        try:
            db_key = 'tags.addr:street'
            cursor = mongoCollection.find({db_key: {'$exists': True}})

            for doc in cursor:
                street_name = doc['tags']['addr:street']

                # 巨鹿路/Julu Rd
                new_street_name = re.sub('[ \r\n/]+', ' ', street_name)
                new_street_name = new_street_name.replace('路 ', '路').replace(' 弄', '弄')
                #淮海中路， 近茂名路
                new_street_name = re.sub('， ', ',', new_street_name)

                # 黄渡 • 绿苑路
                new_street_name = new_street_name.replace('•', '·')
                # 徐泾.育才路
                new_street_name = new_street_name.replace('.', '·')
                
                new_street_name = re.sub(' *· *', '·',new_street_name)
                
                '.'
                new_street_name = new_street_name.strip()

                # not change db key
                if street_name != new_street_name:
                    mongoCollection.update_one({'_id': doc['_id']},
                                                {'$set': {db_key: new_street_name}})
        except Exception as e:
            self._logger.info("_clean_street_character_clean {}".format(e))
        finally:
            self._logger.info("leave _clean_street_character_clean")

    # def _clean_street_only_chinese(self):
    #     self._logger.info("enter _clean_street_only_chinese")

    #     org_key = 'tags.addr:street'
    #     tar_key = 'address.street'

    #     try:
    #         self._log_count(org_key, True)
    #         self._log_count(tar_key, True)
            
    #         cursor = mongoCollection.find({org_key: {'$exists': True}})



    #         for doc in cursor:
    #             street_name = doc['tags']['addr:street']

    #             cn_street = ''.join(ChineseFilter.findall(street_name))
                
    #             if cn_street == street_name:
    #                 # all chinese
    #                 self._rename_one_record(doc['_id'], org_key, tar_key)
    #                 self._logger.info("rename chinese {}".format(doc['_id']))
    #             elif re.search('^S\d+$', street_name):
    #                 # S308
    #                 self._rename_one_record(doc['_id'], org_key, tar_key)
    #                 self._logger.info("rename SXXX {}".format(doc['_id']))

                

    #     except Exception as e:
    #         self._logger.error("_clean_street_only_chinese {}".format(e))
    #     finally:
    #         self._log_count(org_key, False)
    #         self._log_count(tar_key, False)
    #         self._logger.info("leave _clean_street_only_chinese")


    def _clean_housenumber_charactor_clean(self):
        db_key = 'address.housenumber'
        cursor = mongoCollection.find({db_key: {'$exists': True}})

        for doc in cursor:
            housenumber = doc['address']['housenumber']

            new_housenumber = housenumber.replace('號','号').replace('橋', '桥').replace('～', '-').replace('~', '-')

            if new_housenumber != housenumber:
                mongoCollection.update_one({'_id': doc['_id']}, {'$set' : {db_key: new_housenumber}})
        
    def clean_special_housenumber(self):
        ''' 
        use new db_key 
            housenumber,housename,floor,interpolation
        others use tags. db_key
        '''
        self._logger.info("enter clean_special_housenumber")
        # http://wiki.openstreetmap.org/wiki/Addresses#Using_interpolation
                
        def interpolation_check(outer):
            outer._logger.info("enter interpolation_check")
            cursor = mongoCollection.find({'tags.addr:housenumber': {'$exists': True},
                                            'tags.addr:interpolation':{'$exists': False}})

            filter_sharp_hao = '^(\d+)[号|#](\d+号{0,1})$'
            try:
                for doc in cursor:
                    housenumber = doc['tags']['addr:housenumber']
                    
                    if re.search('^(\d+-\d+)$', housenumber):
                        outer._rename_one_record(doc['_id'], 'tags.addr:housenumber', 'tags.addr:interpolation')
                    elif re.search('^(\d+[~|～]\d+)$', housenumber):
                        # 98~114
                        # 204;203 不处理
                        housenumber.replace('~', '-').replace('～', '-')
                        outer._rename_one_with_content_change(doc['_id'], 'tags.addr:housenumber', 'tags.addr:interpolation', housenumber)
                    elif re.search(filter_sharp_hao, housenumber):
                        # 106#305
                        
                        (housenumber,interpolation) = re.findall(filter_sharp_hao, housenumber)[0]
                        housenumber = housenumber + '号'
                        if 'addr:interpolation' in doc['tags']:
                            outer._logger.error("interpolation_check check interpolation {}".format(doc['_id']))
                            continue
                        outer._set_unset_one_record(doc['_id'], 
                                                        {'tags.addr:housenumber' : housenumber,
                                                         'tags.addr:interpolation' : interpolation})
            except Exception as e:
                outer._logger.info("interpolation_check {}".format(e))
            
            finally:
                outer._logger.info("leave interpolation_check")
        def move_door(outer):
            outer._logger.info("enter move_door")
            
            # 104边门
            # 337北门
            # 605弄后门
            # 65边门
            # 337南门
            
            try:
                cursor = mongoCollection.find({'tags.addr:housenumber': {'$exists': True},
                                            'tags.addr:door':{'$exists': False}})
                door_filter = re.compile('^(\d+弄{0,1})([边|北|后|南]门)$')

                for doc in cursor:
                    org_housenumber = doc['tags']['addr:housenumber']

                    if door_filter.search(org_housenumber):
                        (housenumber, door) = door_filter.findall(org_housenumber)[0]
                        self._set_unset_one_record(doc['_id'], 
                                                    {'tags.addr:housenumber' : housenumber,
                                                    'address.door' : door},
                                                    {})
            except Exception as e:
                outer._logger.info("move_door {}".format(e))
            
            finally:
                outer._logger.info("leave move_door")


        interpolation_check(self)
        move_door(self)

        # 搜索条件 冗余, 保证 修改后再调用，不会出错
        mongoCollection.update_one(            
            {'id': '5183134521', 'tags.addr:housenumber': '中国上海市嘉定区仓场路260号'},
            {'$set': {'address.housenumber': '260号',
                    'tags.addr:country': 'CN',
                    'tags.addr:city': '上海',
                    'tags.addr:district': '嘉定'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one(            
            {'id': '4755897528', 'tags.addr:housenumber': '473号新华联购物中心LG1层'},
            {'$set': {'address.housenumber': '473号',
                    'address.housename': '新华联购物中心',
                    'address.floor': 'G1层'},
             '$unset': {'tags.addr:housenumber': ''}})

        # not sure 1 is number or 87 is number
        mongoCollection.update_one({'id': '4535125890', 'tags.addr:housenumber': '129弄1号A座10G'},
            {'$set': {'address.street_alley': '129弄',
                    'address.housenumber': '1号',
                    'address.interpolation': 'A座10G'},
             '$unset': {'tags.addr:housenumber': ''}})

        # 数据异常，删除
        mongoCollection.update_one({'id': '4489531989', 'tags.addr:housenumber': '361500081号'},
            {'$unset': {'tags.addr:housenumber': ''}})

        # name，删除
        mongoCollection.update_one({'id': '1972114974', 'tags.addr:housenumber': 'U2cake'},
            {'$unset': {'tags.addr:housenumber': ''}})

        # postcode，删除
        mongoCollection.update_one({'id': '4623571655', 'tags.addr:housenumber': '200001'},
            {'$unset': {'tags.addr:housenumber': ''}})


        # 删除
        mongoCollection.update_one({'id': '4774726522', 'tags.addr:housenumber': '人马街002B'},
            {'$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4729252490', 'tags.addr:housenumber': 'Lane 637, 24'},
            {'$set': {'address.street_alley': '637弄',
                    'address.street_alley_en': 'Lane 637',
                    'address.housenumber': '24',
                    'address.housenumber_en': '24'},
             '$unset': {'tags.addr:housenumber': ''}})

        # 200031,postcode
        mongoCollection.update_one({'id': '524248625', 'tags.addr:housenumber': '200031'},
            {'$rename': {'tags.addr:housenumber': 'tags.addr:postcode'}})
        
        mongoCollection.update_one({'id': '4498698991', 'tags.addr:housenumber': '1, no. 11'},
            {'$set': {'address.housenumber': '11号',
                    'address.interpolation': '1'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4380214489', 'tags.addr:housenumber': '652号F座'},
            {'$set': {'address.housenumber': '652号',
                    'address.interpolation': 'F座'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '5244409321', 'tags.addr:housenumber': '1F'},
            {'$set': {'address.floor': '1楼'},
             '$unset': {'tags.addr:housenumber': ''}})
        mongoCollection.update_one({'id': '5056454922', 'tags.addr:housenumber': '5th floor'},
            {'$set': {'address.floor': '5楼'},
             '$unset': {'tags.addr:housenumber': ''}})


        mongoCollection.update_one({'id': '3406878629', 'tags.addr:housenumber': 'D106, 1733'},
            {'$set': {'address.interpolation': 'D106',
                    'address.housenumber':'1733'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4498699091', 'tags.addr:housenumber': '32 room w'},
            {'$set': {'address.interpolation': '西32室'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '449038540', 'tags.addr:housenumber': '119 Long No.18'},
            {'$set': {'address.street_alley': '119弄',
                    'address.housenumber': '18号'},
             '$unset': {'tags.addr:housenumber': ''}})

        # housename may need change
        mongoCollection.update_one({'id': '4591873289', 'tags.addr:housenumber': 'Bingo Plaza 345'},
            {'$set': {'address.housenumber': '345',
                    'tags.addr:housename:en': 'Bingo Plaza'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4458869191', 'tags.addr:housenumber': '3300号环球港3楼'},
            {'$set': {'address.housenumber': '3300号',
                    'address.环球港': '3300号',
                    'address.floor': '3楼'},
             '$unset': {'tags.addr:housenumber': ''}})

        

        mongoCollection.update_one({'id': '4674827092', 'tags.addr:housenumber': '87 Villa #1'},
            {'$set': {'address.housenumber': '1号',
                    'address.housename': 'Villa',
                    'address.interpolation': '87'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '156158696', 'tags.addr:housenumber': '#990 昌平路'},
            {'$set': {'address.housenumber': '990号',
                    'tags.addr:street': '昌平路'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4311050789', 'tags.addr:housenumber': '283 4/F'},
            {'$set': {'address.housenumber': '283',
                    'address.floor': '4楼'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4971729321', 'tags.addr:housenumber': '300/3F/306'},
            {'$set': {'address.housenumber': '300',
                    'address.interpolation': '306',
                    'address.floor': '3楼'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4498699290', 'tags.addr:housenumber': '25, 1st floor'},
            {'$set': {'address.housenumber': '25',
                    'address.floor': '1楼'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4407599795', 'tags.addr:housenumber': '188号E座24-25楼'},
            {'$set': {'address.housenumber': '188号',
                    'address.floor': '24-25楼',
                    'address.interpolation': 'E座'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4426893993', 'tags.addr:housenumber': '江興東路1688號'},
            {'$set': {'address.housenumber': '1688号'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4331713690', 'tags.addr:housenumber': '528-N1 37'},
            {'$set': {'address.housenumber': '528号',
                    'address.interpolation': 'N1 37'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4772277522', 'tags.addr:housenumber': '91号天工艺苑6楼'},
            {'$set': {'address.housenumber': '91号',
                    'address.housename': '天工艺苑',
                    'address.floor': '6楼'},
             '$unset': {'tags.addr:housenumber': ''}})

        # "name" : "家",
        mongoCollection.update_one({'id': '3382778443', 'tags.addr:housenumber': '宝山区淞南三村64号'},
            {'$set': {'address.housenumber': '64号',
                    'address.housename': '淞南三村',
                    'tags.addr:district': '宝山区'},
             '$unset': {'tags.addr:housenumber': '', 'tags.name': ''}})
            
        mongoCollection.update_one({'id': '4931727422', 'tags.addr:housenumber': '250号天街生活广场8幢508'},
            {'$set': {'address.housenumber': '250号',
                    'address.housename': '天街生活广场',
                    'address.interpolation': '8幢508'},
             '$unset': {'tags.addr:housenumber': ''}})
        
        mongoCollection.update_one({'id': '4426893992', 'tags.addr:housenumber': '浦东新区民夏路238号'},
            {'$set': {'address.housenumber': '238号',                    
                    'tags.addr:district': '浦东新区'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '1514013444', 'tags.addr:housenumber': '德清'},
            {'$set': {'address.subdistrict': '德清县'},
             '$unset': {'tags.addr:housenumber': ''}})            

        mongoCollection.update_one({'id': '4125278703', 'tags.addr:housenumber': '567 晶釆世纪大厦15层'},
            {'$set': {'address.housename': '晶釆世纪大厦',
                    'address.floor' : '15楼',
                    'address.housenumber':'567'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4448673790', 'tags.addr:housenumber': '666号南区66-68轴'},
            {'$set': {'address.housenumber': '666号',            
                    'address.interpolation': '南区66-68轴'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '5037599826', 'tags.addr:housenumber': '199弄中芯花园17号'},
            {'$set': {'address.street_alley': '199弄',
                    'address.housename': '中芯花园',
                    'address.housenumber': '17号'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4882823359', 'tags.addr:housenumber': 'Lane 1'},
            {'$set': {'address.street_alley': '1弄'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '3609090494', 'tags.addr:housenumber': '20th lane, 155'},
            {'$set': {'address.street_alley': '20弄',
                    'address.housenumber': '155号'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '2556666430', 'tags.addr:housenumber': 'Suite 1607-1609, I'},
            {'$set': {'address.housenumber': '106号',
                    'tags.addr:street': 'Zhongjiang Road',
                    'address.interpolation': 'Suite 1607-1609, I'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '348539937', 'tags.addr:housenumber': '浦泉路399弄，闵行浦泉路399弄，江柳路1号'},
            {'$set': {'address.housenumber': '1号',
                    'address.street_alley': '浦泉路399弄',
                    'tags.addr:country': 'CN',
                    'tags.addr:city': '上海',
                    'tags.addr:street': '江柳路',
                    'tags.addr:district': '闵行区'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one(
            {'id': '5164631431', 'tags.addr:housenumber': '澄浏中路3172号', 'tags.addr:street': '仓城东路'},
            {'$set': {'address.street': '仓城东路,澄浏中路',
                    'address.housenumber': '澄浏中路3172号'},
             '$unset': {'tags.addr:housenumber': '', 'tags.addr:street': ''}})

        mongoCollection.update_one(
            {'id': '4227727390', 'tags.addr:housenumber': '光复西路133弄1号楼'},
            {'$set': {'address.street_alley': '133弄',
                    'address.housenumber': '1号楼',
                    'tags.addr:street': '光复西路'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one(
            {'id': '4774748421', 'tags.addr:housenumber': '699号正大乐城三楼'},
            {'$set': {'address.housenumber': '699号',
                    'address.floor' : '3楼',
                    'address.housename': '正大乐城'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one(
            {'id': '3069756055', 'tags.addr:housenumber': 'No.600 Unit A6'},
            {'$set': {'address.housenumber': '600号',
                    'address.housenumber:en': 'No. 600',
                    'address.interpolation_en' : 'Unit A6',
                    'address.interpolation': 'A6单元'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one(
            {'id': '4406360489', 'tags.addr:housenumber': '1601号•越洋广场5楼'},
            {'$set': {'address.housenumber': '1601号',
                    'address.floor' : '5楼',
                    'address.housename': '越洋广场'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one(
            {'id': '4405213390', 'tags.addr:housenumber': '2-8号兰生大厦1F'},
            {'$set': {'address.housenumber': '2-8号',
                    'address.floor' : '1楼',
                    'address.housename': '兰生大厦'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4397696178', 'tags.addr:housenumber': '279-1边门'},
            {'$set': {'address.housenumber': '279号',
                    'address.door': '1边门'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one({'id': '4398807861', 'tags.addr:housenumber': '一九〇弄9号'},
            {'$set': {'address.street_alley': '190弄',
                    'address.housenumber': '9号'},
             '$unset': {'tags.addr:housenumber': ''}})
       

        mongoCollection.update_many({'$and':[
                                            {'tags.addr:housenumber': '1111号，美罗城'},
                                            {'$or' : [{'id': '4417201189'}, {'id': '4417145289'}]}
                                            ]},
                                        {'$set': {'address.housenumber': '1111号',
                                                'address.housename': '美罗城'},
                                         '$unset': {'tags.addr:housenumber': ''}})
       
        mongoCollection.update_many({'$and':[
                                            {'tags.addr:housenumber': '6F'},
                                            {'$or' : [{'id': '4218570893'}, {'id': '4218586789'}]}
                                            ]},
                                        {'$set': {'address.floor' : '6楼'},
                                         '$unset': {'tags.addr:housenumber': ''}})

        
        mongoCollection.update_one({'id': '5036738021', 'tags.addr:housenumber': 'Floor 6'},
            {'$set': {'address.street_alley': '6楼'},
             '$unset': {'tags.addr:housenumber': ''}})

        mongoCollection.update_one(
            {'id': '4470940413', 'tags.addr:housenumber': '宜山路691弄入口'},
            {'$set': {'address.street_alley' : '宜山路691弄',
                    'address.door': '入口'},
             '$unset': {'tags.addr:housenumber' : ''}})

        mongoCollection.update_one(
            {'id': '375192463', 'tags.addr:housenumber': '宝塔路9号', 'tags.addr:street': '永阳街道'},
            {'$set': {'address.housenumber': '9号',
                     'tags.addr:street': '宝塔路',
                     'address.subdistrict': '永阳街道',
                     'tags.addr:city': '南京市',
                     'tags.addr:district': '溧水区'},
             '$unset': {'tags.addr:housenumber': '' }})
        

        self._logger.info("leave clean_special_housenumber")

    def _clean_housenumber_remove_character(self):
        self._logger.info("enter _clean_housenumber_remove_character")
        try:
            db_key = 'address.housenumber'

            cursor = mongoCollection.find({db_key: {'$exists': True}})

            for doc in cursor:
                housenumber = doc['address']['housenumber']

                # 'No. 100'
                # 'No.1138'
                # no,212
                filter_no = '^[N|n]o\. *(\d+)$'
                if re.search(filter_no, housenumber):
                    new_housenumber = re.findall(filter_no, housenumber)[0]
                    
                    mongoCollection.update_one({'_id' : doc['_id']},
                            {'$set': {db_key: '{}号'.format(new_housenumber), 
                                    "{}_en".format(db_key): 'No. {}'.format(new_housenumber)}})

       
        except Exception as e:
            self._logger.error("_clean_housenumber_remove_character {}".format(e))
        finally:
            self._logger.info("leave _clean_housenumber_remove_character")

    def _clean_housenumber_with_alley(self):
        self._logger.info("enter _clean_housenumber_with_alley")
        try:
            housenumber_key = 'address.housenumber'
            alley_key = 'address.street_alley'
            interpolation_key = 'address.interpolation'

            # 7058弄
            # 3825弄
            alley_filter_00 = re.compile('^\d+弄$')

            # 2729弄802
            alley_filter_01 = re.compile('^(\d+弄)(\d+ *号{0,1})$')

            # 1292弄6-40号
            alley_filter_02 = re.compile('^(\d+弄)(\d+-\d+号)$')

            # 9弄14号204室, 3339弄7号210室
            # 2899弄2号605
            alley_filter_03 = re.compile('^(\d+弄)(\d+号)(\d+室{0,1})$')

            # 231弄9号楼
            alley_filter_04 = re.compile('^(\d+弄)(\d+号楼)$')

            # No 11, Lane 248
            alley_filter_05 = re.compile('^No +(\d+), +Lane +(\d+)$')

            # 闵驰二路58弄
            alley_filter_06 = re.compile('^([\u4e00-\u9fff]+路)(\d+弄)$')

            cursor = mongoCollection.find({housenumber_key: {'$exists': True},
                                            alley_key: {'$exists': False}})

            for doc in cursor:
                org_housenumber = doc['address']['housenumber']

                if alley_filter_00.search(org_housenumber):
                    # 7058弄

                    self._set_unset_one_record(doc['_id'], 
                                                    {alley_key : org_housenumber},
                                                    {housenumber_key: ''})
            
                elif alley_filter_01.search(org_housenumber):
                    # 2729弄802

                    (alley, housenumber) = alley_filter_01.findall(org_housenumber)[0]
                    housenumber = housenumber.replace(' ', '')

                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    alley_key : alley},
                                                    {})
                elif alley_filter_02.search(org_housenumber):
                    # 1292弄6-40号
                    (alley, housenumber) = alley_filter_02.findall(org_housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    alley_key : alley},
                                                    {})
                elif alley_filter_03.search(org_housenumber):
                    # 9弄14号204室, 3339弄7号210室

                    (alley,housenumber, interpolation) = alley_filter_03.findall(org_housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    alley_key : alley,
                                                    interpolation_key : interpolation},
                                                    {})
                elif alley_filter_04.search(org_housenumber):
                    # 231弄9号楼

                    (alley, housenumber) = alley_filter_04.findall(org_housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    alley_key : alley},
                                                    {})
                elif alley_filter_05.search(org_housenumber):
                    # No 11, Lane 248
                    (housenumber, alley) = alley_filter_05.findall(org_housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {alley_key : "{}弄".format(alley),
                                                    housenumber_key : "{}号".format(housenumber)},
                                                    {})
                elif alley_filter_06.search(org_housenumber):
                    # 闵驰二路58弄
                    (road_name, alley) = alley_filter_06.findall(org_housenumber)[0]
                    
                    if ('tags' in doc and 'addr:street' in doc['tags']) or \
                       ('address' in doc and 'street' in doc['address']):
                       self._logger.error("check street housenumber by hand {}".format(e))
                       continue
                    self._set_unset_one_record(doc['_id'], 
                                                    {alley_key : "{}弄".format(alley),
                                                    'address.street' : road_name},
                                                    {housenumber_key:''})
                
             

        except Exception as e:
            self._logger.error("_clean_housenumber_with_alley {}".format(e))
        finally:
            self._logger.info("leave _clean_housenumber_with_alley")

    def _clean_housenumber_with_interpolation(self):
        self._logger.info("enter _clean_housenumber_with_interpolation")
    
        try:
            housenumber_key = 'address.housenumber'
            interpolation_key = 'address.interpolation'
            
            
            # 132号10-12号
            interpolation_filter_01 = re.compile('^(\d+号)(\d+-\d+号)$')
            
            # 1230号甲一号
            interpolation_filter_02 = re.compile('^(\d+号)(甲一号)$')

            # 1201号T52-10
            interpolation_filter_03 = re.compile('^(\d+号)(T\d+-\d+)$')

            # 1068#
            # #1361
            # 901#
            interpolation_filter_21 = re.compile('^#{0,1}(\d+)#{0,1}$')


            # 123号5楼
            # 770 2层
            # 468号4F
            interpolation_filter_31 = re.compile('^(\d+[号 ])(\d+)[楼|层|F]$')

            # 760 - Floor 14
            interpolation_filter_32 = re.compile('^(\d+) *- *Floor *(\d+)$')

            # 123号6号楼5楼
            interpolation_filter_33 = re.compile('^(\d+号)(\d+号楼)(\d+楼)$')

            # 527 11/F
            interpolation_filter_34 = re.compile('^(\d+) +(\d+)/F$')


            # 22/404
            interpolation_filter_41 = re.compile('^(\d+)/(\d+)$')
            
            # 118, B2
            # 88/B1
            # 555 B1
            interpolation_filter_42 = re.compile('^(\d+)[,/ ] *B(\d+)$')

            # 705号101B
            # 5678号B13B
            interpolation_filter_43 = re.compile('^(\d+号)(B{0,1}\d+B)$')
            
            # 753, bld B
            interpolation_filter_44 = re.compile('^(\d+), +bld +(B)$')

            # 822 building 69
            interpolation_filter_45 = re.compile('^(\d+) +building +(\d+)$')

            # No. 20, 205            
            interpolation_filter_46 = re.compile('^No. +(\d+), +(\d+)$')

            # 155, #39
            interpolation_filter_51 = re.compile('^(\d+), +#(\d+)$')
            
            # 1201号-9
            interpolation_filter_52 = re.compile('^(\d+号)-(\d+)$')

            # 112号103室
            interpolation_filter_61 = re.compile('^(\d+号)(\d+室)$')
            
            # no,212
            interpolation_filter_13 = re.compile('^no,(\d+)$')

            
            cursor = mongoCollection.find({housenumber_key: {'$exists': True},
                                            interpolation_key: {'$exists': False}})            
            for doc in cursor:
                housenumber = doc['address']['housenumber']
                
                if interpolation_filter_01.search(housenumber):
                    # 132号10-12号
                    (housenumber, interpolation) = interpolation_filter_01.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : interpolation},
                                                    {})                                
                elif interpolation_filter_02.search(housenumber):
                    # 1230号甲一号
                    (housenumber, interpolation) = interpolation_filter_02.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : interpolation},
                                                    {})
                elif interpolation_filter_03.search(housenumber):
                    # 1201号T52-10
                    (housenumber, interpolation) = interpolation_filter_03.findall(housenumber)[0]

                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : interpolation},
                                                    {})
                elif interpolation_filter_21.search(housenumber):
                    # 1068#
                    # #1361
                    housenumber = interpolation_filter_21.findall(housenumber)[0]
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : "{}号".format(housenumber)},
                                                    {})
                
                elif interpolation_filter_31.search(housenumber):
                    # 123号5楼
                    # 770 2层
                    # 468号4F

                    if 'addr:floor' in doc['tags'] or 'floor' in doc['address']:
                        self._logger.error("check housenumber and floor {}".format(doc['_id']))
                        continue

                    (housenumber, floor) = interpolation_filter_31.findall(housenumber)[0]
                    housenumber = housenumber.strip()
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    'address.floor' : "{}楼".format(floor)},
                                                    {})
                elif interpolation_filter_32.search(housenumber):
                    # 760 - Floor 14
                    if 'addr:floor' in doc['tags'] or 'floor' in doc['address']:
                        self._logger.error("check housenumber and floor {}".format(doc['_id']))
                        continue

                    (housenumber, floor) = interpolation_filter_32.findall(housenumber)[0]
                    housenumber = housenumber.strip()

                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    'address.floor' : '{}楼'.format(floor)},
                                                    {})
                elif interpolation_filter_33.search(housenumber):
                    # 123号6号楼5楼
                    if 'addr:floor' in doc['tags'] or 'floor' in doc['address']:
                        self._logger.error("check housenumber and floor {}".format(doc['_id']))
                        continue

                    (housenumber, interpolation,floor) = interpolation_filter_33.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : interpolation,
                                                    'address.floor' : floor},
                                                    {})

                elif interpolation_filter_34.search(housenumber):
                    # 527 11/F
                    if 'addr:floor' in doc['tags'] or 'floor' in doc['address']:
                        self._logger.error("check housenumber and floor {}".format(doc['_id']))
                        continue

                    (housenumber, floor) = interpolation_filter_34.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    'address.floor' : '{}楼'.format(floor)},
                                                    {})

                elif interpolation_filter_41.search(housenumber):
                    # 22/404
                    (housenumber, interpolation) = interpolation_filter_41.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : interpolation},
                                                    {})
                
                elif interpolation_filter_42.search(housenumber):
                    # 118, B2
                    (housenumber, interpolation) = interpolation_filter_42.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : "{}栋".format(interpolation)},
                                                    {})

                elif interpolation_filter_43.search(housenumber):
                    # 705号101B
                    (housenumber, interpolation) = interpolation_filter_43.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : interpolation},
                                                    {})
                    
            
                elif interpolation_filter_44.search(housenumber):
                    # 753, bld B
                    (housenumber, interpolation) = interpolation_filter_44.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : "{}栋".format(interpolation)},
                                                    {})
                elif interpolation_filter_45.search(housenumber):
                    # 822 building 69
                    (housenumber, interpolation) = interpolation_filter_45.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : "{}栋".format(interpolation)},
                                                    {})

                    
             
                elif interpolation_filter_46.search(housenumber):
                    # No. 20, 205
                    (housenumber, interpolation) = interpolation_filter_46.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : "{}号".format(housenumber),
                                                    interpolation_key : interpolation},
                                                    {})
                elif interpolation_filter_52.search(housenumber):
                    # 1201号-9
                    (housenumber, interpolation) = interpolation_filter_52.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : interpolation},
                                                    {})

            
                elif interpolation_filter_51.search(housenumber):
                    # 155, #39
                    (interpolation, housenumber) = interpolation_filter_51.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : "{}号".format(housenumber),
                                                    interpolation_key : interpolation},
                                                    {})
                
                elif interpolation_filter_61.search(housenumber):
                    # 112号103室
                    (interpolation, housenumber) = interpolation_filter_61.findall(housenumber)[0]
                    
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : housenumber,
                                                    interpolation_key : interpolation},
                                                    {})
                elif interpolation_filter_13.search(housenumber):
                    # no,212
                    housenumber = interpolation_filter_13.findall(housenumber)[0]
                    self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : "{}号".format(housenumber)},
                                                    {})            
                else:
                    # 181   号
                    new_housenumber = re.sub(' +', ' ', housenumber)
                    new_housenumber = re.sub(' +号', '号', new_housenumber)
                    
                    # 6、7号
                    new_housenumber = re.sub('、 *', '-', new_housenumber)

                    if new_housenumber != housenumber:
                        self._set_unset_one_record(doc['_id'], 
                                                    {housenumber_key : new_housenumber},
                                                    {})

        except Exception as e:
            self._logger.error("_clean_housenumber_with_interpolation {}".format(e))
        finally:            
            self._logger.info("leave _clean_housenumber_with_interpolation")

    def _clean_housenumber_with_street_alley(self):
        self._logger.info("enter _clean_housenumber_with_street_alley")
        db_key = 'address.housenumber'
        try:
            # 航头路1528弄8号
            street_alley_filter = re.compile('^([\u4e00-\u9fff]+路)(\d+弄)(\d+号){0,1}$')

            cursor = mongoCollection.find({db_key: {'$regex': street_alley_filter},
                                              'tags.addr:street': {'$exists': False},
                                              'address.street': {'$exists': False},
                                              'tags.addr:housenumber': {'$exists': False}})

            for doc in cursor:
                org_housenumber = doc['address']['housenumber']

                # 航头路1528弄8号
                (street_name, alley, housenumber) = street_alley_filter.findall(org_housenumber)[0]

                set_dict = {'address.street': street_name,
                              'address.street_alley': alley}

                if len(housenumber) > 0:
                    set_dict['address.housenumber'] = housenumber

                self._set_unset_one_record(doc['_id'],
                                                set_dict,
                                                {})
                self._logger.info("update {}".format(doc['_id']))

        except Exception as e:
            self._logger.error("_clean_housenumber_with_street_alley {}".format(e))
            raise e
        finally:            
            self._logger.info("leave _clean_housenumber_with_street_alley")

    def _clean_housenumber_with_street_org_street(self):
        self._logger.info("enter _clean_housenumber_with_street_org_street")
    
        try:
            db_key = 'address.housenumber'

            # 阊胥路88号
            street_housenumber_filter = re.compile('^([\u4e00-\u9fff]+路)(\d+ *号)$')

            
            
            cursor = mongoCollection.find({'$and': [{db_key: {'$regex': street_housenumber_filter}},                                                      
                                                      {'$or': [{'tags.addr:street': {'$exists': True}},
                                                              {'address.street': {'$exists': True}}]}
                                                    ]})
                                              
                                              
            # http://wiki.openstreetmap.org/wiki/Addresses#Using_interpolation
            for doc in cursor:
                org_housenumber = doc['address']['housenumber']

                org_street = doc['tags']['addr:street'] if ('tags' in doc and 'addr:street' in doc['tags']) else ''

                if len(org_street) ==0:
                    org_street = doc['address']['street'] if ('street' in doc['address']) else ''

                if len(org_street) == 0:
                    self._logger.error("check street {}".format(doc['_id']))
                    continue

                # 交叉路
                if org_housenumber == '澄浏中路3172号':
                    continue
                
                (street_name, housenumber) = street_housenumber_filter.findall(org_housenumber)[0]
                housenumber = housenumber.replace(' ','')

                if org_street != street_name:
                    self._logger.error("check housenumber, street by hand{}".format(doc['_id']))
                    continue

                set_dict ={db_key: housenumber}
                unset_dict ={}
                
                if 'tags' in doc and 'addr:street' in doc['tags']:
                    set_dict['address.street'] = street_name
                    unset_dict['tags.addr:street'] = ''
                     
                self._set_unset_one_record(doc['_id'],set_dict,unset_dict)

        except Exception as e:
            self._logger.error("_clean_housenumber_with_street_org_street {}".format(e))
        finally:            
            self._logger.info("leave _clean_housenumber_with_street_org_street")
         
    def _clean_housenumber_with_street_no_org_street(self):
        self._logger.info("enter _clean_housenumber_with_street_no_org_street")
    
        try:
            db_key = 'address.housenumber'

            street_housenumber_filter = re.compile('^([\u4e00-\u9fff]+路)(\d+ *号)$')

            cursor = mongoCollection.find({db_key: {'$regex': street_housenumber_filter},
                                              'tags.addr:street': {'$exists': False},
                                              'address.street': {'$exists': False}})
            # http://wiki.openstreetmap.org/wiki/Addresses#Using_interpolation
            for doc in cursor:
                org_housenumber = doc['address']['housenumber']
                
                set_dict ={}

                (street_name, housenumber) = street_housenumber_filter.findall(org_housenumber)[0]
                housenumber = housenumber.replace(' ','')

                set_dict = {
                        db_key: housenumber,
                        'address.street': street_name
                    }
                     
                self._set_unset_one_record(doc['_id'],set_dict,{})
                    

        except Exception as e:
            self._logger.error("_clean_housenumber_with_street_no_org_street {}".format(e))
        finally:            
            self._logger.info("leave _clean_housenumber_with_street_no_org_street")
         
    def _check_housenumber_is_phone(self):
        db_key = 'address.housenumber'
        cursor = mongoCollection.find({db_key: {'$exists': True}})    
    
        for doc in cursor:
            housenumber = doc['address']['housenumber']
            if re.search('^\+86\d{11}', housenumber):
                if 'phone' in doc or ('tags' in doc and 'phone' in doc['tags']):
                    self._logger.error("_check_housenumber_is_phone need check {}".format(doc['_id']))
                    continue
                else:                                    
                    mongoCollection.update_one({'_id': doc['_id']},{'$set': {'phone': housenumber}, '$unset': {db_key:''}})
                    
                    self._logger.info("change housenumber for it's actual phone {}".format(doc['_id']))

    def _clean_housenumber_without_number(self):
        db_key = 'address.housenumber'

        cursor = mongoCollection.find({db_key: {'$exists': True}})    
    
        for doc in cursor:
            housenumber = doc['address']['housenumber']
            if not re.search('\d+', housenumber):
                
                if 'housename' not in doc['address'] and 'addr:housename' not in doc['tags']:
                    mongoCollection.update_one({'_id': doc['_id']},{'$set': {'address.housename': housenumber}, '$unset': {db_key:''}})
                else:
                    mongoCollection.update_one({'_id': doc['_id']},{'$unset': {db_key:''}})
                self._logger.info("unset housenumber for without number : {}".format(doc['_id']))

    

    def _clean_district(self):
        self._logger.info('enter _clean_district')
        district_en_cn_dict = {
            'pudong': '浦东新区',
            'luwan': '卢湾区',
            'jiading': '嘉定区',
            'hongkou': '虹口区',
            'putuo': '普陀区',
            'jinqiao': '金桥',
            'pu xi': '浦西',
            'nanxun': '南浔区',
            'hongqiao': '虹桥',
            'qingpu' : '青浦区',
            'minhang': '闵行区',
            'zhenhai': '镇海区'
        }
        district_cn_check_dict = {
            '徐汇': '徐汇区',
            '浦东': '浦东新区',
            '西湖': '西湖区',
            '嘉定': '嘉定区',
            '长宁': '长宁区'
         }
        try:
            org_db_key ='tags.addr:district'
            tar_db_cn_key = 'address.district'
            subdistrict_cn_key = 'address.subdistrict'
            tar_db_en_key = 'address.district_en'
            subdistrict_en_key = 'address.subdistrict_en'
            
            cursor = mongoCollection.find({org_db_key: {'$exists': True}})
            
           
            for doc in cursor:
                set_dict = {}
                unset_dict ={org_db_key:''}

                district_name = doc['tags']['addr:district']
                cn_district = ''
                en_district = ''
                    
                if ChineseFilter.search(district_name):
                    cn_district = district_name
                    if cn_district in district_cn_check_dict:
                        cn_district = district_cn_check_dict[cn_district]                    
                else:            
                    en_district = re.sub(' +district', '', district_name.lower())
                    cn_district = district_en_cn_dict.get(en_district, '')
                    
                # it's subdistrict
                if cn_district in self._subdistrict_cn_set:
                    if 'subdistrict' in doc['address'] :
                        self._logger.error("check subdistrict {}".format(doc['_id']))
                        continue
                    set_dict[subdistrict_cn_key] = cn_district
                    if len(en_district)>0:
                        set_dict[subdistrict_en_key] = en_district
                else:
                    set_dict[tar_db_cn_key] = cn_district
                    if len(en_district)>0:
                        set_dict[tar_db_en_key] = en_district

                if len(cn_district) == 0 :
                    self._logger.error("check cn: {} \t en: '{}', {}".format(cn_district, 
                                                                              en_district,
                                                                              doc['_id']))
                    raise Exception("Check district")
                
                mongoCollection.update_one({'_id': doc['_id']},{'$set': set_dict, '$unset': unset_dict})
                    
                self._logger.info("change district : {}".format(doc['_id']))
        except Exception as e:
            self._logger.error("_clean_district {}".format(e))
            raise e
        finally:
            self._logger.info('leave _clean_district')

    def _check_district_contain_city(self):
        '''run after _clean_district'''
        self._logger.info('enter _check_district_contain_city')
        try:
            db_cn_key = 'address.district'
            cursor = mongoCollection.find({db_cn_key:{'$exists': True}})
            for doc in cursor:
                district_name = doc['address']['district']
                if district_name == '上海':
                    mongoCollection.update_one({'_id': doc['_id']}, {'$unset': {db_cn_key:''}})

        except Exception as e:
            self._logger.error("_check_district_contain_city {}".format(e))
        finally:
            self._logger.info('leave _check_district_contain_city')
    
    def _check_chinese_city(self,city_name):
        city_name = re.sub('上 *海市{0,1}', '上海市', city_name)
        
        city_name = re.sub('浙江省{0,1}', '浙江省', city_name)
        city_name = re.sub('杭州市{0,1}', '杭州市', city_name)

        city_name = re.sub('宁波市{0,1}', '宁波市', city_name)
        city_name = re.sub('湖州市{0,1}', '湖州市', city_name)
        city_name = re.sub('嵊州市{0,1}', '嵊州市', city_name)
        city_name = re.sub('嘉兴市{0,1}', '嘉兴市', city_name)

        city_name = re.sub('靖江市{0,1}', '靖江市', city_name)
        city_name = re.sub('奉化市{0,1}', '奉化市', city_name)

        city_name = re.sub('潘家徙', '上虞市', city_name)
        city_name = re.sub('镇江市{0,1}', '镇江市', city_name)
        city_name = re.sub('扬州市{0,1}', '扬州市', city_name)
        city_name = re.sub('无锡市{0,1}', '无锡市', city_name)
        city_name = re.sub('苏州市{0,1}', '苏州市', city_name)
        
        city_name = re.sub('南京市{0,1}', '南京市', city_name)
        city_name = re.sub('泰兴市{0,1}', '泰兴市', city_name)
        city_name = re.sub('启东市{0,1}', '启东市', city_name)
        city_name = re.sub('南通市{0,1}', '南通市', city_name)

        city_name = re.sub('如皋市{0,1}', '如皋市', city_name)
        
        city_name = re.sub('金山[市|区]{0,1}', '金山区', city_name)
        city_name = re.sub('闵行[市|区]{0,1}', '闵行区', city_name)
        city_name = re.sub('松江[市|区]{0,1}', '松江区', city_name)
        
        # 特定名称，不改
        # city_name = re.sub('宝山城市', '宝山区', city_name)
        city_name = re.sub('浦江漕河泾', '浦江镇漕河泾', city_name)
        
        city_name = re.sub('吴江[市|区]{0,1}', '吴江区', city_name)
        city_name = re.sub('余杭[市|区]{0,1}', '余杭区', city_name)

        city_name = re.sub('[ ·]+', '', city_name)
        return city_name

    def _combine_address_city(self):
        '''
        '''
        self._logger.info('enter _combine_address_city')
        possible_city_tags = ['addr:city_1', 'addr:city', 'addr:city:en']
        
        self._log_multi_count(['tags.addr:city_1', 
                                  'tags.addr:city', 
                                  'tags.addr:city:en', 
                                  'address.city', 
                                  'address.city_en'], 
                                  True)

        try:
            cursor = mongoCollection.find({ '$or': [{'tags.addr:city_1' : {'$exists': True}}, 
                                                    {'tags.addr:city' : {'$exists': True}},
                                                    {'tags.addr:city:en' : {'$exists': True}}
                                               ]})
            for doc in cursor:
                if 'address' in doc and 'city' in doc['address']:                    
                    self._log_record(doc['_id'])
                    raise Exception("already have address and  city")
                city_name_cn_set = set()
                city_name_en_set = set()

                # for update mongoDB
                set_dict = {}
                unset_dict = {}

                for city_tag in possible_city_tags:
                    if city_tag in doc['tags']:
                        city_name = doc['tags'][city_tag]
                        
                        # set remove tags
                        unset_dict["tags.{}".format(city_tag)] = ""
                        
                        if ChineseFilter.search(city_name):                                                        
                            city_name = self._check_chinese_city(city_name)

                            city_name_cn_set.add(city_name)                            
                        else:
                            city_name = re.sub(' +city$', '',city_name.lower())
                            city_name = re.sub(' +town$', '',city_name.lower())
                            
                            city_name_en_set.add(city_name)

                # 处理特殊值
                if len(city_name_cn_set) == 0:
                    en_city = "".join(city_name_en_set)
                
                    # 上海
                    if en_city in ['pudong, shanghai', 'pudong district, shanghai', 
                                    'huinanzhen, pudong, shanghai', 'pu dong', 'pudong']:
                        if 'addr:district' in doc['tags']:
                            raise Exception('addr:district already exist for {}'.format(doc['_id']))
                        else:
                            set_dict['tags.addr:district'] = 'pudong'
                            city_name_en_set.clear()
                            city_name_en_set.add('shanghai')
                            city_name_cn_set.add(self._city_en_cn_dict['shanghai'])
                    elif en_city in ['minhang, shanghai, china', ]:
                        if 'addr:district' in doc['tags']:
                            raise Exception('addr:district already exist for {}'.format(doc['_id']))
                        else:
                            set_dict['tags.addr:district'] = 'minhang'
                            city_name_en_set.clear()
                            city_name_en_set.add('shanghai')
                            city_name_cn_set.add(self._city_en_cn_dict['shanghai'])
                    elif en_city =='west bdc, shanghai':
                        if 'name' in doc['tags']:
                            raise Exception('name already exist for {}'.format(doc['_id']))
                        else:
                            set_dict['tags.name'] = 'West BDC'
                            city_name_en_set.clear()
                            city_name_en_set.add('shanghai')
                            city_name_cn_set.add(self._city_en_cn_dict['shanghai'])
                    elif en_city in ['anting', 'shaghai']: 
                        city_name_en_set.clear()
                        city_name_en_set.add('shanghai')
                        city_name_cn_set.add(self._city_en_cn_dict['shanghai'])
                    # 南通
                    elif en_city == 'netda, nantong':
                        city_name_en_set.clear()
                        city_name_en_set.add('nantong')
                        city_name_cn_set.add(self._city_en_cn_dict['nantong'])   
                    # 苏州
                    elif en_city == 'suzhou sip (suzhou culture and arts center north gate)':
                        city_name_en_set.clear()
                        city_name_en_set.add('suzhou')
                        city_name_cn_set.add(self._city_en_cn_dict['suzhou'])   
                    # 无锡
                    elif en_city == 'hudai':
                        city_name_en_set.clear()
                        city_name_en_set.add('wuxi')
                        city_name_cn_set.add(self._city_en_cn_dict['wuxi'])   
                    # 杭州
                    elif en_city == 'hanzghou':
                        city_name_en_set.clear()
                        city_name_en_set.add('hangzhou')
                        city_name_cn_set.add(self._city_en_cn_dict['hangzhou'])
                    # 南浔
                    elif en_city in ['nanxun', 'nanxun, huzhou']:
                        if 'addr:district' in doc['tags']:
                            if doc['tags']['addr:district'].lower() != 'nanxun':
                                raise Exception("check city nanxun {}".format(doc['_id']))
                            unset_dict['tags.addr:district'] = ''
                        elif 'address' in doc and 'district' in doc['address']:
                            if doc['address']['district'].lower() != 'nanxun':
                                raise Exception("check city nanxun {}".format(doc['_id']))
                        
                        set_dict['address.district_en'] = 'nanxun'
                        set_dict['address.district'] = '南浔区'
                        city_name_en_set.clear()
                        city_name_en_set.add('huzhou')
                        city_name_cn_set.add(self._city_en_cn_dict['huzhou'])
                    elif en_city in self._city_en_cn_dict:
                        city_name_cn_set.add(self._city_en_cn_dict[en_city])
                # 
                if len(city_name_cn_set)>1 or \
                    len(city_name_en_set)> 1 or \
                    len(city_name_cn_set) == 0:
                
                    raise Exception("check city for {}".format(doc['_id']))
            
                
                if len(city_name_cn_set) == 1:
                    set_dict['address.city'] = "".join(city_name_cn_set)
                if len(city_name_en_set) == 1:
                    set_dict['address.city_en'] = "".join(city_name_en_set)
              
                mongoCollection.update_one({'_id': doc['_id']}, {'$set': set_dict, '$unset' : unset_dict})
            
                self._logger.info('{} address city changed'.format(doc['_id']))   
        except Exception as e:
            self._logger.error("_combine_address_city {}".format(e))
        finally:
            self._log_multi_count(['tags.addr:city_1', 
                                  'tags.addr:city', 
                                  'tags.addr:city:en', 
                                  'address.city', 
                                  'address.city_en'], 
                                  False)

            self._logger.info('leave _combine_address_city')

    def _check_city_only_num(self):
        # 城市名称全数字，删除        
        only_num_count = mongoCollection.count({'tags.addr:city' : {'$regex': "^\d+$"}})
        self._logger.info("before 城市名称全数字 num: {}".format(only_num_count))

        mongoCollection.update_many({'tags.addr:city' : {'$regex': "^\d+$"}},{'$unset': {'tags.addr:city':''}})
        
        only_num_count = mongoCollection.count({'tags.addr:city' : {'$regex': "^\d+$"}})
        self._logger.info("after 城市名称全数字 num: {}".format(only_num_count))

    def _check_city_cn_district(self):
        self._logger.info("enter _check_city_cn_district")
        # 
        try:            
            db_key = 'address.city'
            self._log_count(db_key, True)
            cursor = mongoCollection.find({db_key: {'$exists': True}})
            for doc in cursor:
                cn_city = doc['address']['city']
                if cn_city in self._district_cn_set:                    
                    org_district = doc['address']['district'] if 'district' in doc['address'] else ''
                    if len(org_district) == 0:
                        org_district = doc['tags']['addr:district'] if 'tags' in doc and 'addr:district' in doc['tags'] else ''
                    
                    if len(org_district) == 0:
                        self._rename_one_record(doc['_id'], db_key, 'address.district')
                    elif org_district == cn_city:
                        self._set_unset_one_record(doc['_id'],{},{db_key:''})
        except Exception as e:
            self._logger.info("_check_city_cn_district {}".format(e))
            raise e
        finally:
            self._log_count(db_key, False)
            self._logger.info("leave _check_city_cn_district")


    def _check_city_cn_subdistrict(self):
        self._logger.info("enter _check_city_cn_subdistrict")
        # 
        try:            
            db_key = 'address.city'
            self._log_multi_count([db_key,'tags.addr:subdistrict','address.subdistrict'], True)
       
            cursor = mongoCollection.find({db_key: {'$exists': True}})
            for doc in cursor:
                cn_city = doc['address']['city']
                if cn_city in self._subdistrict_cn_set:                    
                    org_subdistrict = doc['address']['subdistrict'] if 'subdistrict' in doc['address'] else ''
                    if len(org_subdistrict) == 0:
                        org_subdistrict = doc['tags']['addr:subdistrict'] if 'tags' in doc and 'addr:subdistrict' in doc['tags'] else ''
                    
                    if len(org_subdistrict) == 0:
                        self._rename_one_record(doc['_id'], db_key, 'address.subdistrict')
                    elif org_subdistrict == cn_city:
                        self._set_unset_one_record(doc['_id'],{},{db_key:''})
        except Exception as e:
            self._logger.info("_check_city_cn_subdistrict {}".format(e))
            raise e
        finally:
            self._log_multi_count([db_key,'tags.addr:subdistrict','address.subdistrict'], False)            
            self._logger.info("leave _check_city_cn_subdistrict")
        
    def _check_city_cn_content(self):
        self._logger.info("enter _check_city_cn_content")
        try:
            db_key = 'address.city'
            cursor = mongoCollection.find({db_key: {'$exists': True}})

            for doc in cursor:
                cn_city = doc['address']['city']

                set_dict = {}
                self._logger.info(cn_city)
                
                if re.search('^([\u4e00-\u9fff]+市)([\u4e00-\u9fff]+区)([\u4e00-\u9fff]+路)(\d+号{0,1}\d*){0,1}$', cn_city):
                    (city,district,street, housenumber) =re.findall('([\u4e00-\u9fff]+市)([\u4e00-\u9fff]+区)([\u4e00-\u9fff]+路){0,1}(\d+号{0,1}\d*){0,1}', cn_city)[0]
                    if len(district)>0 and ('district' in doc['address'] and doc['address']['district'] != district) or \
                        ('tags' in doc and 'addr:district' in doc['tags'] and doc['tags']['addr:district'] != district):
                        self._logger.error("check city,district {}".format(doc['_id']))
                        continue
                    if len(street)>0 and ('street' in doc['address'] and doc['address']['street'] != street) or \
                        ('tags' in doc and 'addr:street' in doc['tags'] and doc['tags']['addr:street'] != street):
                        self._logger.error("check city,street {}".format(doc['_id']))
                        continue
                    if len(housenumber)>0 and ('housenumber' in doc['address'] and doc['address']['housenumber'] != housenumber) or \
                        ('tags' in doc and 'addr:housenumber' in doc['tags'] and doc['tags']['addr:housenumber'] != housenumber):
                        self._logger.error("check city, housenumber {}".format(doc['_id']))
                        continue
                    set_dict[db_key] =city
                    set_dict['address.district'] = district
                    if len(street)>0:
                        set_dict['address.street'] = street
                    if len(housenumber)>0:
                        set_dict['address.housenumber'] = housenumber
                
                elif re.search('^([\u4e00-\u9fff]+省){0,1}([\u4e00-\u9fff]+市)([\u4e00-\u9fff]+区)([\u4e00-\u9fff]+街道){0,1}$', cn_city):
                    # 浙江省舟山市定海区盐仓街道
                    # 舟山市定海区
                    
                    (province, city, district, subdistrict) = re.findall('([\u4e00-\u9fff]+省){0,1}([\u4e00-\u9fff]+市)([\u4e00-\u9fff]+区)([\u4e00-\u9fff]+街道){0,1}', cn_city)[0]
                    if ('province' in doc['address'] and doc['address']['province'] != province) or \
                        ('tags' in doc and 'addr:province' in doc['tags'] and doc['tags']['addr:province'] != province):
                        self._logger.error("check 省,市,区,街道 province {}".format(doc['_id']))
                        continue
                    if ('district' in doc['address'] and doc['address']['district'] != district) or \
                        ('tags' in doc and 'addr:district' in doc['tags'] and doc['tags']['addr:district'] != district):
                        self._logger.error("check 省,市,区,街道district {}".format(doc['_id']))
                        continue
                    if ('subdistrict' in doc['address'] and doc['address']['subdistrict'] != subdistrict) or \
                        ('tags' in doc and 'addr:subdistrict' in doc['tags'] and doc['tags']['addr:subdistrict'] != subdistrict):
                        self._logger.error("check 省,市,区,街道 subdistrict {}".format(doc['_id']))
                        continue
                    if len(province) >0:
                        set_dict['address.province'] = province
                    if len(subdistrict) >0:
                        set_dict['address.subdistrict'] = subdistrict

                    set_dict['address.city'] = city
                    set_dict['address.district'] = district
                elif UnicodeAlphabetFilter.search(cn_city):
                    list_cn = re.findall('[ \u4e00-\u9fff]+', cn_city)
                    if len(list_cn)!=1:
                        self._logger.error("check 中文 city {}".format(doc['_id']))
                        continue
                    new_cn_city = list_cn[0].replace(' ', '')
                    if new_cn_city == cn_city:
                        continue
                    set_dict = {'address.city': new_cn_city}
                    en_city = re.sub('[ \u4e00-\u9fff]+', '', cn_city).strip().lower()
                    en_city = re.sub(' +', ' ', en_city)
                    if len(en_city)>0:
                        if 'city_en' in doc['address'] and doc['address']['city_en'].lower() !=en_city:
                            self._logger.error("check en city {}".format())
                            continue
                        set_dict['address.city_en'] = en_city
                if len(set_dict)>0:
                    self._set_unset_one_record(doc['_id'], set_dict, {})


        except Exception as e:
            self._logger.error("_check_city_cn_content {}".format(e))
            raise e
        finally:
            self._logger.info("leave _check_city_cn_content")
        
        

    def _check_postal_code(self):
        '''
        postcode 标签：
            postal_code
        '''
        self._log_multi_count(['tags.postal_code', 'address.postcode'], True)
        
        cursor = mongoCollection.find({ 'tags.postal_code' : {'$exists': True}})

        for doc in cursor:
            if 'addr:postcode' in doc['tags']:
                self._logger.error("not change, addr:postcode exist before clean {}".format(doc['_id']))
                continue
            mongoCollection.update_one({'_id': doc['_id']}, {'$rename': {'tags.postal_code':'address.postcode'}})            

            self._log_record(doc['_id'])
        self._log_multi_count(['tags.postal_code', 'address.postcode'], False)

    def _check_postcode(self):
        '''
        db.final.find({'address.postcode': {$exists: true}}, { 'address.postcode': 1, '_id': 0})
        '''
        db_key = 'address.postcode'
        
        address_postcode_num = mongoCollection.count({'address.postcode' :{'$exists': True}})
        self._logger.info("before address.postcode:{}".format(address_postcode_num))

        cursor = mongoCollection.find({ db_key : {'$exists': True}})

        for doc in cursor:
            post_code = doc['address']['postcode']
            if re.search('^\d{6}$', post_code):
                continue
            
            if post_code == '201315 上海':
                mongoCollection.update_one({'_id': doc['_id']}, {'$set': {'address.postcode':'201315', 'address.city':'上海'}})
                self._logger.info('201315 上海 after change')
                self._log_record(doc['_id'])
                continue
                
            postcode_list = re.findall('\d+', post_code)
            if not postcode_list or len(postcode_list[0]) != 6:
                self._logger.info('delete {} for postcode {}'.format(doc['_id'], post_code))

                mongoCollection.update_one({'_id': doc['_id']}, {'$unset' : {db_key: ''}})
                

        address_postcode_num = mongoCollection.count({'address.postcode' :{'$exists': True}})
        self._logger.info("after address.postcode:{}".format(address_postcode_num))

addressClean = AddressClean()