from lib.osmBase import OsmBase
from lib.osmCommon import (
                              mongoCollection,
                              CodeCharFilter,
                              ChineseFilter,
                              UnicodeAlphabetFilter,
                              NumberFilter
                            )

import re
import pprint

class NameClean(OsmBase):    
    '''
    related tags:
        name
        name_1
        name_2
    '''
    def __init__(self):
        super(NameClean, self).__init__('clean_name', 'clean_name.log')

    def clean_special_name(self):
        self._logger.info("enter clean_special_name")

        def hubin_intime(outer):
            outer._logger.info("enter hubin_intime")
            try:
                hubin_filter = '湖滨银泰in77 *([A-Z0-9]+区)'
            
                cursor = mongoCollection.find({'tags.name': {'$regex': hubin_filter}})

                for doc in cursor:
                    set_dict = {
                        'name': '湖滨银泰',
                        'name_en': 'HUBIN INTIME',
                        'address.housenumber': '77',
                        }
                    unset_dict = {'tags.name': ''}

                    org_name = doc['tags']['name']
                    
                    set_dict['address.interpolation'] = re.findall(hubin_filter, org_name)[0]

                    if 'name:en' in doc['tags']:
                        unset_dict['tags.name:en'] = ''

                    outer._set_unset_one_record(doc['_id'], set_dict, unset_dict)
            except Exception as e:
                outer._logger.info("hubin_intime {}".format(e))
            finally:
                outer._logger.info("leave hubin_intime")
            
        try:
            
            hubin_intime(self)

            # 35-37号
            # 11/29
            mongoCollection.update_many({'tags.name': {'$regex': "^\d+[-|~|/]\d+号{0,1}$"},
                                            'tags.addr:housenumber': {'$exists': False}},
                                            {'$rename': {'tags.name': 'address.housenumber'}})
            # 50-51号楼
            mongoCollection.update_many({'tags.name': {'$regex': "^\d+-\d+号楼$"},
                                            'tags.addr:housenumber': {'$exists': False}},
                                            {'$rename': {'tags.name': 'address.housenumber'}})

            animal_list = [ '豺', '狼','鹰','袋鼠',
                              '棕熊', '大羚羊', '东北虎','非洲狮',
                              '野驴', '野马','白虎','骆驼',
                              '鸵鸟','驼羊','角马','斑马',
                              '犀牛','长颈鹿','亚洲小爪水獭','亚洲象','火烈鸟']
            mongoCollection.update_many({'tags.name': {'$regex': '|'.join(map(lambda x: "^{}$".format(x),animal_list))},
                                            'tags.animal': {'$exists': False}},
                                            {'$rename': {'tags.name': 'tourism.animal'}})


            # 5 record
            # one do not have name
            mongoCollection.update_one({'tags.name_1': '双虹桥'},
                                            {'$unset': {'tags.name_1': ''},
                                             '$set': {'tags.name': '宝界双虹桥'}})

            mongoCollection.update_one({'id': '346833196', 'tags.name_1': '上海交通大学医学院附属仁济医院嘉定分院'},
                                            {'$set': {'address.full': '上海交通大学医学院附属仁济医院嘉定分院',
                                                    'address.city': '上海',
                                                    'address.district': '嘉定区'},
                                            '$unset': {'tags.name_1': ''}})

            mongoCollection.update_one({'id': '263913980', 'tags.name_1': 'Jaso-Tongji Reception Center'},
                                            {'$rename': {'tags.name_1': 'name_en',
                                                        'tags.name': 'name'}})
            mongoCollection.update_one({'tags.name_1': 'G104'},
                                            {'$unset': {'tags.name_1': ''}})

            mongoCollection.update_one({'tags.name_1': '海军诞生地纪念馆'},
                                            {'$unset': {'tags.name_1': ''}})

            mongoCollection.update_many({'tags.name_2': {'$exists': True}},
                                            {'$unset': {'tags.name_2': ''}})

            mongoCollection.update_one({'id': '26466690', 'tags.name': '玛雅酒吧 Maya Pub'},
                                            {'$set': {'name': '玛雅酒吧',
                                                     'name_en': 'Maya Pub'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '332056025', 'tags.name': '杭州最佳西方梅苑宾馆'},
                                            {'$set': {'name': '杭州最佳西方梅苑宾馆',
                                                     'name_en': 'Hangzhou Best Western Premier Hotel'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '477661623', 'tags.name:zh': '上海书城（福州路店）'},
                                            {'$set': {'name': '上海书城',
                                                     'name_en': 'Shanghai Book City',
                                                     'branch': '福州路店'},
                                            '$unset': {'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '1663981886', 'tags.name': '枫叶速8酒店 大连路店 Super8 Hotel'},
                                            {'$set': {'name': '枫叶速8酒店',
                                                     'name_en': 'Super 8 Hotel',
                                                     'branch': '大连路店'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4062601619', 'tags.name': '漫游童话时光 “Once Upon a Time” Adventure'},
                                            {'$set': {'name': '漫游童话时光',
                                                     'name_en': '"Once Upon a Time" Adventure'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4062772650', 'tags.name': '明日世界E 空间聚乐部 Club Destin-E'},
                                            {'$set': {'name': '明日世界E 空间聚乐部',
                                                     'name_en': 'Club Destin-E'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4064621301', 'tags.name': 'M大街购物廊 Avenue M Arcade'},
                                            {'$set': {'name': 'M大街购物廊',
                                                     'name_en': 'Avenue M Arcade'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '915535747', 'tags.name': '普陀山'},
                                            {'$set': {'name': '普陀山佛顶山'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4313843989', 'tags.name': 'Sky Ring'},
                                            {'$set': {'name': '摩天轮－大悦城',
                                                     'name_en': 'Sky Ring - Joy City Mall'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4327610689', 'tags.name': 'Fried Dumplings'},
                                            {'$set': {'name': '小杨生煎',
                                                     'name_en': 'Fried Dumplings',
                                                     'address.floor': '1楼'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4456305692', 'tags.name': 'RBCN'},
                                            {'$set': {'name': '博世上海',
                                                     'name_en': 'Bosch Shanghai'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4470997903', 'tags.name': 'Очень классная видовая'},
                                            {'$set': {'name_en': 'View from Vue Bar'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4471596890', 'tags.name': '新东坡宾馆'},
                                            {'$set': {'name': '新东坡宾馆',
                                                     'name_en': 'New Dongpo Hotel'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'$and': [{'tags.name': '浙江吴兴区织里镇永昌路 13-8-22 号'},
                                                    {'$or': [{'id': '4614196604'},{'id': '4614196604'}]}
                                                    ]},
                                            {'$set': {'address.province': '浙江省',
                                                     'address.district': '吴兴区',
                                                     'address.subdistrict': '织里镇',
                                                     'address.street': '永昌路',
                                                     'address.housenumber': '13-8-22号'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})

           

            mongoCollection.update_one({'id': '4761268855', 'tags.name': 'Apple 上海环贸 iapm'},
                                            {'$set': {'name': '苹果上海环贸店',
                                                     'name_en': 'Apple at Shanghai iapm'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})
            mongoCollection.update_one({'id': '4892920622', 'tags.name': 'Mei zi qimg'},
                                            {'$set': {'name': '梅子青酒店',
                                                     'name_en': 'Meiziqing Hotel'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '4911157722', 'tags.name:en': 'Mahdude江苏苏州市虎丘区苏州市虎丘茶花村昌华路8号'},
                                            {'$set': {'address.province': '江苏省',
                                                     'address.city': '苏州市',
                                                     'address.district': '虎丘区',
                                                     'address.subdistrict': '茶花村',
                                                     'address.street': '昌华路',
                                                     'address.housenumber': '8号'},
                                            '$unset': {'tags.name:en': ''}})

            mongoCollection.update_one({'id': '4913222321', 'tags.name:en': '江苏苏州市姑苏区城北西路1599号B6幢majlesi'},
                                            {'$set': {'address.province': '江苏省',
                                                     'address.city': '苏州市',
                                                     'address.district': '姑苏区',
                                                     'address.street': '城北西路',
                                                     'address.housenumber': '1599号',
                                                     'address.interpolation': 'B6幢'},
                                            '$unset': {'tags.name:en': ''}})

            mongoCollection.update_one({'id': '4913275222', 'tags.name:en': '江苏苏州市姑苏区城北西路1599号B6幢'},
                                            {'$set': {'address.province': '江苏省',
                                                     'address.city': '苏州市',
                                                     'address.district': '姑苏区',
                                                     'address.street': '城北西路',
                                                     'address.housenumber': '1599号',
                                                     'address.interpolation': 'B6幢'},
                                            '$unset': {'tags.name:en': ''}})

            mongoCollection.update_one({'id': '5042033125', 'tags.name:en': '南塘浜路117号'},
                                            {'$set': {'address.street': '南塘浜路',
                                                     'address.housenumber': '117号'},
                                            '$unset': {'tags.name:en': ''}})

            mongoCollection.update_one({'id': '5056454922', 'tags.name': 'Tang Gong'},
                                            {'$set': {'name': '唐宫海鲜坊',
                                                     'name_en': 'The Tang Palace Seafood Restaurant'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '34561346', 'tags.name': '金桥路 2622 弄'},
                                            {'$set': {'address.street': '金桥路',
                                                     'address.street_alley': '2622弄'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

          

            mongoCollection.update_one({'id': '244083553', 'tags.name:zh': '胜浦镇'},
                                            {'$rename': {'tags.name:zh': 'address.subdistrict'}})

            
            mongoCollection.update_one({'id': '244083558', 'tags.name:zh': '西山镇'},
                                            {'$unset': {'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '244084540', 'tags.name:zh': '崇安区'},
                                            {'$unset': {'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '343611248', 'tags.name': '秋水伊人'},
                                            {'$unset': {'tags.name': ''}})

            mongoCollection.update_one({'id': '266499577', 'tags.name': 'United Family'},
                                            {'$set': {'name': '上海和睦家医院',
                                                     'name_en': 'Shanghai United Family Hospital'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})

            mongoCollection.update_one({'id': '476068109', 'tags.name': '海欧饭店 seagull on the bund '},
                                            {'$set': {'name': '海欧饭店',
                                                     'name_en': 'Haiou Hotel'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})

            mongoCollection.update_one({'id': '658996791', 'tags.name': 'Shuiledong'},
                                            {'$set': {'name': '水乐洞',
                                                     'name_en': 'Shuiledong cave'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})

            mongoCollection.update_one({'id': '784179386', 'tags.name': 'Captain Hostel '},
                                            {'$set': {'name_en': 'Captain Hostel'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})

            mongoCollection.update_one({'id': '824530194', 'tags.name': '安亭北 North Anting'},
                                            {'$set': {'name': '安亭北',
                                                     'name_en': 'North Anting'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})

            mongoCollection.update_one({'id': '1070286782', 'tags.name': '新寺'},
                                            {'$set': {'name': '新寺镇'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': '',
                                                      'tags.name:zh': ''}})

            mongoCollection.update_one({'id': '838146982', 'tags.name': 'Hotel Nikko Shanghai'},
                                            {'$set': {'name_en': 'Hotel Nikko Shanghai'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})

            mongoCollection.update_one({'id': '1723245783', 'tags.name': '中国建设银行 嵊泗沙河路储蓄所 China Construction Bank'},
                                            {'$set': {'name': '中国建设银行 嵊泗沙河路储蓄所',
                                                    'name_en': 'China Construction Bank Shengsi Shahe Road Office'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})
            mongoCollection.update_one({'id': '4274865465', 'tags.name': '衡山·和集 The Mix Place'},
                                            {'$set': {'name': '衡山·和集',
                                                    'name_en': 'The Mix Place'},
                                            '$unset': {'tags.name': ''}})

            

            mongoCollection.update_one({'id': '156606654', 'tags.name': '1028弄1支弄5C'},
                                            {'$set': {'address.street_alley': '1028弄1支弄',
                                                    'address.housenumber': '5C'},
                                            '$unset': {'tags.name': ''}})

            mongoCollection.update_one({'id': '156606657', 'tags.name': '1028弄3支弄5D'},
                                            {'$set': {'address.street_alley': '1028弄3支弄',
                                                    'address.housenumber': '5D'},
                                            '$unset': {'tags.name': ''}})

            mongoCollection.update_one({'id': '156606657', 'tags.name': '1028弄3支弄5D'},
                                            {'$set': {'address.street_alley': '1028弄3支弄',
                                                    'address.housenumber': '5D'},
                                            '$unset': {'tags.name': ''}})

            mongoCollection.update_one({'id': '397782710', 'tags.name': '上海新发展亚太JW万豪酒店 Marriott hotel Changfeng park '},
                                            {'$set': {'name': '上海新发展亚太JW万豪酒店',
                                                    'name_en': 'JW Marriott Changfeng Park'},
                                            '$unset': {'tags.name': '',
                                                      'tags.name:en': ''}})

            mongoCollection.update_one({'id': '426713752', 'tags.name': '上海浦东软件园祖冲之园 Y1 座'},
                                            {'$set': {'address.housename': '上海浦东软件园祖冲之园',
                                                    'address.interpolation': 'Y1座'},
                                            '$unset': {'tags.name': ''}})

            mongoCollection.update_one({'id': '426713754', 'tags.name': '上海浦东软件园祖冲之园 Y2 座'},
                                            {'$set': {'address.housename': '上海浦东软件园祖冲之园',
                                                    'address.interpolation': 'Y2座'},
                                            '$unset': {'tags.name': ''}})

            mongoCollection.update_one({'id': '196079325', 'tags.name': '祖冲之路887弄88号'},
                                            {'$set': {'address.street_alley': '887弄',
                                                    'address.street': '祖冲之路',
                                                    'address.housenumber': '88号'},
                                            '$unset': {'tags.name': ''}})

            mongoCollection.update_one({'id': '293512914', 'tags.name': '大木桥路 20弄 14号'},
                                            {'$set': {'address.street_alley': '20弄',
                                                    'address.street': '大木桥路',
                                                    'address.housenumber': '14号'},
                                            '$unset': {'tags.name': ''}})

            mongoCollection.update_one({'id': '293512915', 'tags.name': '大木桥路 273弄 3号'},
                                            {'$set': {'address.street_alley': '273弄',
                                                    'address.street': '大木桥路',
                                                    'address.housenumber': '3号'},
                                            '$unset': {'tags.name': ''}})



        except Exception as e:
            self._logger.info("clean_special_name {}".format(e))
        finally:
            self._logger.info("leave clean_special_name")

    def clean_name(self):
        '''
        source db key
            'tags.name',
            'tags.name:zh',
            'tags.name:zh-CN',
            'tags.name:zh-simplified',
            'tags.name:en',
        '''
        self._logger.info("enter clean_name")
        
        try:
            org_db_full_key = ['tags.name',
                                'tags.name:zh',
                                'tags.name:zh-CN',
                                'tags.name:zh-simplified',
                                'tags.name:en',]
            org_db_key =[]
            query_dict_list =[]
            for item in org_db_full_key:
                query_dict_list.append({item:{'$exists': True}})
                org_db_key.append(item[len('tags.'):])

            cursor = mongoCollection.find({'$or': query_dict_list})

            for doc in cursor:
                name_set = set()

                set_dict = {}
                unset_dict = {}

                for item in org_db_key:
                    if item in doc['tags']:
                        unset_dict['tags.{}'.format(item)] = ''
                        name_set.add(doc['tags'][item])

                if name_set != {''}:
                    (cn_name, en_name) = self._clean_name_set(name_set)
                    
                    # if not cn_name and not en_name:
                    #     # remove all
                    #     self._logger.warning("check cn en name {}".format(doc['_id']))
                        
                    if cn_name:
                        set_dict['name'] = cn_name
                    if en_name:
                        set_dict['name_en'] = en_name
                self._set_unset_one_record(doc['_id'], set_dict, unset_dict)


        except Exception as e:
            self._logger.info("clean_name {}".format(e))
        finally:
            self._logger.info("leave clean_name")
    
    def _check_name_chinese(self, org_str):
        self._logger.info("enter clean_name")

        if not ChineseFilter.search(org_str):
            return False

        filter_list = [
            # 浦东T2 (机场环一线)
            # 浦东T2 (机场一线)
            # M1轮渡码头
            # S20 外环隧道方向
            # G50 湖州 上海市区方向/沪青平公路
            # G2 G42 苏州方向
            # 'S39 武进 泰州大桥 S38 泰州'
            '[TMGS]\d+',
            # T1-T2 摆渡车
            'T\d+-T\d+',
            # 泰兴X307/X207县道
            # 泰兴X303县道(根思-新街)
            '^泰兴X\d+|泰兴X\d+/X\d+',
            'Y\d+'
            # 朗盛（无锡）HPM工厂
            'HPM工厂',
            # 85度C面包店
            '85度C',
            # 中山soho广场A座
            'soho广场[A-Z]座',
            # 新城花园（A区）
            # 集美百货A店
            # D座-大学生创业园
            # 长江国际商务大厦A座
            # 中央车站(公交A岛)
            # 天堂软件园C幢
            # 无锡太湖国际博览中心A馆
            '[A-Z][区|店|座|岛|幢|馆]',
            # 湖州奔驰之星4S店
            '4S店',
            # 零点KTV视听歌城;百信手机连锁步行街旗舰店
            'KTV视听歌城',
            # 浦发ATM机
            'ATM机',
            # 公交BRT一号线六号大街二十三号路口自行车租赁点
            '公交BRT',
            # 地铁下沙西站B(地铁下沙西站西北角)自行车租赁点
            # 地铁九堡站C(杭海路九和路西南角)自行车租赁点
            # 地铁西兴路D(滨安路阡陌路口西北角)自行车租赁点
            '[站|路][A-Z]\([\u4e00-\u9fff]+\)',
            # 政苑小区B39号商铺自行车租赁点
            '[A-Z]\d+号',
            # CRE 中铁快 (铁路行包房)
            'CRE 中铁',                        
            # 西溪3D奇幻艺术馆
            '3D奇幻',            
            # 超妍美容SPA生活馆
            'SPA生活',
            # 中国电信天翼飞YOUNG营业厅
            'YOUNG营业厅',
            # 中国联通沃3G营业厅
            '3G营业厅',
            # 盒马鲜生Kings88店
            '盒马鲜生Kings\d+店',
            # 行知楼(K4)-生科院
            # 行建楼(K2)传说中的J4
            '\(K\d+\)[\u4e00-\u9fff\-]+(J\d+){0,1}',
            # 沈杜公路P+R停车场
            'P\+R停车场',
            # 红楼（B楼）
            # 城规学院C楼
            '[A-Z]楼',
            # 宝辰体育馆1km跑道
            '\d+km跑道',
            # 里木栅500KV超高压变电站
            '\d+KV超高压',
            # 上海电力公司大学220kV变电站
            # 110V变电站（规划）
            # 220KV龙东变电所
            '\d+[kK]V[\u4e00-\u9fff]|\d+V[\u4e00-\u9fff]',
            # 梅花苑50区A块
            '\d+区[A-Z]块',
            # 棕榈苑s8区
            's\d+区',
            # 杭州JW万豪酒店
            'JW万豪',
            # 服务区P匝道桥
            '服务区P',
            # 上海工程技术大学教学楼J301教室
            'J\d+教室',
            # 上海外滩 W 酒店
            'W {0,1}酒店',
            # 九亭U天地
            'U天地',
            # 宁波GQY视讯股份有限公司
            'GQY视讯',
            # 西大街G-27住宅
            'G-\d+住宅',
            # 天阳D32时尚街区 
            'D\d+时尚',
            # 超乐迪量贩式KTV
            # 金色年代ktv
            '[\u4e00-\u9fff]KTV',
            # 行敏楼(K1)
            # 厚生楼(B1)
            '[\u4e00-\u9fff]\([KB]\d+\)',
            # S半岛清水湾花园,
            'S半岛',
            

        ]

        for item in filter_list:
            temp = re.sub(item, '', org_str).strip()
            if not re.search('[a-zA-Z]',temp):                
                return True
        return False

    def _splict_name_en_cn(self,org_str):

        if not ChineseFilter.search(org_str) or not UnicodeAlphabetFilter.search(org_str):
            return (False,None)
                
        # 3号门 Gate 3
        # 1号门 Gate1
        gate_filter = '^(\d+号门) (Gate {0,1}\d+)$'
        if re.search(gate_filter, org_str):
            (cn, en) = re.findall(gate_filter, org_str)[0]
            en = en.strip()
            if len(en) == len('Gate') + 1:
              en = 'Gate {}'.format(en.replace('Gate', '').strip())
            return (True, [cn, en])

        # 105 县道 - 105 County Road
        country_road_filter = '^(\d+ 县道) - (\d+ County Road)$'
        if re.search(country_road_filter, org_str):
            (cn, en) = re.findall(country_road_filter, org_str)[0]
            return (True, [cn, en])

        # H（行政服务中心）
        # J（杭州市图书馆）
        # K（青少年发展中心）
        en_rm_filter = '^[H|J|K]\(([\u4e00-\u9fff]+)\)$'
        if re.search(en_rm_filter, org_str):
            cn = re.findall(en_rm_filter, org_str)[0]
            return (True, [cn, ''])

        cn_list = ChineseFilter.findall(org_str)
        if len(cn_list) ==1:
            cn_str = cn_list[0]
            en = ChineseFilter.sub('', org_str).strip()

            # 杭州百合花饭店 (Lily Hotel)
            if en[0] == '(' and en[-1] == ')':
                en = en[1:-1].strip()
            
            return (True, [cn_list[0], en])
        # 芦潮港-嵊泗 Luchaogang-Shengsi
        filter_dash = '[\u4e00-\u9fff]+-[\u4e00-\u9fff]+'
        if re.search(filter_dash, org_str):
            cn_dash_list = re.findall(filter_dash,org_str)
            if len(cn_dash_list) ==1:
                
                en = re.sub(filter_dash,'', org_str).strip()
                return (True, [cn_dash_list[0], en])

        # '蓝枪鱼(blue marlin)西餐'
        # # 地铁2号线(Line2)
        filter_en_brace = '\(([a-zA-Z 0-9]+)\)'
        en_brace_list = re.findall(filter_en_brace,org_str)
        if len(en_brace_list) == 1:
            cn = re.sub(filter_en_brace,'', org_str).strip()
            return (True, [cn,en_brace_list[0]])

        # 翱翔•飞越地平线 Soarin' Over the Horizon
        filter_dot = '[\u4e00-\u9fff]+·[\u4e00-\u9fff]+'
        if re.search(filter_dot, org_str):

            cn_dot_list = re.findall(filter_dot,org_str)
            if len(cn_dot_list) ==1:
                
                en = re.sub(filter_dot,'', org_str).strip()
                return (True, [cn_dot_list[0], en])

        
        

        return (False,None)
            

    def _clean_name_set(self, name_set):
        # self._logger.info("enter _clean_name_set")
        def check_double_quotation(org_str):
            if len(org_str)>=2 and org_str[0]== '"' and org_str[-1]=='"':
                return org_str[1:-1]
            else:
                return org_str

        def clean_name_charactor(org_str):
          rst = re.sub(' +', ' ', org_str).strip()
          # 汉森（Hanssem）家具
          rst = rst.replace('（', '(').replace('）', ')')
          # 松江大学城學生公寓五期B區
          rst = rst.replace('區','区').replace('學','学')
          # 中達電子吳江廠
          rst = rst.replace('達','达').replace('電','电').replace('吳','吴').replace('廠','厂')
          # 復興東路隧道 (下層)
          rst = rst.replace('復','复').replace('興','兴').replace('東','东').replace('層','层')
          # 上海十字腦科醫院
          rst = rst.replace('腦','脑').replace('醫','医')
          # 現代藝術基地
          # 现代艺术基地
          rst = rst.replace('現','现').replace('藝','艺').replace('術','术')
          # 翱翔•飞越地平线
          rst = rst.replace('•', '·')

          # 高 · 尚领域 A5
          rst = re.sub(' *· *', '·', rst)
          
          rst = re.sub(' */ *', '/', rst)

          # 金色年代ktv
          if re.search('[\u4e00-\u9fff]ktv', rst):
              rst = rst.replace('ktv', 'KTV')
          return rst
    
        try:
            cn_set = set()
            en_set = set()

            for item in name_set:

                # self._logger.warning("org {}".format(item))
                        
                item = clean_name_charactor(item)
                # self._logger.warning("after {}".format(item))
                
                if item == 'KFC':
                    return ('肯德基', 'KFC')
                elif item == 'HSBC银行' or item == 'HSBC Bank' or item == 'HSBC':
                    return ('汇丰银行', 'HSBC Bank')
                elif item == '世纪联华' or item == '联华':
                    return ('世纪联华', 'Lianhua')
                elif item.lower() == 'starbucks' or item == '星巴克' or re.search('星巴克 ',item):
                    return ('星巴克', "Starbuck's Coffee")
                elif item in ['上海.Volkswagen - 永达',
                               '东南.Mitsubishi - 汇胜', 
                               '一汽.Toyota - 和裕',
                               '广汽.Toyota - 晨隆',
                               '环贸 iapm',
                               'ATM',
                               'P',
                               'J', 'H', # subway_entrance
                               'Spa', # sauna
                               ]: 
                    cn_set.add(item)
                    continue

                elif re.search('^高·尚领域 [A-Z][0-9]$', item):
                    cn_set.add(item)
                    continue
                elif re.search('^[ACGHNYP]\d+$', item):
                    # Y105
                    # X17, X23;西23
                    # C12
                    # N2
                    # P1
                    # A4
                    # G5
                    # H5                    
                    cn_set.add(item)
                    continue
                elif re.search('^[A-Gc]$', item):
                    continue
                elif re.search('^C\d+/C\d+$', item):
                    # C23/C26
                    cn_set.add(item)
                    continue
                elif re.search('^P\d+-[A-Z]$', item):
                    # P3-A
                    cn_set.add(item)
                    continue

                elif item in ['我',
                               '85·',
                               '路',
                               '家',
                               '吧',
                               '戴伟',
                               # '民主',
                               # '侯',
                               # '玲',
                               'Q', 'yvvy',
                               '유신촨차이', '신세계백화점', '미니소',
                               'отель', 'ç',
                               'décathlon', 
                               'Yàn Yún Lóu', 'Yà Kè Xi Jiulóu', 'Wúmén Rénjiā',
                               'Häagen Dazs', 'Mòqiáng Line',
                               ]:
                    continue
                elif re.search('^[\d ]+$', item):
                    # pure number
                    # the meaning not sure, housenumber, bus number?
                    # so remove
                    continue
                elif re.search('^LCurve\d+_\d+$|^SCurve\d+$',item):
                    # LCurve1_1
                    # SCurve2
                    continue

                
                if not UnicodeAlphabetFilter.search(item):
                    cn_set.add(item)
                    continue
                elif ChineseFilter.search(item):
                    if re.search('^Apple [\u4e00-\u9fff]+$', item):
                        # Apple 环球港
                        item = re.sub('Apple {0,1}','苹果', item)
                        item = re.sub('店{0,1}$', '店', item)
                        cn_set.add(item)
                        continue

                    # Apple Store 零售店 西湖
                    if re.search('^Apple Store 零售店[\u4e00-\u9fff ]+', item):
                        item = re.sub('Apple Store {0,1}零售店','苹果零售店', item)
                        cn_set.add(item)
                        continue

                    if self._check_name_chinese(item):
                        cn_set.add(item)
                        continue
                    
                    (is_pass,rst_list) = self._splict_name_en_cn(item)
                    if is_pass:
                        cn_set.add(rst_list[0])
                        en_set.add(rst_list[1])
                        continue
                    else:
                        return ('','')
                
                else:
                    en_set.add(item)

            # real work need
            # if len(cn_set) >1 or len(en_set)>1:
            #     return ('','')

            # "Korean Restaurant"
            en_set = set(map(lambda x:check_double_quotation(x),en_set))

            en_set = set(map(lambda x:re.sub('^[\- /]+|[\- /]+$', '',x) ,en_set))

            # Jiangchuan Rd. (E.)
            en_set = set(map(lambda x:re.sub(' Lu$| Rd$| Rd.$| Rd. ', ' Road ',x) ,en_set))
            en_set = set(map(lambda x:x.strip(),en_set))

            en_set = set(map(lambda x:re.sub(' Rd./', ' Road/',x) ,en_set))

            # remove empty brace
            en_set = set(map(lambda x:re.sub('\( *\)', '',x) ,en_set))
            en_set = set(map(lambda x:x.strip(),en_set))

            en_set = set(map(lambda x:re.sub(' St$| St.$| Str$', ' Street',x) ,en_set))

            en_set = set(map(lambda x:re.sub(' Hwy.$', ' Highway',x) ,en_set))
            en_set = set(map(lambda x:re.sub(' Ave$', ' Avenue',x) ,en_set))

            cn_set = set(map(lambda x:x.strip(),cn_set))

            return (';'.join(cn_set), ';'.join(en_set))

        except Exception as e:
            self._logger.info("_clean_name_set {}".format(e))
            raise e
        finally:
            # self._logger.info("leave _clean_name_set")
            pass

    

nameClean = NameClean()