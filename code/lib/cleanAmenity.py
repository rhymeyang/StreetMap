from lib.osmBase import OsmBase
from lib.osmCommon import (
                              mongoCollection,
                            )


class AmenityClean(OsmBase):
    '''
    related tags:
        atm
        amenity
        bench
        bicycle
        bicycle_parking
        bus
        subway

      

    doc:
        http://wiki.openstreetmap.org/wiki/Key:backrest

        http://wiki.openstreetmap.org/wiki/Key:amenity
        http://wiki.openstreetmap.org/wiki/Pl:Key:building

        http://wiki.openstreetmap.org/wiki/Bicycle#Bicycle Restrictions
        http://wiki.openstreetmap.org/wiki/Naviki
            bicycle=private
    '''
    def __init__(self):
        super(AmenityClean, self).__init__("amenity_clean", "amenity_clean.log")

    def clean_simple_amenity(self):
        '''
        only rename key
        '''
        rename_dict = {
            'tags.bus' : 'bus',
            'tags.atm': 'atm',
            'tags.bench': 'bench',
            'tags.bicycle': 'bicycle',
            'tags.bicycle_parking': 'bicycle_parking',
            'tags.bicycle_road': 'bicycle_road',
            'tags.billiards:pool': 'billiards_pool',
            'tags.bin': 'bin',
            'tags.board_type': 'board_type',
            'tags.boat':'boat',
            'tags.bollard':'bollard',
            'tags.border_type': 'border_type',
            'tags.boundary':'boundary',
            'tags.branch':'branch',
            'tags.brand': 'brand',
            'tags.zoo': 'zoo',
            'tags.tourism': 'tourism',
            'tags.tourist_bus': 'tourist_bus',
            'tags.bridge:structure': 'bridge_structure',
        }

        for org_key, tar_key in rename_dict.items():
            self._rename_only(org_key, tar_key)

    def clean_special_amenity(self):
        self._logger.info("enter clean_special_amenity")
        try:
            mongoCollection.update_one({'id': '821865003', 'tags.amenity': '进才中学北校'},
                                            {'$rename': {'tags.amenity': 'name'},
                                             '$set': {'amenity': 'school'}})

            mongoCollection.update_one({'id': '1928678199', 'tags.amenity': '建平世纪中学'},
                                            {'$rename': {'tags.amenity': 'name'},
                                             '$set': {'amenity': 'school'}})

            mongoCollection.update_one({'id': '2346534410', 'tags.amenity': '联华超市'},
                                            {'$rename': {'tags.amenity': 'name'}})

            mongoCollection.update_many({'$and': [{'tags.amenity': 'Green Hills Gate'},
                                                    {'$or': [{'id': '71005318'},{'id': '400944452'}]}]},
                                            {'$rename': {'tags.amenity': 'name_en'}})
            mongoCollection.update_many({'tags.amenity': 'Bar'},
                                            {'$set': {'tags.amenity': 'bar'}})

            # http://wiki.openstreetmap.org/wiki/Key:amenity
            mongoCollection.update_one({'id': '558660231', 'tags.amenity': 'Centre commercial'},
                                            {'$set': {'building': 'commercial'},
                                            '$unset': {'tags.amenity': ''}})

            # http://wiki.openstreetmap.org/wiki/Tag:amenity%3Dbus_station
            mongoCollection.update_many({'$and': [{'tags.bus': 'destination'},
                                                    {'tags.amenity': {'$exists': False}},
                                                    {'$or': [{'id': '178002938'},{'id': '446272669'}]}]},
                                            {'$set': {'amenity': 'bus_station'},
                                             '$unset': {'tags.bus': ''}})


        except Exception as e:
            self._logger.info("clean_special_amenity {}".format(e))
        finally:
            self._logger.info("leave clean_special_amenity")

    def clean_special_bridge(self):
        self._logger.info("enter clean_special_bridge")
        try:
            # http://wiki.openstreetmap.org/wiki/Tag:bridge:structure%3Dsuspension
            # bridge 没有关键字 suspension
            # bridge:structure 有关键字 suspension
            mongoCollection.update_one({'tags.bridge': 'suspension', 
                                            'tags.bridge:structure': {'$exists': False}},
                                           {'$rename': {'tags.bridge': ' bridge_structure'}})

            # 没有关键字
            mongoCollection.update_many({'tags.bridge': 'abandoned'},
                                            {'$unset': {'tags.bridge':''}})
            mongoCollection.update_many({'tags.bridge': 'no'},
                                            {'$unset': {'tags.bridge':''}})
            # 没有关键字
            mongoCollection.update_many({'tags.bridge': 'agricultural'},
                                            {'$unset': {'tags.bridge':''}})
            mongoCollection.update_many({'tags.bridge': '1'},
                                            {'$unset': {'tags.bridge':''}})
            
        except Exception as e:
            self._logger.info("clean_special_bridge {}".format(e))
        finally:
            self._logger.info("leave clean_special_bridge")

    def clean_subway(self):
        self._logger.info("enter clean_subway")
        try:            
            cursor = mongoCollection.find({'tags.subway': 'yes'})
            for doc in cursor:
                self._rename_one_with_content_change(doc['_id'],'tags.subway', 'station', 'subway')
        except Exception as e:
            self._logger.info("clean_subway {}".format(e))
        finally:
            self._logger.info("leave clean_subway")

    def clean_amenity(self):
        self._logger.info("enter clean_amenity")
        try:

            org_db_key = 'tags.amenity'
            tar_db_key = 'amenity'

            # http://wiki.openstreetmap.org/wiki/Key:backrest
            mongoCollection.update_many({'$and': [{org_db_key: 'bench'},
                                                    {'$or': [{'tags.backrest': 'yes'},
                                                            {'tags.backrest': 'no'}]}
                                                    ]},
                                            {'$rename': {org_db_key: tar_db_key,
                                                'tags.backrest': 'backrest'}})

            mongoCollection.update_many({org_db_key:{'$exists': True},
                                            org_db_key:{'$not': {'$eq': 'bench'}}},
                                            {'$rename': {org_db_key: tar_db_key}})

        except Exception as e:
            self._logger.info("clean_amenity {}".format(e))
        finally:
            self._logger.info("leave clean_amenity")

    


amenityClean = AmenityClean()