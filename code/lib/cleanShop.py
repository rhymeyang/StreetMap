from lib.osmCommon import (mongoCollection,)
from lib.osmBase import OsmBase

class ShopClean(OsmBase):
    """
    origin key
        'shop',
        'shop_1',
        'shop_2',
        'shop_3',
    final_key
        'shop'
    """
    def __init__(self):
        super(ShopClean, self).__init__('clean_shop', 'clean_shop.log')
        
    def clean_shop(self):
        self._logger.info("enter clean_shop")
        org_db_key_list = ['shop','shop_1','shop_2','shop_3',]
        tar_db_key = 'shop'
        find_or_query = []
        for item in org_db_key_list:
            find_or_query.append({'tags.{}'.format(item):{'$exists': True}})

        try:
            cursor = mongoCollection.find({'$or': find_or_query})
            
            for doc in cursor:
                shop_set = set()
                unset_dict = {}

                for org_db_key in org_db_key_list:
                    if org_db_key in doc['tags']:
                        shop_set.add(doc['tags'][org_db_key])
                        unset_dict['tags.{}'.format(org_db_key)] =''
                self._set_unset_one_record(doc['_id'],{'shop': list(shop_set)}, unset_dict)
        except Exception as e:
            self._logger.info("clean_shop {}".format(e))
        finally:
            self._logger.info("leave clean_shop")
        

shopClean = ShopClean()