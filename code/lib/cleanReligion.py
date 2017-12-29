from lib.osmCommon import (
                                mongoCollection,
                                CodeCharFilter,
                                ChineseFilter,
                                UnicodeAlphabetFilter,
                                NumberFilter
                            )
from lib.osmBase import OsmBase

class ReligionClean(OsmBase):
    """
    origin key
        'religion',
        'religion_1',
        'religion_2',
        'religion_3',
        'religion:zh',
    final_key
        'religion'
    """
    def __init__(self):
        super(ReligionClean, self).__init__('clean_religion', 'clean_religion.log')
        
    def clean_religion(self):
        self._logger.info("enter clean_religion")
        org_db_key_list = ['religion','religion_1','religion_2','religion_3']
        tar_db_key = 'religion'
        find_or_query = []
        for item in org_db_key_list:
            find_or_query.append({'tags.{}'.format(item):{'$exists': True}})
        try:
            cursor = mongoCollection.find({'$or': find_or_query})

            for doc in cursor:
                religion_set = set()
                unset_dict = {}

                for org_db_key in org_db_key_list:
                    if org_db_key in doc['tags']:
                        religion_set.add(doc['tags'][org_db_key])
                        unset_dict['tags.{}'.format(org_db_key)] =''
                self._set_unset_one_record(doc['_id'],{'religion': list(religion_set)}, unset_dict)
                self._logger.info("update {}".format(doc['_id']))

        except Exception as e:
            self._logger.info("clean_religion {}".format(e))
        finally:
            self._logger.info("leave clean_religion")

religionClean = ReligionClean()