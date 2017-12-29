from lib.osmBase import OsmBase
from lib.osmCommon import (
                              mongoCollection,
                            )

class TourismClean(OsmBase):
    """docstring for TourismClean"""
    def __init__(self):
        super(TourismClean, self).__init__('clean_tourism', 'clean_tourism.log')

    def clean_simple_tourism(self):
        '''
        only rename key
        '''
        rename_dict = {
            'tags.tourism' : 'tourism.tourism'
            
        }

        for org_key, tar_key in rename_dict.items():
            self._rename_only(org_key, tar_key)
                
    def clean_attraction(self):
        self._logger.info("enter clean_attraction")
        db_key = 'tags.attraction'
        new_key = 'tourism.attraction'

        attraction_dict = {
            'animal': '动物',
            'big_wheel': '摩天轮',
            'carousel': '旋转木马',
            'roller_coaster': '过山车'
        }

        try:
            cursor = mongoCollection.find({db_key: {'$exists': True},
                                              new_key: {'$exists': False}})

            for doc in cursor:
                org_attraction = doc['tags'] ['attraction']

                if org_attraction not in attraction_dict:
                    self._logger.error("check attraction {}".format(doc['_id']))
                    continue
                self._rename_one_with_content_change(doc['_id'],
                                                           db_key,
                                                           new_key,
                                                           attraction_dict[org_attraction])
        except Exception as e:
            self._logger.info("clean_attraction {}".format(e))
        finally:
            self._logger.info("leave clean_attraction")

    def clean_animal(self):
        self._logger.info("enter clean_animal")
        
        new_key = 'tourism.animal'

        mongoCollection.update_one({'tags.animals': 'black_swams'},
                                        {'$set': {new_key: '黑天鹅'},
                                        '$unset': {'tags.animals': ''}})

        db_key = 'tags.animal'
        

        animal_dict = {
            'snake': ' 蛇'          
        }

        try:
            cursor = mongoCollection.find({db_key: {'$exists': True},
                                              new_key: {'$exists': False}})

            for doc in cursor:
                org_animal = doc['tags'] ['animal']

                if org_animal in animal_dict:
                    animal=animal_dict[animal]

                self._rename_one_with_content_change(doc['_id'],
                                                           db_key,
                                                           new_key,
                                                           animal)
        except Exception as e:
            self._logger.info("clean_animal {}".format(e))
        finally:
            self._logger.info("leave clean_animal")
   
        
tourismClean = TourismClean()