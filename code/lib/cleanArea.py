from lib.osmBase import OsmBase
from lib.osmCommon import (
                              mongoCollection,
                            )

class AreaClean(OsmBase):
    '''
    related tags:
        area
        area:highway
        highway

    doc:
        http://wiki.openstreetmap.org/wiki/Key:area
        http://wiki.openstreetmap.org/wiki/Proposed_features/Street_area

    correct:
         highway=* combined with area=yes

         area:highway=* only to add an area representation for linear highways
         where a way tagged highway=* without area=yes exists.

    incorrect:
         area:highway=* combined with area=yes
    '''
    def __init__(self):
        super(AreaClean, self).__init__('clean_area', 'clean_area.log')

    def clean_special_area(self):
        self._logger.info("enter clean_special_area")
        try:
            mongoCollection.update_one({'id': '166189596', 'tags.area': '亚光科技大厦'},
                                        {'$rename': {'tags.area': 'address.housename'}})

            mongoCollection.update_one({'tags.area:highway': 'yes'},
                                        {'$unset': {'tags.area:highway': ''}})

            error_area_count = mongoCollection.count({'tags.area': 'yes', 
                                                           'tags.highway' : {'$exists': False}})
            if error_area_count>0:
                self._logger.error("{} highway=* combined with area=yes not match".format(error_area_count))

        except Exception as e:
            self._logger.info("clean_special_area {}".format(e))
        finally:
            self._logger.info("leave clean_special_area")

    def clean_area(self):
        self._logger.info("enter clean_area")
        try:
            
            mongoCollection.update_many({'tags.area': 'yes', 'tags.highway' : {'$exists': True},
                                            {'$rename': {'tags.area' : 'area', 
                                                        'tags.highway' : 'highway' }
                                            }})

            # tags.area: highway
            # tags.highway: XXX
            # ->
            # area: yes
            # highway: XXX
            mongoCollection.update_many({'tags.area': 'highway', 'tags.highway' : {'$exists': True},
                                            {'$rename': {'tags.highway' : 'highway'},
                                             '$set': {'area': 'yes'},
                                             '$unset': {'tags.area': ''}
                                            }})
            
        except Exception as e:
            self._logger.info("clean_area {}".format(e))
        finally:
            self._logger.info("leave clean_area")
        
        