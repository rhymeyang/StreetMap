from lib.osmCommon import mongoCollection
from lib.osmBase import OsmBase


class SimpleClean(OsmBase):
    """clean tags without complicated logic"""
    def __init__(self):
        super(SimpleClean, self).__init__("simple_clean", 'simple_clean.log')
        
    def rename_created_by(self):
        '''
        db shell:
            db.final.find({'tags.created_by' : {$exists: true}}).count()
            db.final.find({'created.by' : {$exists: true}}).count()
        '''
        created_by_count = mongoCollection.count({'tags.created_by' :{'$exists': True}})
        by_count = mongoCollection.count({'created.by' :{'$exists': True}})
        self._logger.info("before change tags.created_by:{}".format(created_by_count))
        self._logger.info("before change created.by:{}".format(by_count))
                                        
        mongoCollection.update_many({'tags.created_by' : {'$exists': True}},{'$rename': {'tags.created_by':'created.by'}})
        
        created_by_count = mongoCollection.count({'tags.created_by' :{'$exists': True}})
        by_count = mongoCollection.count({'created.by' :{'$exists': True}})
        self._logger.info("after change tags.created_by:{}".format(created_by_count))
        self._logger.info("after change created.by:{}".format(by_count))
          
    def delete_abandoned_record(self):
        '''
        delete record with tag:
            abandoned
            abandoned:place
        '''
        abandoned_count = mongoCollection.count({'tags.abandoned' :{'$exists': True}})
        abandoned_place_count = mongoCollection.count({'tags.abandoned:place' :{'$exists': True}})
        self._logger.info("before delete tags.abandoned:{}".format(abandoned_count))
        self._logger.info("before delete ags.abandoned:place :{}".format(abandoned_place_count))
        
        mongoCollection.delete_many({'tags.abandoned' : {'$exists': True}})
        mongoCollection.delete_many({'tags.abandoned:place' : {'$exists': True}})

        abandoned_count = mongoCollection.count({'tags.abandoned' :{'$exists': True}})
        abandoned_place_count = mongoCollection.count({'tags.abandoned:place' :{'$exists': True}})
        self._logger.info("after delete tags.abandoned:{}".format(abandoned_count))
        self._logger.info("after delete ags.abandoned:place :{}".format(abandoned_place_count))

    def rename_only(self):
        
        rename_dict = {
            'tags.abutters' : 'abutters', 
            'tags.access':'access',
            'tags.aerialway': 'aerialway',
            'tags.aerialway:access': 'aerialway_access'
            
        }

        for org_key, tar_key in rename_dict.items():
            self._rename_only(org_key, tar_key)
        
        
 
simpleClean = SimpleClean()