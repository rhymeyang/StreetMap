from lib.osmLog import streetLog
from lib.osmCommon import mongoCollection
from bson.objectid import ObjectId
import json


class OsmBase(object):
    """base class for data handle"""
    def __init__(self, modle_name, log_file):
        super(OsmBase, self).__init__()
        self._logger = streetLog(modle_name, log_file)

    def _log_record(self,record_id):
        if isinstance(record_id,ObjectId) :
            id_obj = record_id 
        else:
            id_obj = ObjectId(record_id)
            
        doc = mongoCollection.find_one({'_id': id_obj}) 
        if doc:
            self._logger.info(record_id)
            del doc['_id']
            self._logger.info(json.dumps(doc, indent = 2))
        else:
            self._logger.warring("can not find {}".format(record_id))
    def _rename_one_record(self, doc_id, org_key,tar_key):
        id_obj = doc_id if isinstance(doc_id,ObjectId) else ObjectId(doc_id)
        mongoCollection.update_one({'_id': id_obj}, {'$rename': {org_key :tar_key}}) 

    def _set_unset_one_record(self, doc_id, set_dict, unset_dict= {}):
        id_obj = doc_id if isinstance(doc_id,ObjectId) else ObjectId(doc_id)
        if len(set_dict)>0 and len(unset_dict)>0:
            mongoCollection.update_one({'_id': id_obj},{'$set': set_dict, '$unset':unset_dict})
        elif len(set_dict) >0:
            mongoCollection.update_one({'_id': id_obj},{'$set': set_dict})
        elif len(unset_dict) >0:
            mongoCollection.update_one({'_id': id_obj},{'$unset': unset_dict})
        else:
            self._logger.error("_set_unset_one_record should not go here {}".format(doc['_id']))

    def _rename_only(self, org_key, tar_key):
        '''
        rename db key
        '''
        try:                        
            self._log_count(org_key, True)
            self._log_count(tar_key, True)
            mongoCollection.update_many({org_key : {'$exists': True}}, {'$rename': {org_key :tar_key}})

            self._log_count(org_key, False)
            self._log_count(tar_key, False)
        except Exception as e:
            self._logger.error('rename_only- org_key: {}, tar_key: {}, error: {}'.format(org_key,tar_key,e))

    def _rename_one_with_content_change(self, doc_id, org_key,tar_key, new_value):
        '''
        rename db key
        '''
        try:                        
            self._log_count(org_key, True)
            self._log_count(tar_key, True)
            mongoCollection.update_one({'_id': doc_id}, {'$set': {tar_key :new_value}, '$unset':{org_key: ''}})

            self._log_count(org_key, False)
            self._log_count(tar_key, False)
        except Exception as e:
            self._logger.error('_rename_one_with_content_change- org_key: {}, tar_key: {}, error: {}'.format(org_key,tar_key,e))
    def _log_count(self, db_key, is_before):
        comments = "before" if is_before else "after"
        record_count = mongoCollection.count({db_key :{'$exists': True}})
        self._logger.info("{} change {} : {}".format(comments, db_key, record_count))
    def _log_multi_count(self, key_list, is_before):
        for db_key in key_list:
            self._log_count(db_key, is_before)