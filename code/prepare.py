#!/bin/env python

import xml.etree.cElementTree as ET
import json

from lib.osmLog import streetLog
from lib.osmCommon import mongoOrgCollection

logger = streetLog("osmImport", 'importFixedOsm.log')


class osmImport(): 
    '''Parse OSM files'''
    def __init__(this):        
        this._collection = mongoOrgCollection
        this._collection.drop()

    def _insert(this,json_obj):
        try:
            this._collection.insert_one(json_obj)
        except Exception as e:
            logger.error("{}".format(e))
    
    def _set_element_attr(this, tag_type, ele_attr):
        '''set common attributes for node, way, relation'''
        try:
            rst_ele = {
                    'id' : ele_attr['id'],
                    'type' : tag_type,                    
                    'created' : {
                        'version' : ele_attr['version'],
                        'changeset' : ele_attr['changeset'],
                        'timestamp' : ele_attr['timestamp'],
                        'uid' : ele_attr['uid'],
                        'user' : ele_attr['user'],
                    }
                }

            if ele_attr.get("lat", None) and ele_attr.get("lon", None) :
                rst_ele['pos'] = [float(ele_attr['lat']), float(ele_attr['lon'])]
            else:
                if tag_type == 'node':
                    logger.info("{} id={} don't have position".format(tag_type,ele_attr['id']))

            if ele_attr.get("visible", None):
                rst_ele['visible'] = ele_attr['visible']
            
        except Exception as ex:
            logger.error("_set_element_attr {}\n{}".format(ex, json.dumps(ele_attr, indent=2)))
        else:
            return rst_ele
    

    def _add_tag(this, element, ele_obj):
        try:
            # set tag for node, way, relation
            tags_obj = {}   

            for tag_item in element.iter('tag'):
                key = tag_item.attrib['k']
                value = tag_item.attrib['v']
                
                if key in tags_obj:
                    raise Exception("tag key {} duplicate".format(key))
                tags_obj[key] = value

            if len(tags_obj) > 0:
                ele_obj['tags'] = tags_obj
                
        except Exception as e:
            logger.error("_add_tag ->{}".format(e))
            raise e
            

    def _add_nd(this, element, ele_obj):
        try:
            # set tag for node, way, relation
            nd_refs = []
            for nd_item in element.iter('nd'):
                attrib = nd_item.attrib
                nd_refs.append(attrib['ref'])
            if len(nd_refs) > 0:
                ele_obj['nd_refs'] = nd_refs
        except Exception as ex:
            logger.error("_add_nd ->{}".format(ex))

    def _add_member(this, element, ele_obj):
        try:
            members = [ ]
            for member_item in element.iter('member'):
                attrib = member_item.attrib
                members.append({
                    "type": attrib['type'],
                    "ref": attrib['ref'],
                    "role": attrib['role']
                })
            if len(members) > 0:
                ele_obj['members'] = members
        except Exception as ex:
            logger.error("_add_nd ->{}".format(ex))

    def insert_one(this, osmfile, id):
        '''this function only for test'''
        logger.info("start iter file {}".format(osmfile))
        
        find = False
        ele_obj= ""

        for _, element in ET.iterparse(osmfile, events =("start", "end")):
            # logger.info("check {}".format(element.tag))
            if element.tag in ["node", "way", "relation"] :
                 
                if not element.attrib:  
                    continue                     
                try:
                    # set basic info    
                    ele_obj = this._set_element_attr(element.tag, element.attrib)
                    
                    this._add_tag(element, ele_obj)

                    # set nd for way                
                    if element.tag == 'way':
                        this._add_nd(element, ele_obj)

                    # set member for relation                
                    if element.tag == 'relation':
                        this._add_member(element, ele_obj)
                        
                    if ele_obj['id'] == id:
                        find = True
                        this._insert(ele_obj) 
                except Exception as e:
                    logger.error("insert_one : {}".format(e))
                    break                
                finally:
                    element.clear()
                   

            
            if find:
                logger.info("insert_one  _id \n {}".format(ele_obj['_id']))
                ele_obj['_id'] = ""
                logger.info("insert_one \n {}".format(json.dumps(ele_obj, indent=2)))
                return
        
        
    def iter_file(this, osmfile):
        logger.info("start iter file {}".format(osmfile))
        
        test_count = 0

        for _, element in ET.iterparse(osmfile, events =("start", "end")):
            # logger.info("check {}".format(element.tag))
            # if element.tag in ["node", "way", "relation"] :
            if element.tag in ["node", "way"] :
                 
                if not element.attrib:  
                    continue                     
                
                # set basic info    
                ele_obj = this._set_element_attr(element.tag, element.attrib)
                
                this._add_tag(element, ele_obj)

                # set nd for way                
                if element.tag == 'way':
                    this._add_nd(element, ele_obj)

                # # set member for relation                
                # if element.tag == 'relation':
                #     this._add_member(element, ele_obj)
                    

                this._insert(ele_obj)    

                test_count += 1
                
            element.clear()
            if test_count%1000 == 0:
                logger.info("record count: {}".format(test_count))
        logger.info("final test_count: {}".format(test_count))

if __name__ == '__main__':
    osmImportObj = osmImport()
    osmImportObj.iter_file('./src/shanghai_china.osm')
    
    
    
