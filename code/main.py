#!/bin/env python

from lib.osmLog import streetLog

from lib.cleanSimple import simpleClean
from lib.cleanAddress import addressClean
from lib.cleanTourism import tourismClean
from lib.generalTools import generalTools
from lib.cleanAmenity import amenityClean
from lib.cleanName import nameClean
from lib.osmCommon import mongoCollection
from lib.cleanShop import shopClean
from lib.cleanReligion import religionClean

# import os
import datetime
import pprint

logger = streetLog("osm_main", 'osm_main.log')

def pre_working():
    logger.info("enter pre_working")

    addressClean.clean_special_address()
    # handle special value, can not change sequance
    addressClean.clean_special_city()

    # before street
    addressClean.clean_special_province()

    # street put in address.
    addressClean.clean_special_street()

    

    # housenumber in tags.housenumber
    # it need to handle with interpolation together
    addressClean.clean_special_housenumber()
    
    # interpolation will wait after housenumber finished
    # no need special handle

    amenityClean.clean_special_amenity()
    amenityClean.clean_special_bridge()

    nameClean.clean_special_name()

    logger.info("leave pre_working")
    

def working():
    logger.info("enter working")

    '''working code'''
    addressClean.clean_simple_address()

    simpleClean.rename_created_by()
    simpleClean.delete_abandoned_record()
    simpleClean.rename_only()


    addressClean.clean_postcode()
    # city must before district
    addressClean.clean_city()
    addressClean.clean_country()
    addressClean.clean_district()

    # before housenumber, will change house number
    addressClean.clean_street()
    addressClean.clean_province()
    addressClean.clean_housenumber()
    addressClean.clean_interpolation()

    #
    tourismClean.clean_simple_tourism()
    tourismClean.clean_attraction()
    tourismClean.clean_animal()

    #
    
    amenityClean.clean_simple_amenity()
    
    amenityClean.clean_subway()
    amenityClean.clean_amenity()

    nameClean.clean_name()

    shopClean.clean_shop()
    religionClean.clean_religion()

    generalTools.unset_empty_tags()
    generalTools.unset_empty_address()
    generalTools.clean_empty_node()

    logger.info("leave working")

def check_database(db_key, check_orgion = False):
    rst_logger = streetLog("test",
                              '{}_{}_{}.log'.format("org" if check_orgion else "rst",
                                                    db_key,
                                                    datetime.datetime.now().isoformat(timespec='minutes')),
                              'output')
    if check_orgion:
        generalTools.check_key_origion(db_key,rst_logger,True)
    else:
        generalTools.print_query_key_exists(db_key,rst_logger,True)
    del rst_logger
    rst_logger = None

if __name__ == '__main__':
    
    generalTools.refresh_db()
    logger.info("after refresh, node count: {}".format(mongoCollection.count({'type': 'node'})))
    logger.info("after refresh, way count: {}".format(mongoCollection.count({'type': 'way'})))
    generalTools.remove_db_keys()
    # # generalTools.refresh_one_record('5a36ce47c836876becc897c5')
    
    pre_working()
    working()
    logger.info("after clean, node count: {}".format(mongoCollection.count({'type': 'node'})))
    logger.info("after clean, way count: {}".format(mongoCollection.count({'type': 'way'})))


    # Test
    # nameClean.clean_name()
    # addressClean.clean_street()
    

    
    # # Result check    
    
    # grep 'k="aerodrome:type"'  '../../src/shanghai_china.osm' |cut -d'"' -f4|sort|uniq    
    for item in [              
                # 'religion',                
                # 'shop',
                # 'tags.shop',
                ]:
        
        check_database(item, True)
  