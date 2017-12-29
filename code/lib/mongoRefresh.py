#! /bin/env python

from pymongo import MongoClient
from bson.objectid import ObjectId

from lib.osmLog import streetLog

import json

import os

logger = streetLog("osmImport", 'importFixedOsm.log')






