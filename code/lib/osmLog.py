import logging
import os     

from lib.osmCommon import LogDir

def streetLog(modle_name, filename='StreetMap.log', folder = LogDir):     
      
    if not os.path.exists(folder):
        os.mkdir(folder)
    logfile = os.path.join(folder, filename)
    
    logger = logging.getLogger(modle_name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s %(asctime)s:%(name)s - %(message)s")

    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger