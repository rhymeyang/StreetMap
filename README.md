# StreetMap

数据来源 : mapzen.com

+ [下载页面](https://mapzen.com/data/metro-extracts/metro/shanghai_china/102027181/Shanghai/)
+ [下载链接](https://s3.amazonaws.com/metro-extracts.mapzen.com/shanghai_china.osm.bz2)

## 运行环境

1. MongoDB shell version v3.4.10
2. Python 3.6.3
3. pymongo 3.5.1
4. pprint 0.1

## 处理过程

1. 下载osm文件后解压缩，取代当前 code/src/shanghai_china.osm
2. 运行 prepare.py， 将原始数据导入 mongodb
3. 运行 main.py，进行数据清理

