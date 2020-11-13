# basic package
import os
import sys
import pytz
import datetime
import requests
import dateutil.parser
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta

# custom package
from libs import dataList
from libs import fetchAPIUrl
from libs import getAttributeList
from libs import postgres
from libs import AESCBC

# 初始化各連線物件
host = '192.168.63.160'
dbname = 'api'
user = 'postgres'
keycode = 'NlIe786398qCoG9xH1XQixbqdAgunafPALQ3gfF1Hqc='

def parseAPI(connObj):
    # 初始化XML資料陣列
    xml_list = []

    # 初始化資料集編號陣列
    dataId_list = []

    # 初始化資料集計數器
    dataSeq_count = 0
    for dataId_seq in range(1):
        dataId = dataList.fetchDataId(dataId_seq)
        if dataId:
            dataUrl = fetchAPIUrl.parse(dataId).getDataUrl()
        else:
            break

        # query latest etag
        selVariable = dataId
        selData = '''select etag from cwb.main_opendata_cwb_etag
                        where opendata_id = \'''' + selVariable + '''\';'''

        # get feedback result
        select = postgres.connection('', '', '', '', connObj, selData, selVariable).query()
        latest_etag = select[0][0]

        request_headers = {'If-None-Match': latest_etag}

        request_status = requests.get(dataUrl, headers = request_headers)

        if(request_status.status_code == 200):
            print("open data has been updated! continue...")
        elif(request_status.status_code == 304):
            print("no update data available")
            continue;
        else:
            print("encounter unexpected status code " + request_status.status_code)
            break;
        
        etag = requests.get(dataUrl).headers.get('ETag')
        
        # update etag
        updVariable = []
        updVariable.append(etag)
        updVariable.append(datetime.now(pytz.timezone('Asia/Taipei')))
        updVariable.append(dataId)
        updData = '''update cwb.main_opendata_cwb_etag
                        set etag = %s, lastupddate = %s
                        where opendata_id = %s;'''
        
        #get feedback result
        update = postgres.connection('', '', '', '', connObj, updData, updVariable).update()
        print(update)

        xml = requests.get(dataUrl).text.encode('utf-8-sig')
        weather_report = BeautifulSoup(xml, 'html.parser')

        xml_list.append(weather_report)
        dataId_list.append(dataId)
    
    if not xml_list:
        return

    for data_seq in xml_list:
        starttime_list = []
        endtime_list = []
        location_list = []
        result_collection = []

        issuetime = data_seq.find("datasetinfo").find("issuetime")
        starttime = data_seq.find("weatherelement").find_all("starttime")
        endtime = data_seq.find("weatherelement").find_all("endtime")
        city = data_seq.find("dataset").find_all("locationname")

        # 取得現在資料中所有起始時間並加入list
        for starttime_element in starttime:
            starttime_list.append(starttime_element.text)

        # 取得現在資料中所有結束時間並加入list
        for endtime_element in endtime:
            endtime_list.append(endtime_element.text)
        
        # 取得現在資料中所有的鄉鎮並加入list
        for location_element in city:
            location_list.append(location_element.text)
        
        # delete data which already exist
        delVariable = '\'%s\'' * len(starttime_list)
        delVariable = '(' + delVariable.replace('\'\'', '\',\'') + ')'
        delVariable = delVariable.replace('\'', '')
        delData = '''delete from cwb.temp_opendata_cwb_weekweatherforecast
                        where start_time in ''' + delVariable + ''' and opendata_id = %s;'''
        deltime_list = starttime_list.copy()
        for deltime_seq in range(len(deltime_list)):
            deltime_list[deltime_seq] = datetime.strftime(dateutil.parser.parse(deltime_list[deltime_seq]).astimezone(pytz.timezone('Asia/Taipei')), "%Y-%m-%d %H:%M:%S")
        deltime_list.append(dataId_list[dataSeq_count])
        delVar = deltime_list

        # get feedback result
        delete = postgres.connection('', '', '', '', connObj, delData, delVar).delete()
        print(delete)

        for data_num in range(len(location_list)):
            for time_num in range(len(starttime_list)):
                result_list = []
                result_list.append(dataId_list[dataSeq_count])
                result_list.append(datetime.strftime(dateutil.parser.parse(issuetime.text).astimezone(pytz.timezone('Asia/Taipei')), "%Y-%m-%d %H:%M:%S"))
                result_list.append(location_list[data_num])
                result_list.append(datetime.strftime(dateutil.parser.parse(starttime_list[time_num]).astimezone(pytz.timezone('Asia/Taipei')), "%Y-%m-%d %H:%M:%S"))
                result_list.append(datetime.strftime(dateutil.parser.parse(endtime_list[time_num]).astimezone(pytz.timezone('Asia/Taipei')), "%Y-%m-%d %H:%M:%S"))
                result_list.extend(getAttributeList.get_weather_data(data_seq, location_list[data_num], endtime_list[time_num]))
                result_list.append(datetime.now(pytz.timezone('Asia/Taipei')))

                result_collection.extend(result_list)
        
        # insert new data
        insVariable = '\'%s\'' * len(result_list)
        insVariable = '(' + insVariable.replace('\'\'', '\',\'') + ')'
        insVariable = insVariable.replace('\'', '')
        insCollection = insVariable * int(len(result_collection) / 9)
        insCollection = insCollection.replace(')(', '), (')
        insData = '''INSERT INTO cwb.temp_opendata_cwb_weekweatherforecast
                        (opendata_id, issued_time, city, start_time, end_time, wx, maxt, mint, lastupddate)
                        VALUES''' + insCollection + ''';'''
        insVar = result_collection
        
        # get feedback result
        insert = postgres.connection('', '', '', '', connObj, insData, insVar).insert()
        print(insert)

        # transfer data from temp table to main table
        # 01. delete all data from main table with only criteria "open data id"
        main_delVariable = []
        main_delVariable.append(dataId_list[dataSeq_count])
        main_delData = '''delete from cwb.main_opendata_cwb_weekweatherforecast
                            where opendata_id = %s;'''
        
        # get feedback result
        delete = postgres.connection('', '', '', '', connObj, main_delData, main_delVariable).delete()
        print(delete)

        # 02. transfer data from temp to main table
        main_insVariable = []
        main_insVariable.append(dataId_list[dataSeq_count])
        main_insData = '''insert into cwb.main_opendata_cwb_weekweatherforecast
                            select * from cwb.temp_opendata_cwb_weekweatherforecast where opendata_id = %s;'''
        
        # get feedback result
        insert = postgres.connection('', '', '', '', connObj, main_insData, main_insVariable).insert()
        print(insert)


        dataSeq_count += 1

# 取得連線物件
connObj = postgres.connection(host, dbname, user, AESCBC.AESCipher().decrypt(keycode).decode('utf-8'), '', '', '').getConnectionObject()

# main process
parseAPI(connObj)

# 釋放連線物件
postgres.connection('', '', '', '', connObj, '', '').release()