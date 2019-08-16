import json, csv
import requests
import boto3
import os, shutil

from datetime import timedelta, time, datetime, date

import time

from os import path

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler

# Capture the output files into this location
# csvFilePath = '/home/ubuntu/data/awair/'
fields = ['timestamp', 'temp', 'humid', 'co2', 'voc', 'pm25', 'lux', 'spl_a']

def retrieveDevices():
    #First retrieves available devices
    url = 'http://developer-apis.awair.is/v1/users/self/devices'
    payload = {}
    headers = {
      'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiMjAxNzQifQ.jkyCMOemBhIgHkoxq4xJiAmnyGeSTOWbrITAc64JXdg'
    }

    response = requests.request('GET', url, headers=headers, data=payload, allow_redirects=False)


    #Converting to list
    response_dict = response.json()
    devices = response_dict['devices']

    #Converting to csv
    fields = ['name', 'deviceId', 'deviceType', 'latitude', 'longitude', 'spaceType', 'timezone']
    device_id = []
    device_type = []

    for device in devices:
        deviceInfo = []
        for field in fields:
            if (field=='latitude' or field=='longitude'):
                num = device[field]
                deviceInfo.append(round(num, 6))
            else:
                deviceInfo.append(device[field])
        device_id.append(deviceInfo[1])
        device_type.append(deviceInfo[2])
    print(device_id, device_type)
    return device_id, device_type
    

def deviceMapdata():
    fields = ['name', 'deviceId', 'deviceType', 'latitude', 'longitude', 'spaceType', 'timezone']
    for device in devices:
             deviceInfo = []
             for field in fields:
                if (field=='latitude' or field=='longitude'):
                    num = device[field]
                    deviceInfo.append(round(num, 6))
                else:
                    deviceInfo.append(device[field])

def retrieveDeviceData(name, deviceId, deviceType, startDate, endDate):

    urlHeader = 'https://developer-apis.awair.is/v1/users/self/devices/' + deviceType + '/' + deviceId + '/air-data/raw?'

    #Iterating dates for reading
    iterDate = startDate

    while iterDate + timedelta(hours=1) < endDate:
        start = iterDate.isoformat(sep='T', timespec='auto') + '.000Z'
        iterDate += timedelta(hours=1)
        end = iterDate.isoformat(sep='T', timespec='auto') + '.000Z'
        #calls helper method to write data
        try:
            singleDataRequest(deviceId, start, end, urlHeader)
        except:
            continue

def singleDataRequest(deviceId, deviceType, start, end):
    urlHeader = 'https://developer-apis.awair.is/v1/users/self/devices/' + deviceType + '/' + deviceId + '/air-data/raw?'
    url = urlHeader + 'from=' + start + '&to=' + end
    payload = {}
    headers = {
        'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiMjAxNzQifQ.jkyCMOemBhIgHkoxq4xJiAmnyGeSTOWbrITAc64JXdg'
    }

    print(url)
    try:
        response = requests.request('GET', url, headers=headers, data=payload, allow_redirects=False)
    except:
        print('error')
    #print(response.text)

    dataJson = response.json()

    #print(dataJson)

    dataJson = dataJson['data']

    for sample in dataJson:
        formattedData = {}
        rowData = []

        formattedData['timestamp'] = (sample['timestamp'])

        for reading in sample['sensors']:
            num = reading['value']
            formattedData[reading['comp']] = round(num, 2)

        rowData.append(deviceId)
        for field in fields:
            try:
                rowData.append(formattedData[field])
            except:
                rowData.append(None)

        print(rowData)
        #fileCsv.writerow(rowData)

if __name__ == '__main__':


    startDate = datetime(2019, 8, 15, 7, 0, 0)
    #endDate = datetime.utcnow()
    endDate = datetime(2019, 8, 15, 8, 0, 0)
    devIdList, devTypeList = retrieveDevices()
    numberOfDevices = len(devIdList)
    #print("Length of the device list {}".format(len(devIdList)))
    
    for index in range(numberOfDevices):
        devId = devIdList[index]
        devType = devTypeList[index]
        start = startDate.isoformat(sep='T', timespec='auto') + '.000Z'
        end = endDate.isoformat(sep='T', timespec='auto') + '.000Z'
    
        singleDataRequest(str(6928), 'awair-omni', start, end)