#!/usr/bin/env python
# coding: utf-8

# In[76]:


import requests
import json 
from datetime import datetime, date, time, timedelta
import time
import csv
import pprint
import os
import numpy as np

from os import path
import pandas as pd

# Change the env for fetching data from different cloud environments.
baseURL = 'https://api-qa.viewcorp.xyz/api/v1'
auth_payload = "{\n\t\"username\": \"lab155@view.com\",\n\t\"password\": \"}A~@{YK8}-djc8Pw\"\n}"

access_token = ""
# Capture all the end points from which to pull the data
device_end_points = [
    "/peakAmplitudedbA",
    "/medianAmplitudedB",
    "/temperatureCelsius"
]
# Capture the output files into this location
csvFilePath = '/Users/rahulgopala/Documents/View-python/' 


def login():
    global access_token
    url = baseURL + '/okta/login'
    headers = {
        'Content-Type': "application/json"
    }

    response = requests.request('POST', url, headers=headers, data=auth_payload, allow_redirects=False)
    if response.status_code == 201:
        access_token = json.loads(response.text)['access_token']
    else:
        print('An error occurred calling ' + url + ': ' + str(response.json()))


def retrieveDevices():    
    url = baseURL + '/tenant/devices'
    headers = {
        'Authorization': "Bearer {0}".format(access_token)
    }

    response = requests.request('GET', url, headers=headers, allow_redirects=False)
    #Converting to csv
    fields = ['ip', 'mac', 'name', 'serial', 'deviceId']
    deviceFile = open(csvFilePath + 'view_sense_devices.csv', 'w', newline='')
    deviceCsv = csv.writer(deviceFile)
    deviceCsv.writerow(fields)

    devices = json.loads(response.text)
    
    deviceIds = []
    for device in devices:
        print(device)
        deviceInfo = []
        deviceIds.append(device['deviceId'])
        for field in fields:
            if(field in fields):
                deviceInfo.append(device[field])
        print("deviceInfo: {0}\n".format(deviceInfo))
        deviceCsv.writerow(deviceInfo)
    
    deviceFile.close()
    return deviceIds


def retrieveHistoricalDeviceData(deviceIds, startDate, endDate):
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    #date = str(startDate)
    
    # For each device, fetch data between start and end times
    # Filenames are based on deviceID's and the date
    for deviceId in deviceIds:
        deviceDataFile = open(csvFilePath + deviceId + startDate.strftime("%Y-%m-%dT") + '_vs_data.csv', 'a', newline='')
        deviceDataCsv = csv.writer(deviceDataFile)
        headers = ['endpoints', 'timestamp', 'value']
        #deviceDataCsv.writerow(fields)
        
        # To print header only when the file is empty
        filename = csvFilePath + deviceId + startDate.strftime("%Y-%m-%dT") + '_vs_data.csv'
        fileEmpty = os.stat(filename).st_size == 0
        
        with open(filename, "a") as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n',fieldnames=headers)
            if fileEmpty:
                writer.writeheader()
        
        # Construct the start and end times for reading data
        iterDate = startDate
        while iterDate + timedelta(minutes=1) <= endDate:
            start = str(iterDate.strftime(time_format))
            iterDate += timedelta(minutes=1)
            end = str(iterDate.strftime(time_format))            
            retrieveDeviceData(deviceDataCsv, deviceId, start, end)
    
    deviceDataFile.close()
        
        


def retrieveDeviceData(fileCsv, deviceId, startTime, endTime):
    headers = {
        'Authorization': "Bearer {0}".format(access_token)
    }

    # NOTE: Sending parameters in 'queryparams' was not working. Hence appending directly to URL
    url = baseURL + '/astarte/devices/' + deviceId + '?filter=since=' + startTime+ '&to=' + endTime
    #url = baseURL + '/astarte/devices/' + deviceId + '?limit=1'
    req = requests.Request('GET', url, headers=headers)
    prepared = req.prepare()
    printRequest(prepared)
    response = requests.request('GET', url, headers=headers)
    # printResponse(response)

    endpoint = []
    time_stamp = []
    result = []
    if response.status_code == 200:
        jsonData = json.loads(response.text)
        #pprint.pprint(jsonData)
        for graphNodes in jsonData:
            graphNode = graphNodes["graphData"]
            #print("\n\nGraphNode:{0}\n\n".format(graphNode))
            for endNode in graphNode:
                for endPoint, values in endNode.items():
                    if(endPoint in device_end_points):
                        if(not values): 
                            continue
                        dataNodes = endNode[endPoint]["data"]
                        for dataNode in dataNodes:
                            deviceInfo = []
                            value = dataNode["value"]
                            ts = dataNode["timestamp"]
                            #print("{0},{1},{2},{3}".format(deviceId, endPoint, ts, value))
                            #deviceInfo.append([endPoint, ts, value])
                            #fileCsv.writerow(deviceInfo)
                            
                            # Appending features and labels in different columns
                            endpoint.append(endPoint)
                            time_stamp.append(ts)
                            result.append(value)
                           
                        a = np.vstack((endpoint,time_stamp,result))   
                        df = pd.DataFrame(a).transpose()
                        for i in range(df[0].count() - 1):
                            i = i + 1
                            fileCsv.writerow(df.iloc[i])
    else:
        print('An error occurred calling ' + url + ': ' + str(response.json()))


def printRequest(request):
    print('{}\n{}\n{}\n\n{}'.format(
        '-----------START-----------',
        request.method + ' ' + request.url,
        '\n'.join('{}: {}'.format(k, v) for k, v in request.headers.items()),
        request.body,
    ))
def printResponse(response):
    print("Status: {0}".format(response.status_code))
    print("Response: {0}".format(response.text))




if __name__ == '__main__':
    #startDate = datetime.datetime(2019, 8, 5, 10, 0, 10)
    endDate = datetime.utcnow()
    startDate = endDate - timedelta(minutes=1)

    login()
    deviceIds = retrieveDevices()
    #deviceIds = ['kflx1h5DWxqloTNUI3rWwg']
    retrieveHistoricalDeviceData(deviceIds, startDate, endDate)


# In[ ]:





# In[ ]:




