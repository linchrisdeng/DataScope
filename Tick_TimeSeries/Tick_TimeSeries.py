#!/usr/bin/python
# -*- coding: UTF-8 -*-
from json import dumps, loads, load
from requests import post
from requests import get
from requests import adapters
from time import sleep
from getpass import _raw_input as input
from getpass import getpass
from getpass import GetPassWarning
from collections import OrderedDict
import os
import gzip
from datetime import datetime
import wget

# new add in code to solve proxy error / exceeds retries
os.environ['NO_PROXY'] = "https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/ExtractRaw"
adapters.DEFAULT_RETRIES = 150

_outputFilePath="./"
_outputFileName=datetime.now().strftime('%Y%m%dT%H%M%S')
_retryInterval=int(5) #value in second used by Pooling loop to check request status on the server
_jsonFileName="Tick_TimeSeries.json"

def RequestToken(username="",password=""):
    _AuthenURL = "https://selectapi.datascope.refinitiv.com/RestApi/v1/Authentication/RequestToken"
    _header= {}
    _header['Prefer']='respond-async'
    _header['Content-Type']='application/json; odata.metadata=minimal'
    _data={
        'Credentials':{
        'Password':password,
        'Username':username
        }
    }

    print("Send Login request")
    resp=post(_AuthenURL,json=_data,headers=_header)

    if resp.status_code!=200:
        message="Authentication Error Status Code: "+ str(resp.status_code) +" Message:"+dumps(loads(resp.text),indent=4)
        raise Exception(str(message))
    return loads(resp.text)['value']


def ExtractRaw(token, jsonBody):
    try:
        _extractRawURL="https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/ExtractRaw"
        #Setup Request Header
        _header={}
        _header['Prefer']='respond-async'
        _header['Content-Type']='application/json; odata.metadata=minimal'
        _header['Accept-Charset']='UTF-8'
        _header['Authorization']='Token'+token
        #Post Http Request to DSS server using extract raw URL
        resp=post(_extractRawURL, data=None, json=jsonBody, headers=_header)

        #Print Status Code return from HTTP Response
        print("Status Code="+str(resp.status_code) )

        #Raise exception with error message if the returned status is not 202 (Accepted) or 200 (Ok)
        if resp.status_code!=200:
            if resp.status_code!=202:
                message="Error: Status Code:"+str(resp.status_code)+" Message:"+resp.text
                raise Exception(message)

            #Get location from header, URL must be https so we need to change it using string replace function
            _location=str.replace(resp.headers['Location'],"http://","https://")

            print("Get Status from "+str(_location))
            _jobID=""

            #pooling loop to check request status every 2 sec.
            while True:
                resp=get(_location,headers=_header)
                _pollstatus = int(resp.status_code)

                if _pollstatus==200:
                    break
                else:
                    print("Status:"+str(resp.headers['Status']))
                sleep(_retryInterval) #wait for _retyInterval period and re-request the status to check if it already completed

        # Get the jobID from HTTP response
        json_resp = loads(resp.text)
        _jobID = json_resp.get('JobId')
        print("\n" + "Status is completed the JobID is "+ str(_jobID)+ "\n")

        # Check if the response contains Notes.If the note exists print it to console.
        if len(json_resp.get('Notes')) > 0:
            print("Notes:\n======================================")
            for var in json_resp.get('Notes'):
                print(var)
            print("======================================\n")

        # Request should be completed then Get the result by passing jobID to RAWExtractionResults URL
        _getResultURL = str("https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/RawExtractionResults(\'" + _jobID + "\')/$value")
        print("Retrieve result from " + _getResultURL)
        resp=get(_getResultURL,headers=_header,stream=True)

        #Write Output to file.
        outputfilepath = str(_outputFilePath + _outputFileName + '.csv')
        size = 0
        if resp.status_code==200:
            with open(outputfilepath, 'wb') as f:
                for chunk in resp.iter_content(chunk_size = 1024):
                    if chunk:
                        f.write(chunk)
                        size += len(chunk)
                        f.flush()

        # outputfilepath = str(_outputFilePath + _outputFileName + '.csv.gz')
        # if resp.status_code==200:
        #     with open(outputfilepath, 'wb') as f:
        #         f.write(resp.raw.read())

        print("\nWrite output to "+outputfilepath+" completed\n\n")
        # Pandas print head and tail
        # print("Sample data from "+ outputfilepath)
        # df=pd.read_csv(outputfilepath,compression='gzip')
        # print(df.head())
        # print("...")
        # print(df.tail())

    except Exception as ex:
        print("Exception occrus:", ex)

    return

def main():
    try:
        #Request a new Token
        print("Login to DataScope")
        _DSSUsername=input('Enter DataScope Username:')
        try:
            _DSSPassword=getpass(prompt='Enter DataScope Password:')
            _token=RequestToken(_DSSUsername,_DSSPassword)            
        except GetPassWarning as e:
             print(e)
        print("Login Success")     
        # print("Token="+_token+"\n")

        # Read the request body from JSON file.
        queryString = {}
        with open(_jsonFileName, "r") as filehandle:
            queryString=load(filehandle,object_pairs_hook=OrderedDict)

        # print(queryString)
        ExtractRaw(_token, queryString)

    except Exception as e:
        print(e)

print(__name__)

if __name__=="__main__":
    main()
