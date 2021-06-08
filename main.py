# ----------------------------------------------------------------------------------
# File:				main.py
# Author:			Brian Martinez
# Date Created:		June 07, 2021

# Description: This script will parse the generic json from bulk data parser to expected json API
# ----------------------------------------------------------------------------------

import json
import sys
import os
import argparse
import os
import subprocess
import sys


def main():
    #Read Json file
    json_input = getJsonDataFromFile("test.json")
    input_obj = json.loads(json_input)

    obj_count = len(input_obj)
    print(f"Re organizing {obj_count} elements")

    obj_array = []
    for obj in input_obj:
        #Re organize object
        output_obj = reformat_data(obj)
        obj_array.append(output_obj)

    #Dump data on file
    output_obj = json.dumps(obj_array, indent=4)
    saveJsonDataOnFile("final.json", output_obj)

    stop = True




def getJsonDataFromFile(file_path):
    f = open(file_path, "r")
    data =  f.read()
    f.close()
    
    return data

def saveJsonDataOnFile(file_path, data):
    f = open(file_path, "w")
    data =  f.write(data)
    f.close()

        
def appendSourceStream(obj_array, header, value):
    for obj in obj_array:
        obj[header] = value

    return obj_array

def reformat_data(input_obj):
    #Get basic info
    request_id = input_obj[1]["PostId"]#Pass this as Request ID
    source_stream = input_obj[0]["PostHeader"]["SourceStream"]

    #Setup Final Object
    output_obj = {}
    output_obj["RequestId"] = request_id

    #Glucose
    temp_param = input_obj[3]["PublicRecords"][0]["GlucoseRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["Glucose"] = final_param

    #MeterRecord
    temp_param = input_obj[3]["PublicRecords"][1]["MeterRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["MeterRecord"] = final_param

    #Sensor
    temp_param = input_obj[3]["PublicRecords"][2]["SensorRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["Sensor"] = final_param

    #UserEvent
    temp_param = input_obj[3]["PublicRecords"][3]["UserEventsRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["UserEvent"] = final_param
    
    #AlertSettings
    temp_param = input_obj[3]["PublicRecords"][4]["AlertSettingsRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["AlertSettings"] = final_param
    
    #DeviceSettings
    temp_param = input_obj[3]["PublicRecords"][5]["DeviceSettingsRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["DeviceSettings"] = final_param

    #SmoothedGlucose
    temp_param = input_obj[3]["PublicRecords"][6]["SmoothedGlucoseRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["SmoothedGlucose"] = final_param

    #AlertSchedule
    temp_param = input_obj[3]["PublicRecords"][7]["AlertScheduleRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["AlertSchedule"] = final_param

    #AlertEvent
    temp_param = input_obj[3]["PublicRecords"][8]["AlertEventRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["AlertEvent"] = final_param

    #Inventory
    temp_param = input_obj[3]["PublicRecords"][9]["InventoryRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["Inventory"] = final_param

    #NotificationSetting
    temp_param = input_obj[3]["PublicRecords"][10]["NotificationSettingsRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["NotificationSetting"] = final_param

    #PenDoses
    temp_param = input_obj[3]["PublicRecords"][11]["InsulinPenDoseRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["PenDoses"] = final_param

    #PenEvents
    temp_param = input_obj[3]["PublicRecords"][12]["InsulinPenEventRecords"]
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["PenEvents"] = final_param

    #Records Not found on bulk data
    output_obj["PhoneUserActivity"] = []
    output_obj["PhoneErrorLog"] = []
    output_obj["PhoneCrashLog"] = []
    output_obj["TxDiagnostic"] = []
    output_obj["ManufacturingData"] = []
    output_obj["FirmwareParameterData"] = []
    output_obj["PCSoftwareParameter"] = []
    output_obj["ActivityLog"] = []
    output_obj["ProcessorErrorBlock"] = []
    output_obj["FirmwareHeader"] = []
    output_obj["InsertionTime"] = []
    output_obj["ReceiverLog"] = []
    output_obj["SessionCommandData"] = []
    output_obj["TransmitterInfo"] = []
    output_obj["TransmitterPrivateData"] = []
    output_obj["TransmitterManifest"] = []
    output_obj["BackfilledEGVData"] = []
    output_obj["ReceiverErrorData"] = []
    output_obj["SensorData"] = []
    output_obj["CalSet"] = []
    output_obj["Aberration"] = []
    output_obj["Communication"] = []
    output_obj["ContentPrivate"] = []

    return output_obj






if __name__ == '__main__':
    sys.exit(main())

