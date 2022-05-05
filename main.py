# ----------------------------------------------------------------------------------
# File:				main.py
# Author:			Brian Martinez
# Date Created:		June 07, 2021

# Description: This script will parse the generic json from bulk data parser to expected json API
# ----------------------------------------------------------------------------------

import json
import sys
import os



def main():
    base_folder_path = "/Users/bm1120/Desktop/ParsedFiles"
    bulk_data_folder_path = os.path.join(base_folder_path ,"Parsed")
    json_data_folder_path = os.path.join(base_folder_path,"DAL")
    files = get_file_name(bulk_data_folder_path)
    if ".DS_Store" in files:
        files.remove(".DS_Store")
    os.mkdir(json_data_folder_path)

    #Variable for folder summary
    summary_array = []

    for file_name in files:
        json_data_file = (os.path.splitext(file_name)[0]).replace("bulk_data","") + ".json"

        input_json_path = os.path.join(bulk_data_folder_path,file_name)
        output_json_path = os.path.join(json_data_folder_path,json_data_file)

        #Read Json file
        json_input = getJsonDataFromFile(input_json_path)
        input_obj = json.loads(json_input)

        obj_count = len(input_obj)
        print(f"Re organizing {obj_count} elements")

        obj_array = []
        
        for obj in input_obj:
            #Re organize object
            output_obj = reformat_data(obj)
            obj_array.append(output_obj)

            #Create summary of patient
            summary_obj = create_summary(output_obj, file_name)
            summary_array.append(summary_obj)

        #Merge array into a single object
        final_obj = merge_patient_data(obj_array)

        #Dump data on file
        if final_obj == None:
            final_obj = {}
        output_obj = json.dumps(final_obj, indent=4)
        saveJsonDataOnFile(output_json_path, output_obj)


        
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
    request_id = input_obj[0]["PostHeader"]["PatientId"]
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
    output_obj["Meter"] = final_param

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
    final_param = clean_accesories_object(final_param)
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

    # ------ Private Records ------

    private_entries = input_obj[5]["PrivateRecords"]["Entries"]

    #PhoneUserActivity
    temp_param = get_private_records(private_entries, "UserActivityRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["PhoneUserActivity"] = final_param

    #PhoneErrorLog
    temp_param = get_private_records(private_entries, "ErrorLogRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["PhoneErrorLog"] = final_param

    #PhoneCrashLog
    temp_param = get_private_records(private_entries, "CrashLogRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["PhoneErrorLog"] = final_param

    #TxDiagnostic
    temp_param = get_private_records(private_entries, "TxDiagnosticRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["TxDiagnostic"] = final_param

    #ManufacturingData
    temp_param = get_private_records(private_entries, "ManufacturingDataRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["ManufacturingData"] = final_param

    #FirmwareParameterData
    temp_param = get_private_records(private_entries, "FirmwareParameterRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["FirmwareParameterData"] = final_param

    #PCSoftwareParameter
    temp_param = get_private_records(private_entries, "PCSoftwareParameterRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["PCSoftwareParameter"] = final_param

    #ActivityLog
    temp_param = get_private_records(private_entries, "ActivityLogRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["ActivityLog"] = final_param

    #ProcessorErrorBlock
    temp_param = get_private_records(private_entries, "ReceiverProcessorErrorRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["ProcessorErrorBlock"] = final_param

    #FirmwareHeader
    temp_param = get_private_records(private_entries, "FirmwareHeaderRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["FirmwareHeader"] = final_param

    #InsertionTime
    temp_param = get_private_records(private_entries, "InsertionTimeRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["InsertionTime"] = final_param

    #ReceiverLog
    temp_param = get_private_records(private_entries, "ReceiverLogRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["ReceiverLog"] = final_param

    #SessionCommandData
    temp_param = get_private_records(private_entries, "SessionCommandDataRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["SessionCommandData"] = final_param

    #TransmitterInfo
    temp_param = get_private_records(private_entries, "TransmitterInfoDataRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["TransmitterInfo"] = final_param

    #TransmitterPrivateData
    temp_param = get_private_records(private_entries, "TransmitterPrivateDataRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["TransmitterPrivateData"] = final_param

    #TransmitterManifest
    temp_param = get_private_records(private_entries, "TransmitterManifestRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["TransmitterManifest"] = final_param

    #BackfilledEGVData
    temp_param = get_private_records(private_entries, "BackfilledEGVDataRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["BackfilledEGVData"] = final_param

    #ReceiverErrorData
    temp_param = get_private_records(private_entries, "ReceiverErrorDataRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["ReceiverErrorData"] = final_param

    #SensorData
    temp_param = get_private_records(private_entries, "SensorDataRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["SensorData"] = final_param

    #CalSet
    temp_param = get_private_records(private_entries, "CalSetRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["CalSet"] = final_param

    #Aberration
    temp_param = get_private_records(private_entries, "AberrationRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["Aberration"] = final_param

    #Communication
    temp_param = get_private_records(private_entries, "CommunicationRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["Communication"] = final_param

    #ContentPrivate
    temp_param = get_private_records(private_entries, "ContentPrivateRecord")
    final_param = appendSourceStream(temp_param,"SourceStream", source_stream)
    output_obj["ContentPrivate"] = final_param

    return output_obj


def get_private_records(private_entries, record_name):

    was_found = False
    record = []
    for entrie in private_entries:
        current_record_name = entrie["RecordType"]
        if current_record_name == record_name:
            record_string = entrie["Records"]
            was_found = True
            break

    if was_found == True:
        #Verify if the record is clean
        record = parse_string_to_obj(record_string)

    return record
    
def parse_string_to_obj(record_string):
    text = record_string
    obj = json.loads(text)
    return obj

def clean_accesories_object(obj_array):
    for obj in obj_array:
        old_value = obj["Accessories"]
        obj["Accessories"] = parse_string_to_obj(old_value)

    return obj_array

def merge_patient_data(data_array):

    if data_array == []:
        return

    patient_obj = {}
    patient_id = data_array[0]["RequestId"]

    print(f"Merging data for patient {patient_id}")
    patient_obj ["RequestId"] = data_array[0]["RequestId"]

    starter = True
    for patient in data_array:
        for key, values in patient.items():
            if key != "RequestId":
                if starter == True:
                    patient_obj[key] = values
                else:
                    patient_obj[key] += values
        starter = False

    return patient_obj

def get_file_name(folder_path):
     file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
     return file_names





def create_summary(patient_obj, file_name):
    #Read Current Patient Info
    summary_obj = {}
    summary_obj["PatientId"] = patient_obj["RequestId"]
    summary_obj["FileName"] = file_name

    valid_keys = []
    for key, info in patient_obj.items():
        if info != []:
            valid_keys.append(key)

    summary_obj["AvaliableRecords"] = valid_keys

    return summary_obj





if __name__ == '__main__':
    sys.exit(main())

