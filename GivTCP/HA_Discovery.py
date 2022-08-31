# version 2022.01.21
from array import array
import logging, sys, time, json
from logging.handlers import TimedRotatingFileHandler
import paho.mqtt.client as mqtt
import logging  
from settings import GiV_Settings
from givenergy_modbus.model.inverter import Model
from mqtt import GivMQTT

logger = logging.getLogger("GivTCP_HA_AUTO_D_"+str(GiV_Settings.givtcp_instance))
logging.basicConfig(format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
if GiV_Settings.Debug_File_Location!="":
    fh = TimedRotatingFileHandler(GiV_Settings.Debug_File_Location, when='D', interval=1, backupCount=7)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
if GiV_Settings.Log_Level.lower()=="debug":
    logger.setLevel(logging.DEBUG)
elif GiV_Settings.Log_Level.lower()=="info":
    logger.setLevel(logging.INFO)
elif GiV_Settings.Log_Level.lower()=="critical":
    logger.setLevel(logging.CRITICAL)
elif GiV_Settings.Log_Level.lower()=="warning":
    logger.setLevel(logging.WARNING)
else:
    logger.setLevel(logging.ERROR)


class HAMQTT():

    entity_type={
        "Last_Updated_Time":["sensor","timestamp"],
        "Time_Since_Last_Update":["sensor",""],
        "status":["sensor",""],
        "Export_Energy_Total_kWh":["sensor","energy"],
        "Battery_Throughput_Total_kWh":["sensor","energy"],
        "AC_Charge_Energy_Total_kWh":["sensor","energy"],
        "Import_Energy_Total_kWh":["sensor","energy"],
        "Invertor_Energy_Total_kWh":["sensor","energy"],
        "PV_Energy_Total_kWh":["sensor","energy"],
        "Load_Energy_Total_kWh":["sensor","energy"],
        "Battery_Charge_Energy_Total_kWh":["sensor","energy"],
        "Battery_Discharge_Energy_Total_kWh":["sensor","energy"],
        "Self_Consumption_Energy_Total_kWh":["sensor","energy"],
        "Battery_Throughput_Today_kWh":["sensor","energy"],
        "PV_Energy_Today_kWh":["sensor","energy"],
        "Import_Energy_Today_kWh":["sensor","energy"],
        "Export_Energy_Today_kWh":["sensor","energy"],
        "AC_Charge_Energy_Today_kWh":["sensor","energy"],
        "Invertor_Energy_Today_kWh":["sensor","energy"],
        "Battery_Charge_Energy_Today_kWh":["sensor","energy"],
        "Battery_Discharge_Energy_Today_kWh":["sensor","energy"],
        "Self_Consumption_Energy_Today_kWh":["sensor","energy"],
        "Load_Energy_Today_kWh":["sensor","energy"],
        "PV_Power_String_1":["sensor","power"],
        "PV_Power_String_2":["sensor","power"],
        "PV_Power":["sensor","power"],
        "PV_Voltage_String_1":["sensor","voltage"],
        "PV_Voltage_String_2":["sensor","voltage"],
        "PV_Current_String_1":["sensor","current"],
        "PV_Current_String_2":["sensor","current"],
        "Grid_Power":["sensor","power"],
        "Import_Power":["sensor","power"],
        "Export_Power":["sensor","power"],
        "EPS_Power":["sensor","power"],
        "Invertor_Power":["sensor","power"],
        "Load_Power":["sensor","power"],
        "AC_Charge_Power":["sensor","power"],
        "Self_Consumption_Power":["sensor","power"],
        "Battery_Power":["sensor","power"],
        "Charge_Power":["sensor","power"],
        "Discharge_Power":["sensor","power"],
        "SOC":["sensor","battery"],
        "SOC_kWh":["sensor","energy"],
        "Solar_to_House":["sensor","power"],
        "Solar_to_Battery":["sensor","power"],
        "Solar_to_Grid":["sensor","power"],
        "Battery_to_House":["sensor","power"],
        "Grid_to_Battery":["sensor","power"],
        "Grid_to_House":["sensor","power"],
        "Battery_to_Grid":["sensor","power"],
        "Battery_Type":["sensor",""],
        "Battery_Capacity_kWh":["sensor",""],
        "Invertor_Serial_Number":["sensor","",""],
        "Modbus_Version":["sensor",""],
        "Meter_Type":["sensor",""],
        "Invertor_Type":["sensor",""],
        "Invertor_Temperature":["sensor","temperature"],
        "Discharge_start_time_slot_1":["select","","setDischargeStart1"],
        "Discharge_end_time_slot_1":["select","","setDischargeEnd1"],
        "Discharge_start_time_slot_2":["select","","setDischargeStart2"],
        "Discharge_end_time_slot_2":["select","","setDischargeEnd2"],
        "Charge_start_time_slot_1":["select","","setChargeStart1"],
        "Charge_end_time_slot_1":["select","","setChargeEnd1"],
        "Charge_start_time_slot_2":["select","","setChargeStart2"],
        "Charge_end_time_slot_2":["select","","setChargeEnd2"],
        "Battery_Serial_Number":["sensor",""],
        "Battery_SOC":["sensor","battery"],
        "Battery_Capacity":["sensor","",""],
        "Battery_Design_Capacity":["sensor","",""],
        "Battery_Remaining_Capacity":["sensor","",""],
        "Battery_Firmware_Version":["sensor",""],
        "Battery_Cells":["sensor","",""],
        "Battery_Cycles":["sensor","",""],
        "Battery_USB_present":["binary_sensor",""],
        "Battery_Temperature":["sensor","temperature"],
        "Battery_Voltage":["sensor","voltage"],
        "Battery_Cell_1_Voltage":["sensor","voltage"],
        "Battery_Cell_2_Voltage":["sensor","voltage"],
        "Battery_Cell_3_Voltage":["sensor","voltage"],
        "Battery_Cell_4_Voltage":["sensor","voltage"],
        "Battery_Cell_5_Voltage":["sensor","voltage"],
        "Battery_Cell_6_Voltage":["sensor","voltage"],
        "Battery_Cell_7_Voltage":["sensor","voltage"],
        "Battery_Cell_8_Voltage":["sensor","voltage"],
        "Battery_Cell_9_Voltage":["sensor","voltage"],
        "Battery_Cell_10_Voltage":["sensor","voltage"],
        "Battery_Cell_11_Voltage":["sensor","voltage"],
        "Battery_Cell_12_Voltage":["sensor","voltage"],
        "Battery_Cell_13_Voltage":["sensor","voltage"],
        "Battery_Cell_14_Voltage":["sensor","voltage"],
        "Battery_Cell_15_Voltage":["sensor","voltage"],
        "Battery_Cell_16_Voltage":["sensor","voltage"],
        "Battery_Cell_1_Temperature":["sensor","temperature"],
        "Battery_Cell_2_Temperature":["sensor","temperature"],
        "Battery_Cell_3_Temperature":["sensor","temperature"],
        "Battery_Cell_4_Temperature":["sensor","temperature"],
        "Mode":["select","","setBatteryMode"],
        "Battery_Power_Reserve":["number","","setBatteryReserve"],
        "Target_SOC":["number","","setChargeTarget"],
        "Enable_Charge_Schedule":["switch","","enableChargeSchedule"],
        "Enable_Discharge_Schedule":["switch","","enableDishargeSchedule"],
        "Enable_Discharge":["switch","","enableDischarge"],
        "Battery_Charge_Rate":["number","","setChargeRate"],
        "Battery_Discharge_Rate":["number","","setDischargeRate"],
        "Night_Start_Energy_kWh":["sensor","energy"],
        "Night_Energy_kWh":["sensor","energy"],
        "Night_Cost":["sensor","money"],
        "Night_Rate":["sensor","money"],
        "Day_Start_Energy_kWh":["sensor","energy"],
        "Day_Energy_kWh":["sensor","energy"],
        "Day_Cost":["sensor","money"],
        "Day_Rate":["sensor","money"],
        "Current_Rate":["sensor","money"],
        "Export_Rate":["sensor","money"],
        "Import_ppkwh_Today":["sensor","money"],
        "Battery_Value":["sensor","money"],
        "Battery_ppkwh":["sensor","money"]
        }
    time_slots=[
    "00:00:00","00:10:00","00:20:00","00:30:00","00:40:00","00:50:00",
    "01:00:00","01:10:00","01:20:00","01:30:00","01:40:00","01:50:00",
    "02:00:00","02:10:00","02:20:00","02:30:00","02:40:00","02:50:00",
    "03:00:00","03:10:00","03:20:00","03:30:00","03:40:00","03:50:00",
    "04:00:00","04:10:00","04:20:00","04:30:00","04:40:00","04:50:00",
    "05:00:00","05:10:00","05:20:00","05:30:00","05:40:00","05:50:00",
    "06:00:00","06:10:00","06:20:00","06:30:00","06:40:00","06:50:00",
    "07:00:00","07:10:00","07:20:00","07:30:00","07:40:00","07:50:00",
    "08:00:00","08:10:00","08:20:00","08:30:00","08:40:00","08:50:00",
    "09:00:00","09:10:00","09:20:00","09:30:00","09:40:00","09:50:00",
    "10:00:00","10:10:00","10:20:00","10:30:00","10:40:00","10:50:00",
    "11:00:00","11:10:00","11:20:00","11:30:00","11:40:00","11:50:00",
    "12:00:00","12:10:00","12:20:00","12:30:00","12:40:00","12:50:00",
    "13:00:00","13:10:00","13:20:00","13:30:00","13:40:00","13:50:00",
    "14:00:00","14:10:00","14:20:00","14:30:00","14:40:00","14:50:00",
    "15:00:00","15:10:00","15:20:00","15:30:00","15:40:00","15:50:00",
    "16:00:00","16:10:00","16:20:00","16:30:00","16:40:00","16:50:00",
    "17:00:00","17:10:00","17:20:00","17:30:00","17:40:00","17:50:00",
    "18:00:00","18:10:00","18:20:00","18:30:00","18:40:00","18:50:00",
    "19:00:00","19:10:00","19:20:00","19:30:00","19:40:00","19:50:00",
    "20:00:00","20:10:00","20:20:00","20:30:00","20:40:00","20:50:00",
    "21:00:00","21:10:00","21:20:00","21:30:00","21:40:00","21:50:00",
    "22:00:00","22:10:00","22:20:00","22:30:00","22:40:00","22:50:00",
    "23:00:00","23:10:00","23:20:00","23:30:00","23:40:00","23:50:00",
    ]

    if GiV_Settings.MQTT_Port=='':
        MQTT_Port=1883
    else:
        MQTT_Port=int(GiV_Settings.MQTT_Port)
    MQTT_Address=GiV_Settings.MQTT_Address
    if GiV_Settings.MQTT_Username=='':
        MQTTCredentials=False
    else:
        MQTTCredentials=True
        MQTT_Username=GiV_Settings.MQTT_Username
        MQTT_Password=GiV_Settings.MQTT_Password
    if GiV_Settings.MQTT_Topic=="":
        GiV_Settings.MQTT_Topic="GivEnergy"


    def on_connect(client, userdata, flags, rc):
        if rc==0:
            client.connected_flag=True #set flag
            logger.info("connected OK Returned code="+str(rc))
            #client.subscribe(topic)
        else:
            logger.info("Bad connection Returned code= "+str(rc))
    
    def publish_discovery(array,SN):   #Recieve multiple payloads with Topics and publish in a single MQTT connection
        mqtt.Client.connected_flag=False        			#create flag in class
        client=mqtt.Client("GivEnergy_GivTCP_"+str(GiV_Settings.givtcp_instance))
        rootTopic=str(GiV_Settings.MQTT_Topic+"/"+SN+"/")
        if HAMQTT.MQTTCredentials:
            client.username_pw_set(HAMQTT.MQTT_Username,HAMQTT.MQTT_Password)
        try:
            client.on_connect=HAMQTT.on_connect     			#bind call back function
            client.loop_start()
            logger.info ("Connecting to broker: "+ HAMQTT.MQTT_Address)
            client.connect(HAMQTT.MQTT_Address,port=HAMQTT.MQTT_Port)
            while not client.connected_flag:        			#wait in loop
                logger.info ("In wait loop")
                time.sleep(0.2)
                ##publish the status message
                client.publish(GiV_Settings.MQTT_Topic+"/"+SN+"/status","online", retain=True)
            ### For each topic create a discovery message
                for p_load in array:
                    if p_load != "raw":
                        payload=array[p_load]
                        logger.info('Publishing: '+rootTopic+p_load)
                        output=GivMQTT.iterate_dict(payload,rootTopic+p_load)   #create LUT for MQTT publishing
                        for topic in output:
                            #Determine Entitiy type (switch/sensor/number) and publish the right message
                            if HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="sensor":
                                if "Battery_Details" in topic:
                                    client.publish("homeassistant/sensor/GivEnergy/"+str(topic).split("/")[-2]+"_"+str(topic).split("/")[-1]+"/config",HAMQTT.create_device_payload(topic,SN),retain=True)
                                else:
                                    client.publish("homeassistant/sensor/GivEnergy/"+SN+"_"+str(topic).split("/")[-1]+"/config",HAMQTT.create_device_payload(topic,SN),retain=True)
                            elif HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="switch":
                                client.publish("homeassistant/switch/GivEnergy/"+SN+"_"+str(topic).split("/")[-1]+"/config",HAMQTT.create_device_payload(topic,SN),retain=True)
                            elif HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="number":
                                client.publish("homeassistant/number/GivEnergy/"+SN+"_"+str(topic).split("/")[-1]+"/config",HAMQTT.create_device_payload(topic,SN),retain=True)
                        #    elif HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="binary_sensor":
                        #        client.publish("homeassistant2/binary_sensor/GivEnergy/"+str(topic).split("/")[-1]+"/config",HAMQTT.create_binary_sensor_payload(topic,SN),retain=True)
                            elif HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="select":
                                client.publish("homeassistant/select/GivEnergy/"+SN+"_"+str(topic).split("/")[-1]+"/config",HAMQTT.create_device_payload(topic,SN),retain=True)
                
            client.loop_stop()                      			#Stop loop
            client.disconnect()
        except:
            e = sys.exc_info()
            logger.error("Error connecting to MQTT Broker: " + str(e))
        return client

    def create_device_payload(topic,SN):
        tempObj={}
        tempObj['stat_t']=str(topic).replace(" ","_")
        tempObj['avty_t'] = GiV_Settings.MQTT_Topic+"/"+SN+"/status"
        tempObj["pl_avail"]= "online"
        tempObj["pl_not_avail"]= "offline"
        tempObj['device']={}
        
        GiVTCP_Device=str(topic).split("/")[2]
        if "Battery_Details" in topic:
            tempObj["name"]=GiV_Settings.ha_device_prefix+" "+str(topic).split("/")[3].replace("_"," ")+" "+str(topic).split("/")[-1].replace("_"," ") #Just final bit past the last "/"
            tempObj['uniq_id']=str(topic).split("/")[3]+"_"+GiVTCP_Device+"_"+str(topic).split("/")[-1]
            tempObj['device']['identifiers']=str(topic).split("/")[3]+"_"+GiVTCP_Device
            tempObj['device']['name']=GiV_Settings.ha_device_prefix+" "+str(topic).split("/")[3].replace("_"," ")+" "+GiVTCP_Device
        else:
            tempObj['uniq_id']=SN+"_"+GiVTCP_Device+"_"+str(topic).split("/")[-1]
            tempObj['device']['identifiers']=SN+"_"+GiVTCP_Device
            tempObj['device']['name']=GiV_Settings.ha_device_prefix+" "+SN+" "+str(GiVTCP_Device).replace("_"," ")
            tempObj["name"]=GiV_Settings.ha_device_prefix+" "+SN+" "+str(topic).split("/")[-1].replace("_"," ") #Just final bit past the last "/"
        tempObj['device']['manufacturer']="GivEnergy"

        try:
            tempObj['command_topic']=GiV_Settings.MQTT_Topic+"/control/"+SN+"/"+HAMQTT.entity_type[str(topic).split("/")[-1]][2]
        except:
            pass
#set device specific elements here:
        if HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="sensor":
            tempObj['unit_of_meas']=""
            if HAMQTT.entity_type[str(topic).split("/")[-1]][1]=="energy":
                tempObj['unit_of_meas']="kWh"
                tempObj['device_class']="Energy"
                if topic.split("/")[-2]=="Total":
                    tempObj['state_class']="total"
                else:
                    tempObj['state_class']="total_increasing"
            if HAMQTT.entity_type[str(topic).split("/")[-1]][1]=="money":
                if "ppkwh" in topic:
                   tempObj['unit_of_meas']="{GBP}/kWh"
                else:
                    tempObj['unit_of_meas']="{GBP}"
                tempObj['device_class']="Monetary"
            if HAMQTT.entity_type[str(topic).split("/")[-1]][1]=="power":
                tempObj['unit_of_meas']="W"
                tempObj['device_class']="Power"
                tempObj['state_class']="measurement"
            if HAMQTT.entity_type[str(topic).split("/")[-1]][1]=="temperature":
                tempObj['unit_of_meas']="C"
                tempObj['device_class']="Temperature"
            if HAMQTT.entity_type[str(topic).split("/")[-1]][1]=="voltage":
                tempObj['unit_of_meas']="V"
                tempObj['device_class']="Voltage"
            if HAMQTT.entity_type[str(topic).split("/")[-1]][1]=="current":
                tempObj['unit_of_meas']="A"
                tempObj['device_class']="Current"
            if HAMQTT.entity_type[str(topic).split("/")[-1]][1]=="battery":
                tempObj['unit_of_meas']="%"
                tempObj['device_class']="Battery"
            if HAMQTT.entity_type[str(topic).split("/")[-1]][1]=="timestamp":
                del(tempObj['unit_of_meas'])
                tempObj['device_class']="timestamp"
        elif HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="switch":
            tempObj['payload_on']="enable"
            tempObj['payload_off']="disable"
    #    elif HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="binary_sensor":
    #        client.publish("homeassistant/binary_sensor/GivEnergy/"+str(topic).split("/")[-1]+"/config",HAMQTT.create_binary_sensor_payload(topic,SN),retain=True)
        elif HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="select":
            if "Mode" in topic:
                options=["Eco","Timed Demand","Timed Export","Unknown", "Eco (Paused)"]
            elif "slot" in topic:
                options=HAMQTT.time_slots
            tempObj['options']=options
        elif HAMQTT.entity_type[str(topic).split("/")[-1]][0]=="number":
            tempObj['unit_of_meas']="%"
        ## Convert this object to json string
        jsonOut=json.dumps(tempObj)
        return(jsonOut)