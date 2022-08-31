import paho.mqtt.client as mqtt
import time, sys, importlib,logging
from os.path import exists
from logging.handlers import TimedRotatingFileHandler
import settings
from settings import GiV_Settings
import write as wr
import pickle
from pickletools import read_uint1
sys.path.append(GiV_Settings.default_path)

logger = logging.getLogger("GivTCP_MQTT_Client_"+str(GiV_Settings.givtcp_instance))
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

regcache=GiV_Settings.cache_location+"/regCache_"+str(GiV_Settings.givtcp_instance)+".pkl"

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
if GiV_Settings.MQTT_Topic=='':
    MQTT_Topic='GivEnergy'
else:
    MQTT_Topic=GiV_Settings.MQTT_Topic

#loop till serial number has been found
while not hasattr(GiV_Settings,'serial_number'):
    time.sleep(5)
    #del sys.modules['settings.GiV_Settings']
    importlib.reload(settings)
    from settings import GiV_Settings
    count=+1
    if count==20:
        logger.error("No serial_number found in MQTT queue. MQTT Control not available.")
        break
    
logger.info("Serial Number retrieved: "+GiV_Settings.serial_number)

def on_message(client, userdata, message):
    payload={}
    logger.info("MQTT Message Recieved: "+str(message.topic)+"= "+str(message.payload.decode("utf-8")))
    writecommand={}
    command=str(message.topic).split("/")[-1]
    if command=="setDischargeRate":
        writecommand['dischargeRate']=str(message.payload.decode("utf-8"))
        result=wr.setDischargeRate(writecommand)
    elif command=="setChargeRate":
        writecommand['chargeRate']=str(message.payload.decode("utf-8"))
        result=wr.setChargeRate(writecommand)
    elif command=="enableChargeTarget":
        writecommand['state']=str(message.payload.decode("utf-8"))
        result=wr.enableChargeTarget(writecommand)
    elif command=="enableChargeSchedule":
        writecommand['state']=str(message.payload.decode("utf-8"))
        result=wr.enableChargeSchedule(writecommand)
    elif command=="enableDishargeSchedule":
        writecommand['state']=str(message.payload.decode("utf-8"))
        result=wr.enableDischargeSchedule(writecommand)
    elif command=="enableDischarge":
        writecommand['state']=str(message.payload.decode("utf-8"))
        result=wr.enableDischarge(writecommand)
    elif command=="setChargeTarget":
        writecommand['chargeToPercent']=str(message.payload.decode("utf-8"))
        result=wr.setChargeTarget(writecommand)
    elif command=="setBatteryReserve":
        writecommand['dischargeToPercent']=str(message.payload.decode("utf-8"))
        result=wr.setBatteryReserve(writecommand)
    elif command=="setBatteryMode":
        writecommand['mode']=str(message.payload.decode("utf-8"))
        result=wr.setBatteryMode(writecommand)
    elif command=="setDateTime":
        writecommand['dateTime']=str(message.payload.decode("utf-8"))
        result=wr.setDateTime(writecommand)
    elif command=="setShallowCharge":
        writecommand['val']=str(message.payload.decode("utf-8"))
        result=wr.setShallowCharge(writecommand)
    elif command=="setChargeStart1":
        if exists(regcache):
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            multi_output=regCacheStack[0]
            finish=multi_output['Timeslots']['Charge_end_time_slot_1']
            payload['start']=message.payload.decode("utf-8")[:5]
            payload['finish']=finish[:5]
            result=wr.setChargeSlot1(payload)
    elif command=="setChargeEnd1":
        if exists(regcache):
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            multi_output=regCacheStack[0]
            start=multi_output['Timeslots']['Charge_start_time_slot_1']
            payload['finish']=message.payload.decode("utf-8")[:5]
            payload['start']=start[:5]
            result=wr.setChargeSlot1(payload)
    elif command=="setDischargeStart1":
        if exists(regcache):
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            multi_output=regCacheStack[0]
            finish=multi_output['Timeslots']['Discharge_end_time_slot_1']
            payload['start']=message.payload.decode("utf-8")[:5]
            payload['finish']=finish[:5]
            result=wr.setDischargeSlot1(payload)
    elif command=="setDischargeEnd1":
        if exists(regcache):
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            multi_output=regCacheStack[0]
            start=multi_output['Timeslots']['Discharge_start_time_slot_1']
            payload['finish']=message.payload.decode("utf-8")[:5]
            payload['start']=start[:5]
            result=wr.setDischargeSlot1(payload)
    elif command=="setDischargeStart2":
        if exists(regcache):
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            multi_output=regCacheStack[0]
            finish=multi_output['Timeslots']['Discharge_end_time_slot_2']
            payload['start']=message.payload.decode("utf-8")[:5]
            payload['finish']=finish[:5]
            result=wr.setDischargeSlot2(payload)
    elif command=="setDischargeEnd2":
        if exists(regcache):
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            multi_output=regCacheStack[0]
            start=multi_output['Timeslots']['Discharge_start_time_slot_2']
            payload['finish']=message.payload.decode("utf-8")[:5]
            payload['start']=start[:5]
            result=wr.setDischargeSlot2(payload)
    #Do something with the result??

def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        logger.info("connected OK Returned code="+str(rc))
        #Subscribe to the control topic for this invertor - relies on serial_number being present
        client.subscribe(MQTT_Topic+"/control/"+GiV_Settings.serial_number+"/#")
        logger.info("Subscribing to "+MQTT_Topic+"/control/"+GiV_Settings.serial_number+"/#")
    else:
        logger.error("Bad connection Returned code= "+str(rc))


client=mqtt.Client("GivEnergy_GivTCP_"+str(GiV_Settings.givtcp_instance)+"_Control")
mqtt.Client.connected_flag=False        			#create flag in class
if MQTTCredentials:
    client.username_pw_set(MQTT_Username,MQTT_Password)
client.on_connect=on_connect     			        #bind call back function
client.on_message=on_message                        #bind call back function
#client.loop_start()

logger.info ("Connecting to broker(sub): "+ MQTT_Address)
client.connect(MQTT_Address,port=MQTT_Port)
client.loop_forever()