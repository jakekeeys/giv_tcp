# version 2022.01.21
import paho.mqtt.client as mqtt
import time
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
from settings import GiV_Settings
import sys
#from HA_Discovery import HAMQTT
from givenergy_modbus.model.inverter import Model

logger = logging.getLogger("GivTCP_MQTT_"+str(GiV_Settings.givtcp_instance))
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


class GivMQTT():

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

    def on_connect(client, userdata, flags, rc):
        if rc==0:
            client.connected_flag=True #set flag
            logger.info("connected OK Returned code="+str(rc))
            #client.subscribe(topic)
        else:
            logger.info("Bad connection Returned code= "+str(rc))
    
    def multi_MQTT_publish(rootTopic,array):   #Recieve multiple payloads with Topics and publish in a single MQTT connection
        mqtt.Client.connected_flag=False        			#create flag in class
        client=mqtt.Client("GivEnergy_GivTCP_"+str(GiV_Settings.givtcp_instance))
        
        ##Check if first run then publish auto discovery message
        
        if GivMQTT.MQTTCredentials:
            client.username_pw_set(GivMQTT.MQTT_Username,GivMQTT.MQTT_Password)
        try:
            client.on_connect=GivMQTT.on_connect     			#bind call back function
            client.loop_start()
            logger.info ("Connecting to broker: "+ GivMQTT.MQTT_Address)
            client.connect(GivMQTT.MQTT_Address,port=GivMQTT.MQTT_Port)
            while not client.connected_flag:        			#wait in loop
                logger.info ("In wait loop")
                time.sleep(0.2)
            for p_load in array:
                payload=array[p_load]
                logger.info('Publishing: '+rootTopic+p_load)
                output=GivMQTT.iterate_dict(payload,rootTopic+p_load)   #create LUT for MQTT publishing
                for value in output:
                    client.publish(value,output[value])
            client.loop_stop()                      			#Stop loop
            client.disconnect()
        except:
            e = sys.exc_info()
            logger.error("Error connecting to MQTT Broker: " + str(e))
        return client

    def iterate_dict(array,topic):      #Create LUT of topics and datapoints
        MQTT_LUT={}
        if isinstance(array, dict):
            # Create a publish safe version of the output
            for p_load in array:
                output=array[p_load]
                if isinstance(output, dict):
                    MQTT_LUT.update(GivMQTT.iterate_dict(output,topic+"/"+p_load))
                    logger.info('Prepping '+p_load+" for publishing")
                else:
                    MQTT_LUT[topic+"/"+p_load]=output
        else:
            MQTT_LUT[topic]=array
        return(MQTT_LUT)