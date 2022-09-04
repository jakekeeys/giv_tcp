# -*- coding: utf-8 -*-
# version 2022.01.31
from concurrent.futures import thread
import sys
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from settings import GiV_Settings
from givenergy_modbus.client import GivEnergyClient
from datetime import timedelta
import threading, time
from os.path import exists
import pickle,os
from GivLUT import GivLUT


forcefullrefresh=GiV_Settings.cache_location+"/.forceFullRefresh_"+str(GiV_Settings.givtcp_instance)
regcache=GiV_Settings.cache_location+"/regCache_"+str(GiV_Settings.givtcp_instance)+".pkl"
schedule=".schedule"

logger = logging.getLogger("GivTCP_Write_"+str(GiV_Settings.givtcp_instance))
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


def enableChargeTarget(payload):
    temp={}
    try:
        if payload['state']=="enable":
            GivEnergyClient(host=GiV_Settings.invertorIP).enable_charge_target()
        elif payload['state']=="disable":
            GivEnergyClient(host=GiV_Settings.invertorIP).disable_charge_target()
        temp['result']="Setting Charge Target was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Charge Target failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def enableChargeSchedule(payload):
    temp={}
    try:
        if payload['state']=="enable":
            GivEnergyClient(host=GiV_Settings.invertorIP).enable_charge()
        elif payload['state']=="disable":
            GivEnergyClient(host=GiV_Settings.invertorIP).disable_charge()
        temp['result']="Setting Charge Enable was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Charge Enable failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def enableDischargeSchedule(payload):
    temp={}
    try:
        if payload['state']=="enable":
            GivEnergyClient(host=GiV_Settings.invertorIP).enable_discharge()
        elif payload['state']=="disable":
            GivEnergyClient(host=GiV_Settings.invertorIP).disable_discharge()
        temp['result']="Setting Charge Enable was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Charge Enable failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setShallowCharge(payload):
    temp={}
    try:
        GivEnergyClient(host=GiV_Settings.invertorIP).set_shallow_charge(int(payload['val']))
        temp['result']="Setting Charge Enable was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Charge Enable failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def enableDischarge(payload):
    temp={}
    try:
        if payload['state']=="enable":
            GivEnergyClient(host=GiV_Settings.invertorIP).set_shallow_charge(4)
        elif payload['state']=="disable":
            GivEnergyClient(host=GiV_Settings.invertorIP).set_shallow_charge(100)
        temp['result']="Setting Discharge Enable was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Discharge Enable failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setChargeTarget(payload):
    temp={}
    if type(payload) is not dict: payload=json.loads(payload)
    target=int(payload['chargeToPercent'])
    try:
        client=GivEnergyClient(host=GiV_Settings.invertorIP)
        client.enable_charge_target(target)
        temp['result']="Setting Charge Target was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Charge Target failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setBatteryReserve(payload):
    temp={}
    if type(payload) is not dict: payload=json.loads(payload)
    target=int(payload['dischargeToPercent'])
    #Only allow minimum of 4%
    if target<4: target=4
    logger.info ("Setting battery reserve target to: " + str(target))
    try:
        GivEnergyClient(host=GiV_Settings.invertorIP).set_battery_power_reserve(target)
        temp['result']="Setting Battery Reserve was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Battery Reserve failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setChargeRate(payload):
    temp={}
    if type(payload) is not dict: payload=json.loads(payload)
    #Only allow max of 100% and if not 100% the scale to a third to get register value
    if int(payload['chargeRate'])>=100:
        target=50
    else:
        target=int(int(payload['chargeRate'])/3)
    logger.info ("Setting battery charge rate to: " + str(target))
    try:
        GivEnergyClient(host=GiV_Settings.invertorIP).set_battery_charge_limit(target)
        temp['result']="Setting Charge Rate was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Charge Rate failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)


def setDischargeRate(payload):
    temp={}
    if type(payload) is not dict: payload=json.loads(payload)
    #Only allow max of 100% and if not 100% the scale to a third to get register value
    if int(payload['dischargeRate'])>=100:
        target=50
    else:
        target=int(int(payload['dischargeRate'])/3)
    logger.info ("Setting battery discharge rate to: " + str(target))
    try:
        GivEnergyClient(host=GiV_Settings.invertorIP).set_battery_discharge_limit(target)
        temp['result']="Setting Discharge Rate was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Discharge Rate failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)


def setChargeSlot1(payload):
    temp={}
    targetresult="Success"
    wintermoderesult="Success"
    if type(payload) is not dict: payload=json.loads(payload)
    if 'chargeToPercent' in payload.keys():
        targetresult=setChargeTarget(payload)
    client=GivEnergyClient(host=GiV_Settings.invertorIP)
    try:
        client.set_charge_slot_1((datetime.strptime(payload['start'],"%H:%M"),datetime.strptime(payload['finish'],"%H:%M")))
        temp['result']="Setting Charge Slot 1 was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Charge Slot 1 failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setChargeSlot2(payload):
    temp={}
    targetresult="Success"
    wintermoderesult="Success"
    if type(payload) is not dict: payload=json.loads(payload)
    if 'chargeToPercent' in payload.keys():
        targetresult=setChargeTarget(payload)
    client=GivEnergyClient(host=GiV_Settings.invertorIP)
    try:
        client.set_charge_slot_2((datetime.strptime(payload['start'],"%H:%M"),datetime.strptime(payload['finish'],"%H:%M")))
        temp['result']="Setting Charge Slot 2 was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Charge Slot 2 failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setDischargeSlot1(payload):
    temp={}
    targetresult="Success"
    wintermoderesult="Success"
    if type(payload) is not dict: payload=json.loads(payload)
    if 'dischargeToPercent' in payload.keys():
        targetresult=setBatteryReserve(payload)
    client=GivEnergyClient(host=GiV_Settings.invertorIP)
    try:
        client.set_discharge_slot_1((datetime.strptime(payload['start'],"%H:%M"),datetime.strptime(payload['finish'],"%H:%M")))
        temp['result']="Setting Discharge Slot 1 was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Discharge Slot 1 failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setDischargeSlot2(payload):
    temp={}
    targetresult="Success"
    if type(payload) is not dict: payload=json.loads(payload)
    if 'dischargeToPercent' in payload.keys():
        targetresult=setBatteryReserve(payload)
    client=GivEnergyClient(host=GiV_Settings.invertorIP)
    try:
        client.set_discharge_slot_2((datetime.strptime(payload['start'],"%H:%M"),datetime.strptime(payload['finish'],"%H:%M")))
        temp['result']="Setting Discharge Slot 2 was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Discharge Slot 2 failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def FEResume(revert):
    payload={}
    print("Reverting Force Export")
    print("Reverting Discharge Rate")
    payload['dischargeRate']=revert["dischargeRate"]
    result=setDischargeRate(payload)
    time.sleep(2)
    payload={}
    payload['start']=revert["start_time"]
    payload['finish']=revert["end_time"]
    payload['dischargeToPercent']=revert["dischargeToPercent"]
    result=setDischargeSlot2(payload)
    time.sleep(2)
    payload={}
    payload["mode"]=revert["mode"]
    print("Reverting Mode")
    result=setBatteryMode(payload)

    print("Settings restored to:")
    print("dischargeRate:"+str(revert["dischargeRate"]))
    print("mode:"+str(revert["mode"]))
    os.remove(".FERunning")

def forceExport(exportTime):
    temp={}
    logger.critical("Forcing Export for "+str(exportTime)+" minutes")
    try:
        exportTime=int(exportTime)
        payload={}
        result={}
        revert={}
        open(".FERunning", 'w').close()
        if exists(regcache):      # if there is a cache then grab it
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            revert["dischargeRate"]=regCacheStack[4]["Control"]["Battery_Discharge_Rate"]
            revert["start_time"]=regCacheStack[4]["Timeslots"]["Disharge_start_time_slot_1"]
            revert["end_time"]=regCacheStack[4]["Timeslots"]["Discharge_end_time_slot_1"]
            revert["dischargeToPercent"]=regCacheStack[4]["Control"]["Battery_Power_Reserve"]
            revert["mode"]=regCacheStack[4]["Control"]["Mode"]
        
        payload['dischargeRate']=100
        result=setDischargeRate(payload)

        payload={}
        payload['start']=GivLUT.getTime(datetime.now())
        payload['finish']=GivLUT.getTime(datetime.now()+timedelta(minutes=exportTime))
        payload['dischargeToPercent']=100
        result=setDischargeSlot2(payload)
        
        payload={}
        payload['mode']="Timed Export"
        result=setBatteryMode(payload)
        
        if "success" in result:
            delay=float(exportTime*60)
            Th = threading.Timer(delay,FEResume,args=[revert])        ########## Needs helper function to revert rate, log and update the device status
            Th.start()
            temp['result']="Export successfully forced "+str(exportTime)+" minutes"
        else:
            temp['result']="Force Export failed"
    except:
        e = sys.exc_info()
        temp['result']="Force Export failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def FCResume(revert):
    
    payload['chargeRate']=revert["chargeRate"]
    result=setChargeRate(payload)
    time.sleep(2)
    payload={}
    payload['state']=revert["chargeScheduleEnable"]
    result=enableChargeSchedule(payload)
    time.sleep(2)
    payload={}
    payload['start']=revert["start_time"]
    payload['finish']=revert["end_time"]
    payload['chargeToPercent']=revert["targetSOC"]
    result=setChargeSlot1(payload)

    logger.info("Charging Settings restored to:")
    logger.info("chargeRate:"+str(payload["chargeRate"]))
    logger.info("Discharge Schedule:"+str(payload["state"]))
    logger.info("Start:"+str(payload["start"]))
    logger.info("finish:"+str(payload["finish"]))
    logger.info("Target SOC:"+str(payload["chargeToPercent"]))
    os.remove(".FCRunning")

def forceCharge(chargeTime):
    temp={}
    logger.critical("Forcing Charge for "+str(chargeTime)+" minutes")
    try:
        chargeTime=int(chargeTime)
        payload={}
        result={}
        revert={}
        open(".FCRunning", 'w').close()
        if exists(regcache):      # if there is a cache then grab it
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            revert["start_time"]=regCacheStack[4]["Timeslots"]["Charge_start_time_slot_1"]
            revert["end_time"]=regCacheStack[4]["Timeslots"]["Charge_end_time_slot_1"]
            revert["chargeRate"]=regCacheStack[4]["Control"]["Battery_Charge_Rate"]
            revert["targetSOC"]=regCacheStack[4]["Control"]["Target_SOC"]
            revert["chargeScheduleEnable"]=regCacheStack[4]["Control"]["Enable_Charge_Schedule"]
        
        payload['chargeRate']=100
        result=setChargeRate(payload)

        payload={}
        payload['state']="enable"
        result=enableChargeSchedule(payload)

        payload={}
        payload['start']=GivLUT.getTime(datetime.now())
        payload['finish']=GivLUT.getTime(datetime.now()+timedelta(minutes=chargeTime))
        payload['chargeToPercent']=100
        result=setChargeSlot1(payload)
        
        if "success" in result:
            delay=float(chargeTime*60)
            Th = threading.Timer(delay,FCResume,args=[revert])        ########## Needs helper function to revert rate, log and update the device status
            Th.start()
            temp['result']="Charge successfully forced "+str(chargeTime)+" minutes"
        else:
            temp['result']="Force charge failed"
    except:
        e = sys.exc_info()
        temp['result']="Force charge failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def tmpPDResume(payload):
    result=setDischargeRate(payload)
    logger.info("Discharge Rate restored to: "+str(payload["dischargeRate"]))
    if exists(".tpdRunning"): os.remove(".tpdRunning")

def tempPauseDischarge(pauseTime):
    temp={}
    try:
        pauseTime=int(pauseTime)
        payload={}
        result={}
        payload['dischargeRate']=0
        result=setDischargeRate(payload)
        logger.critical("Pausing Discharge for "+str(pauseTime)+" minutes")
        #Update read data via pickle
        if exists(regcache):      # if there is a cache then grab it
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
            revertRate=regCacheStack[4]["Control"]["Battery_Discharge_Rate"]
        open(".tpdRunning","w").close()
        if "success" in result:
            payload['dischargeRate']=revertRate
            delay=float(pauseTime*60)
            Th = threading.Timer(delay,tmpPDResume,args=[payload])        ########## Needs helper function to revert rate, log and update the device status
            Th.start()
            temp['result']="Discharge paused for "+str(delay)+" seconds"
        else:
            temp['result']="Pausing Discharge failed"
    except:
        e = sys.exc_info()
        temp['result']="Pausing Discharge failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def tmpPCResume(payload):
    result=setChargeRate(payload)
    logger.info("Charge Rate restored to: "+str(payload["chargeRate"]))
    if exists(".tpcRunning"): os.remove(".tpcRunning")

def tempPauseCharge(pauseTime):
    temp={}
    try:
        payload={}
        result={}
        payload['chargeRate']=0
        result=setChargeRate(payload)
        print (result)
        #Update read data via pickle
        if exists(regcache):      # if there is a cache then grab it
            with open(regcache, 'rb') as inp:
                regCacheStack= pickle.load(inp)
        revertRate=regCacheStack[4]["Control"]["Battery_Charge_Rate"]
        open(".tpcRunning","w").close()
        if "success" in result:
            payload['chargeRate']=revertRate
            delay=float(pauseTime*60)
            Th = threading.Timer(delay,tmpPCResume,args=[payload])
            Th.start()
            temp['result']="Charge paused for "+str(delay)+" seconds"
        else:
            temp['result']="Pausing Charge failed: "
        print(temp)
    except:
        e = sys.exc_info()
        temp['result']="Pausing Charge failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setBatteryMode(payload):
    temp={}
    if type(payload) is not dict: payload=json.loads(payload)
    try:
        if payload['mode']=="Eco":
            client=GivEnergyClient(host=GiV_Settings.invertorIP).set_mode_dynamic()
        elif payload['mode']=="Eco (Paused)":
            client=GivEnergyClient(host=GiV_Settings.invertorIP).set_mode_dynamic()
            client=GivEnergyClient(host=GiV_Settings.invertorIP).set_shallow_charge(100)
        elif payload['mode']=="Timed Demand":
            client=GivEnergyClient(host=GiV_Settings.invertorIP).set_mode_storage()
            GivEnergyClient(host=GiV_Settings.invertorIP).enable_discharge()
        elif payload['mode']=="Timed Export":
            client=GivEnergyClient(host=GiV_Settings.invertorIP).set_mode_storage(export=True)
            GivEnergyClient(host=GiV_Settings.invertorIP).enable_discharge()
        else:
            logger.info ("Invalid Mode requested: "+ payload['mode'])
            temp['result']="Invalid Mode requested"
            return json.dumps(temp)
        temp['result']="Setting Battery Mode was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Battery Mode failed: " + str(e)
        logger.error (temp['result'])
    return json.dumps(temp)

def setDateTime(payload):
    temp={}
    targetresult="Success"
    if type(payload) is not dict: payload=json.loads(payload)
    #convert payload to dateTime components
    try:
        iDateTime=datetime.strptime(payload['dateTime'],"%d/%m/%Y %H:%M:%S")   #format '12/11/2021 09:15:32'
        #Set Date and Time on Invertor
        GivEnergyClient(host=GiV_Settings.invertorIP).set_datetime(iDateTime)
        temp['result']="Invertor time setting was a success"
        open(forcefullrefresh, 'w').close()
    except:
        e = sys.exc_info()
        temp['result']="Setting Battery Mode failed: " + str(e) 
        logger.error (temp['result'])
    return json.dumps(temp)

if __name__ == '__main__':
    if len(sys.argv)==2:
        globals()[sys.argv[1]]()
    elif len(sys.argv)==3:
        globals()[sys.argv[1]](sys.argv[2])
