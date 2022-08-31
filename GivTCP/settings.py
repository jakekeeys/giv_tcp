# version 2022.01.31
class GiV_Settings:
    invertorIP="192.168.2.3"        #Required - IP address of Invertor on local network
    numBatteries=1                  #Required - The number of batteries connected this invertor
    HA_Auto_D=True                  #Optional - Bool - Publishes Home assistant MQTT Auto Discovery messages to push data into HA automagically (requires MQTT to be enabled below)
    ha_device_prefix=""             #Required - This is the prefix used at the start of every Home Assistant device created
#Debug Settings
    Log_Level="Error"               #Optional - Enables logging level. Default is "Error", but can be "Info" or "Debug"
    Debug_File_Location=""          #Optional - Location of logs (Default is console)
    Print_Raw_Registers=True        #Optional - Bool - True publishes all available registers.
#MQTT Output Settings
    MQTT_Output= True               #Optional - Bool - True or False
    MQTT_Address="192.168.2.83"     #Optional - (Required is above set to True) IP address of MQTT broker (local or remote)
    MQTT_Username="mqtt"            #Optional - Username for MQTT broker
    MQTT_Password="mqtt2020"        #Optional - Password for MQTT broker
    MQTT_Topic=""         #Optional - Root topic for all MQTT messages. Defaults to "GivEnergy/<SerialNumber> 
    MQTT_Port=1883                  #Optional - Int - define port that MQTT broker is listening on (default 1883)
#Influx Settings - Only works with Influx v2
    Influx_Output= False            #True or False
    influxURL="http://localhost:8086"
    influxToken=""
    influxBucket="GivEnergy"
    influxOrg="GivTCP"
    first_run= False
    serial_number = "SA2047G098"
    self_run_timer = 5
    default_path = "GivTCP"
    givtcp_instance=1
    day_rate=0.155
    day_rate_start="00:30"
    night_rate=0.055
    night_rate_start="00:00"
    export_rate=0.055
    givtcp_instance=1
    cache_location="C:/Users/mark/Code-Dev/givtcp/givTCP_stable/giv_tcp"