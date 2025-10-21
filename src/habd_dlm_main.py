'''
*****************************************************************************
* File        : habd_dlm_main.py
* Module      : habd_dlm
* Version     : 1.2
* Date        : 28/04/2025
* Description : HABD Data Logging Module Main Class
* Author      : Akash B, Sumankumar Panchal, Kausthubha N K
* Copyright   : Copyright 2025, Lab to Market Innovations Private Limited
*               Bangalore
* Change Log  :
*   Version   Date        Author     Description
*   -------   ----------  ---------  ----------------------------------------
*   1.0       28/04/2025  Akash B    Initial version
*   1.1       15/07/2025  Sumankumar Panchal    Initial version
*   1.2       10/09/2025  Kausthubha N K    Standardization and review
*****************************************************************************
'''

import time
import json
import sys
sys.path.append("..")  # parent folder where habd_common lives

# '''Import HABD packages '''
from habd_log import Log
from habd_model import TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo
from habd_api import HabdAPI
from habd_dlm_conf import HabdDlmConfRead
from mqtt_client import *
from datetime import datetime


from habd_health import Health
from habd_event_error_pub import EventErrorPub
sys.path.insert(1, "/home/l2m/habd-v1/src/habd_common")

class DLMSub:
    # ''' DLM MQTT Subscribe class / methods '''
    def __init__(self, db_api_obj, habd_health_cls_obj):
        self.habd_api = db_api_obj
        self.habd_health = habd_health_cls_obj

    def dpu_pm_tpd_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_tpd_sub_fn : {message.payload}')
        try:
            self.habd_api.insert_train_processed_info(message.payload)
        except Exception as e:
            Log.logger.error(f'Error processing train processed info: {e}')

    def dpu_pm_tcd_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_tcd_sub_fn : {message.payload}')
        try:
            self.habd_api.insert_train_consolidated_info(message.payload)
        except Exception as e:
            Log.logger.error(f'Error processing train consolidated info: {e}')

    def dpu_pm_habd_info_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_habd_info_sub_fn : {message.payload}')
        try:
            self.habd_api.insert_habd_temp_info(message.payload)
        except Exception as e:
            Log.logger.error(f'Error processing HABD info: {e}')

    def dpu_event_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_event_sub_fn : topic: {message.topic}, {message.payload}')
        try:
            self.habd_api.insert_habd_event_info(message.payload)
            self.habd_health.process_health_events(message.payload)
        except Exception as e:
            Log.logger.error(f'Error processing event: {e}')
    
    def dpu_error_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_error_sub_fn : topic: {message.topic}, {message.payload}')
        try:
            self.habd_api.insert_habd_error_info(message.payload)
            self.habd_health.process_health_errors(message.payload)
        except Exception as e:
            Log.logger.error(f'Error processing error: {e}')

    def dpu_health_sub_fn(self, in_client, user_data, message):
        Log.logger.warning(f'dpu_health_sub_fn : {message.payload}')
        try:
            self.habd_api.insert_habd_health_info(message.payload)
        except Exception as e:
            Log.logger.error(f'Error processing health info: {e}')


if __name__ == '__main__':
    if Log.logger is None:
        Log('dlm')
    Log.logger.info("======================================================================")
    Log.logger.info(f"START OF HABD DATA LOGGING MODULE : {datetime.now()}")
    Log.logger.info("======================================================================")
    '''read configuration file'''
    cfg = HabdDlmConfRead()
    cfg.read_cfg('/home/l2m/habd-v1/config/habd_dlm.conf')

    '''Create MQTT Client object and connect '''
    mqtt_client = MqttClient(cfg.local_mqtt_broker.BROKER_IP_ADDRESS, cfg.local_mqtt_broker.PORT, "habd_dlm",
                             cfg.local_mqtt_broker.USERNAME, cfg.local_mqtt_broker.PASSWORD, 'habd_dlm')
    mqtt_client.connect()

    '''initialise habd_api and connect database'''
    db_api = HabdAPI(cfg, mqtt_client)
    psql_db = db_api.connect_database(cfg)

    '''Create database model'''
    if psql_db:
        try:
            psql_db.create_tables([TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo])
            Log.logger.info("Database tables created/verified successfully")
        except Exception as e:
            Log.logger.error(f"Error creating tables: {e}")

    eve_err_pub = EventErrorPub(mqtt_client, cfg.dpu_id)

    '''Health information'''
    habd_health = Health(mqtt_client, cfg.dpu_id, eve_err_pub)

    '''Create DLMSub class object'''
    dlm_sub = DLMSub(db_api, habd_health)

    '''Subscribe all required MQTT topics '''
    mqtt_client.sub("habd_pm/train_consolidated_info", dlm_sub.dpu_pm_tcd_sub_fn)
    mqtt_client.sub("habd_pm/train_processed_info", dlm_sub.dpu_pm_tpd_sub_fn)
    mqtt_client.sub("dpu_pm/habd_info", dlm_sub.dpu_pm_habd_info_sub_fn)  # Add this for temperature data

    '''System reboot information'''
    habd_health.system_reboot_info()

    '''insert health status when program start or restart'''
    try:
        json_health_info = json.dumps(habd_health.health_info)
        db_api.insert_habd_health_info(json_health_info)
        Log.logger.info("Initial health info inserted successfully")
    except Exception as e:
        Log.logger.error(f"Error inserting initial health info: {e}")
    
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        Log.logger.critical(f'Keyboard Interrupt occurred. Exiting the program')
    except Exception as e:
        Log.logger.critical(f'Unexpected error occurred: {e}')