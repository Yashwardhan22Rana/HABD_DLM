"""
*****************************************************************************
*File : habd_dlm_main.py
*Module : habd_dlm
*Purpose : habd data logging module main class
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
"""

import time
import json
import sys
sys.path.append("..")  # parent folder where habd_common lives

# '''Import HABD packages '''
from habd_common.habd_log import Log
from habd_model import TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo
from habd_api import HabdAPI
from habd_dlm_conf import HabdDlmConfRead
from habd_common.MqttClient import MqttClient
from habd_health import Health
from habd_common.habd_event_error_pub import EventErrorPub


class DLMSub:
    # ''' DLM MQTT Subscribe class / methods '''
    def __init__(self, db_api_obj, habd_health_cls_obj):
        self.habd_api = db_api_obj
        self.habd_health = habd_health_cls_obj

    def dpu_pm_tpd_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_tpd_sub_fn : {message.payload}')
        self.habd_api.insert_train_processed_info(message.payload)

    def dpu_pm_tcd_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_tcd_sub_fn : {message.payload}')
        self.habd_api.insert_train_consolidated_info(message.payload)

    def dpu_event_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_event_sub_fn : topic: {message.topic}, {message.payload}')
        self.habd_api.insert_habd_event_info(message.payload)
        self.habd_health.process_health_events(message.payload)
    
    def dpu_error_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_error_sub_fn : topic: {message.topic}, {message.payload}')
        self.habd_api.insert_habd_error_info(message.payload)
        self.habd_health.process_health_errors(message.payload)

    def dpu_health_sub_fn(self, in_client, user_data, message):
        Log.logger.warning(f'dpu_health_sub_fn : {message.payload}')
        self.habd_api.insert_habd_health_info(message.payload)


if __name__ == '__main__':
    if Log.logger is None:
        Log("DLM")
    Log.logger.warning(
        f'\n************************************ DLM Started ******************************************')
    '''read configuration file'''
    cfg = HabdDlmConfRead()
    cfg.read_cfg('../config/habd_dlm.conf')

    '''Create MQTT Client object and connect '''
    mqtt_client = MqttClient(cfg.local_mqtt_broker.BROKER_IP_ADDRESS, cfg.local_mqtt_broker.PORT, "habd_dlm",
                             cfg.local_mqtt_broker.USERNAME, cfg.local_mqtt_broker.PASSWORD, 'habd_dlm')
    mqtt_client.connect()

    '''initialise habd_api and connect database'''
    db_api = HabdAPI(cfg, mqtt_client)
    psql_db = db_api.connect_database(cfg)

    '''Create database model'''
    if psql_db:
        psql_db.create_tables([TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo])

    eve_err_pub = EventErrorPub(mqtt_client, cfg.dpu_id)

    '''Health information'''
    habd_health = Health(mqtt_client, cfg.dpu_id, eve_err_pub)

    '''Create DLMSub class object'''
    dlm_sub = DLMSub(db_api, habd_health)

    '''Subscribe all required MQTT topics '''
    # mqtt_client.sub("dpu_pm/train_processed_info", dlm_sub.dpu_pm_tpd_sub_fn)
    # mqtt_client.sub("dpu_pm/train_consolidated_info", dlm_sub.dpu_pm_tcd_sub_fn)
    mqtt_client.sub("habd_pm/train_consolidated_info", dlm_sub.dpu_pm_tcd_sub_fn)
    mqtt_client.sub("habd_pm/train_processed_info", dlm_sub.dpu_pm_tpd_sub_fn)    
    # mqtt_client.sub("dpu_pm/events", dlm_sub.dpu_event_sub_fn)
    # mqtt_client.sub("dpu_pm/errors", dlm_sub.dpu_error_sub_fn)
    # mqtt_client.sub("dpu_dam/events", dlm_sub.dpu_event_sub_fn)
    # mqtt_client.sub("dpu_dam/errors", dlm_sub.dpu_error_sub_fn)
    # mqtt_client.sub("dpu_tdfm/events", dlm_sub.dpu_event_sub_fn)
    # mqtt_client.sub("dpu_tdfm/errors", dlm_sub.dpu_error_sub_fn)
    # mqtt_client.sub("dpu_dlm/events", dlm_sub.dpu_event_sub_fn)
    # mqtt_client.sub("dpu_dlm/errors", dlm_sub.dpu_error_sub_fn)
    # mqtt_client.sub("dpu_dm/events", dlm_sub.dpu_event_sub_fn)
    # mqtt_client.sub("dpu_dm/errors", dlm_sub.dpu_error_sub_fn)
    # mqtt_client.sub("dpu_cm/events", dlm_sub.dpu_event_sub_fn)
    # mqtt_client.sub("dpu_cm/errors", dlm_sub.dpu_error_sub_fn)
    # mqtt_client.sub("dpu_dam/health_info", dlm_sub.dpu_health_sub_fn)

    '''System reboot information'''
    habd_health.system_reboot_info()

    '''insert health status when program start or restart'''
    json_health_info = json.dumps(habd_health.health_info)
    db_api.insert_habd_health_info(json_health_info)
    
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        Log.logger.critical(f'Keyboard Interrupt occurred. Exiting the program')
