'''
*****************************************************************************
*File : habd_dlm_main.py
*Module : habd_dlm
*Purpose : habd health class
*Author : HABD Team
*Copyright : Copyright 2025, Lab to Market Innovations Private Limited
*****************************************************************************
'''

import time
import json
import os
import subprocess
import sys
sys.path.append("..")  # parent folder where habd_common lives

# '''Import HABD packages '''
from habd_dlm_conf import HabdDlmConfRead
from habd_common.habd_log import Log
from habd_model import TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo
from habd_api import HabdAPI
from habd_common.MqttClient import MqttClient
from habd_common.habd_event_error_pub import EventErrorPub

class Health:
    def __init__(self, mqtt_client, dpu_id_param, eve_err_pub_obj):
        self.mqtt_client = mqtt_client
        self.event_error_pub = eve_err_pub_obj
        self.health_info = {}
        self.health_info["ts"] = 0.0
        self.health_info["dpu_id"] = dpu_id_param 
        self.health_info["comm_link"] = "up" 
        self.health_info["interrogator_link"] = "up" 
        self.health_info["S1"] = "up"
        self.health_info["S2"] = "up"
        self.health_info["S3"] = "up"
        self.health_info["S4"] = "up"
        self.health_info["S5"] = "up"
        self.health_info["S6"] = "up"
        self.health_info["S7"] = "up"
        self.health_info["S8"] = "up"
        self.health_info["S9"] = "up" 
        self.health_info["S10"] = "up"
        self.health_info["S11"] = "up"
        self.health_info["S12"] = "up"
        self.health_info["T1"] = "up"
        self.health_info["T2"] = "up"
        self.health_info["T3"] = "up"
        self.health_info["T4"] = "up"

        '''set flag as per interrogator link status'''
        self.interrogator_link_flag = False

    def process_health_errors(self, msg):
        '''process health info related errors'''
        try: 
            error_msg = json.loads(msg)
            
            if error_msg["error_id"] == "DAM-ERROR-001":
                if self.interrogator_link_flag == False:
                    self.health_info["interrogator_link"] = "down"
                    self.health_info["ts"] = time.time()
                    
                    health_info_dict_key_list = list(self.health_info.keys())
                   
                    '''FBG sensors also down due to interrogator link fail'''
                    for health_idx in self.health_info:
                        if health_idx not in ['ts', 'dpu_id', 'interrogator_link', 'comm_link']:
                            self.health_info[health_idx] = "down"
                        else:
                            pass
                    
                    self.publish_health_info(self.health_info)
                    Log.logger.info(f'interrogator_link: {self.health_info}')

                    self.interrogator_link_flag = True
                else:
                    pass
            
            elif error_msg["error_id"] == "DAM-ERROR-002":
                error_desc = error_msg["error_desc"]
                splited_error_desc_list = error_desc.split(":")
                faulty_sensor_list = str(splited_error_desc_list[1]).split(" ")
                
                health_info_dict_key_list = list(self.health_info.keys())
                
                for health_idx in self.health_info:
                    if health_idx not in ['ts', 'dpu_id', 'interrogator_link', 'comm_link']:
                        self.health_info[health_idx] = "up"
                    else:
                        pass
                
                for sensor_idx in faulty_sensor_list:
                    new_sensor_idx = sensor_idx.replace("S0","S")
                    self.health_info[new_sensor_idx] = "down"
               
                self.health_info["ts"] = time.time()

                self.publish_health_info(self.health_info)
                Log.logger.info(f'{self.health_info}')
            
            elif error_msg['error_id'] in ["CM-ERROR-001", "CM-ERROR-002"]:
                self.health_info["comm_link"] = "down"
                self.health_info["ts"] = time.time()
                self.publish_health_info(self.health_info)
            else:
                pass
        except Exception as ex:
            Log.logger.critical(f'process_health_errors: exception : {ex}', exc_info=True)

    def process_health_events(self, msg):
        '''process health info related events'''
        try:
            event_msg = json.loads(msg) 
            if event_msg["event_id"] == "DAM-EVENT-001":
                if self.interrogator_link_flag == True:
                    self.health_info["interrogator_link"] = "up"
                    self.health_info["ts"] = time.time()

                    health_info_dict_key_list = list(self.health_info.keys())
                    for health_idx in self.health_info:
                        if health_idx not in ['ts', 'dpu_id', 'interrogator_link', 'comm_link']:
                            self.health_info[health_idx] = "up"
                        else:
                            pass
                    self.publish_health_info(self.health_info)
                    self.interrogator_link_flag = False
                else:
                    pass
            elif event_msg['event_id'] == "DAM-EVENT-002":
                self.health_info["interrogator_link"] = "up"
                self.health_info["ts"] = time.time()
                for health_idx in self.health_info:
                    if health_idx not in ['ts', 'dpu_id', 'interrogator_link', 'comm_link']:
                        self.health_info[health_idx] = "up"
                    else:
                        pass
                self.publish_health_info(self.health_info)
            elif event_msg['event_id'] == "CM-EVENT-001":
                self.health_info["comm_link"] = "up"
                self.health_info["ts"] = time.time()
                self.publish_health_info(self.health_info)
            else:
                pass
        except Exception as ex:
            Log.logger.critical(f'process_health_events: exception : {ex}', exc_info=True)
    
    def publish_health_info(self, data):
        '''publish health info'''
        try:
            json_data = json.dumps(data)
            self.mqtt_client.pub("dpu_dam/health_info", json_data) 
        except Exception as ex:
            Log.logger.critical(f'publish_health_info: exception: {ex}', exc_info=True)

    def system_reboot_info(self):
        '''get system reboot time'''
        try:
            filename = "../log/reboot.log"
            tmp_cmd_output = subprocess.check_output(
            'last reboot | grep running | head -n 1 | awk \'{print $5,$6,$7,$8}\'',
            shell=True,
            )

            if os.path.exists(filename):
                with open(filename,'r',encoding = 'utf-8') as f:
                    last_reboot_time = f.readline()
                    Log.logger.warning(f'Last reboot time recorded in the reboot.log: {last_reboot_time}')
                    
                    if len(tmp_cmd_output) != 0:
                        reboot_time = tmp_cmd_output.decode("utf-8").replace("\n","")

                        if reboot_time == last_reboot_time:
                            pass 
                        else:
                            reboot_event_msg = "DPU last rebooted at:" + reboot_time
                            with open(filename,'w',encoding = 'utf-8') as f:
                                f.write(reboot_time)
                                Log.logger.warning(f'{filename} created')

                            if self.event_error_pub is not None:
                                self.event_error_pub.publish_event_info("dlm", "DLM-EVENT-001", reboot_event_msg)
                    else:
                        pass
            else:

                if len(tmp_cmd_output) != 0:
                    reboot_time = tmp_cmd_output.decode("utf-8").replace("\n","")
                    Log.logger.warning(f'DPU last rebooted at: {reboot_time}')
                    reboot_event_msg = "DPU last rebooted at:" + reboot_time

                    with open(filename,'w',encoding = 'utf-8') as f:
                        f.write(reboot_time)
                        Log.logger.warning(f'reboot.log created')

                    if self.event_error_pub is not None:
                        self.event_error_pub.publish_event_info("dlm", "DLM-EVENT-001", reboot_event_msg)
                else:
                    pass
        except Exception as ex:
            Log.logger.critical(f'system_reboot_info: exception: {ex}', exc_info=True)


if __name__ == '__main__':
    if Log.logger is None:
        Log("DLM")

    '''read configuration file'''
    cfg = HabdDlmConfRead()
    cfg.read_cfg('../config/habd_dlm.conf')

    '''Create MQTT Client object and connect '''
    mqtt_client = MqttClient(cfg.local_mqtt_broker.BROKER_IP_ADDRESS, cfg.local_mqtt_broker.PORT, "habd_dlm",
                             cfg.local_mqtt_broker.USERNAME, cfg.local_mqtt_broker.PASSWORD, 'habd_dlm')
    mqtt_client.connect()

    '''Health information'''
    habd_health = Health(mqtt_client, cfg.dpu_id)

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        Log.logger.critical(f'Keyboard Interrupt occurred. Exiting the program')
