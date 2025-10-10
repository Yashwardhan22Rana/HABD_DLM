"""
*****************************************************************************
*File : habd_dlm_conf.py
*Module : habd_dlm
*Purpose : habd data logging module (DLM) configuration class
*Author : HABD Team
*Copyright : Copyright 2025, Lab to Market Innovations Private Limited
*****************************************************************************
"""

# '''import python packages'''
import sys
import json
from os import path
from typing import NamedTuple

import json_checker
from json_checker import Checker
from json_checker.core.exceptions import CheckerError

from habd_common.habd_log import Log


class HabdDlmConfRead:
    schema = {
        "COMMENT": str,
        "VERSION": str,
        "DPU_ID": str,
        "DPU_LOCATION": str,
        "DATABASE": {
            "PROVIDER": str,
            "USER": str,
            "PASSWORD": str,
            "HOST": str,
            "DB_NAME": str
        },

        "LOCAL_MQTT_BROKER": {
            "BROKER_IP_ADDRESS": str,
            "USERNAME": str,
            "PASSWORD": str,
            "PORT": int
        }
    }

    def __init__(self):
        self.comment = None
        self.version = None
        self.dpu_id = None
        self.database = None
        self.local_mqtt_broker = None
        self.json_data = None

    def read_cfg(self, file_name):
        if path.exists(file_name):
            with open(file_name) as f:
                try:
                    self.json_data = json.load(f)
                    Log.logger.info(f'Configuration File: {file_name} loaded successfully\n {self.json_data}')
                except json.JSONDecodeError as jex:
                    Log.logger.critical(f'{file_name} does not have valid Json Config\n{jex}\n  Program terminated',
                                        exc_info=True)
                    sys.exit(2)
        else:
            Log.logger.critical(f'{file_name} not found.  Program terminated')
            sys.exit(1)
        try:
            checker = Checker(HabdDlmConfRead.schema)
            result = checker.validate(self.json_data)
            Log.logger.info(f'{file_name} Checked OK. Result: {result}')
        except json_checker.core.exceptions.DictCheckerError as err:
            Log.logger.critical(f'{file_name} is not valid {err}', exc_info=True)
            sys.exit(3)
        try:
            self.comment = self.json_data['COMMENT']
            self.version = self.json_data['VERSION']
            self.dpu_id = self.json_data['DPU_ID']

            self.database = DatabaseStruct(**self.json_data['DATABASE'])
            self.local_mqtt_broker = LocalMQTTStruct(**self.json_data['LOCAL_MQTT_BROKER'])
            Log.logger.warning(f'Configuration File: {file_name} Read successfully')
            # Log.logger.warning(
            #     f'\n ------------------------------------------------------------'
            #     f'\n Inputs read from {file_name}'
            #     f'\n ------------------------------------------------------------'
            #     f'\n comment         = {self.comment}'
            #     f'\n Version         = {self.version}'
            #     f'\n dpu_id          = {self.dpu_id}'
            #     f'\n DB_PROVIDER     = {self.database.PROVIDER}'
            #     f'\n DB_USER         = {self.database.USER}'
            #     f'\n DB_HOST         = {self.database.HOST}'
            #     f'\n DB_NAME         = {self.database.DB_NAME}\n'
            #     f'\n BROKER_IP_ADDR  = {self.local_mqtt_broker.BROKER_IP_ADDRESS}'
            #     f'\n USERNAME        = {self.local_mqtt_broker.USERNAME}'
            #     f'\n PORT            = {self.local_mqtt_broker.PORT}'
            #     f'\n ------------------------------------------------------------'
            # )
        except KeyError as jex:
            Log.logger.critical(f'{file_name} do not have the data: {jex}', exc_info=True)
            sys.exit(3)


class DatabaseStruct(NamedTuple):
    PROVIDER: str
    USER: str
    PASSWORD: str
    HOST: str
    DB_NAME: str


class LocalMQTTStruct(NamedTuple):
    BROKER_IP_ADDRESS: str
    USERNAME: str
    PASSWORD: str
    PORT: int


if __name__ == "__main__":
    if Log.logger is None:
        Log("habd_dlm_conf")

    cfg = HabdDlmConfRead()
    cfg.read_cfg('../config/habd_dlm.conf')

    Log.logger.info('******************  In Main Program *******************')
    Log.logger.info(f'DATABASE: {cfg.database} \n')
