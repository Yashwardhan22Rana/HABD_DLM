'''
*****************************************************************************
*File : habd_event_error_pub.py
*Module : habd_common
*Purpose : habd data logging module (DLM) MQTT publish EVENT/ERROR class
*Author : HABD Team
*Copyright : Copyright 2025, Lab to Market Innovations Private Limited
*****************************************************************************
'''

# '''import python packages '''
import json
import time

# '''import habd packages'''
from habd_common.MqttClient import MqttClient
from habd_common.habd_log import Log


class EventErrorPub:

    INFO = 1
    WARNING = 2
    CRITICAL = 3

    '''Publish DLM Events and Errors'''

    def __init__(self, mq_client, dpu_id):
        if Log.logger is None:
            Log("event_err_pub")
        self.event_msg_id = 1
        self.error_msg_id = 1
        self.mqtt_client = mq_client
        self.dpu_id = dpu_id

    def publish_error_info(self, module_name, msg_error_id, msg_error_severity, msg_error_desc):
        '''
        JSON {
            “ts”: <UTC time>,
            “msg_id”: <int>,
            "dpu_id": <string>,
            “error_id”: <string>, # e.g .PM-ERROR-001
            “error_severity”: <int>, # e.g. 1 = “Info” or 2 = “Warning” or 3 = “Critical”
            “error_desc”: <string>
        }
        '''
        try:
            error_ts = round(time.time(), 6)
            error_id = str(msg_error_id)
            error_severity = int(msg_error_severity)
            error_desc = str(msg_error_desc)
            error_msg = {"msg_id": self.error_msg_id, "ts": error_ts, "dpu_id": self.dpu_id, "error_id": error_id,
                         "error_severity": error_severity, "error_desc": error_desc}
            error_json_msg = json.dumps(error_msg)
            topic = "dpu_" + module_name + "/errors"
            self.mqtt_client.pub(topic, error_json_msg)
            self.error_msg_id = self.error_msg_id + 1
            Log.logger.info(f'publish_error_info:\ntopic: {topic}\n Message: {error_json_msg}')
        except Exception as e:
            Log.logger.error(f'EventErrorPub: publish_error_info: {e}', exc_info=True)

    def publish_event_info(self, module_name, msg_event_id, msg_event_desc):
        '''
        JSON {
            “ts”: <UTC time>,
            "dpu_id": <string>,
            “msg_id”: <int>,
            “event_id”: <string>, # e.g .PM-EVENT-001
            “event_desc”: <string>
        }
        '''
        try:
            event_ts = round(time.time(), 6)
            event_id = str(msg_event_id)
            event_desc = str(msg_event_desc)
            event_msg = {"msg_id": self.event_msg_id, "ts": event_ts, "dpu_id": self.dpu_id, "event_id": event_id,
                         "event_desc": event_desc}
            event_json_msg = json.dumps(event_msg)
            topic = "dpu_" + module_name + "/events"
            self.mqtt_client.pub(topic, event_json_msg)
            self.event_msg_id = self.event_msg_id + 1
            Log.logger.info(f'publish_event_info:\ntopic: {topic}\n Message: {event_json_msg}')
        except Exception as e:
            Log.logger.error(f'EventErrorPub: publish_event_info: {e}', exc_info=True)


if __name__ == "__main__":
    if Log.logger is None:
        Log("event_err_pub")

    m_client = MqttClient("127.0.0.1", 1883, "dam-main", '', '', 'dam-main')
    m_client.connect()

    Log.logger.info('******************  In Main Program *******************')
