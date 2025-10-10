'''
*****************************************************************************
*File : habd_api.py
*Module : habd_dlm
*Purpose : habd data logging module API class for database operations
*Author : HABD Team
*Copyright : Copyright 202, Lab to Market Innovations Private Limited
*****************************************************************************
'''

# '''Import python packages'''
import sys
import json
import time
from datetime import datetime, timedelta
from peewee import *

# '''Import HABD packages '''
from habd_model import TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo

from habd_dlm_conf import HabdDlmConfRead
from habd_common.habd_event_error_pub import EventErrorPub
from habd_common.habd_log import Log
from habd_common.MqttClient import MqttClient


class HabdAPI:
    '''HABD Database operations such as Select, Insert, Delete records'''

    def __init__(self, cfg_obj, mq_client):
        self.mqtt_client = mq_client
        self.event_msg_id = 0
        self.error_msg_id = 0
        self.dpu_id = cfg_obj.dpu_id
        self.dlm_pub = EventErrorPub(mq_client, self.dpu_id)

    def connect_database(self, config):
        '''Establish connection with database'''
        try:
            db_name = config.database.DB_NAME
            user = config.database.USER
            password = config.database.PASSWORD
            host = config.database.HOST
            port = 5432

            if len(db_name) == 0:
                Log.logger.critical("habd_api: connect_database:  database name missing")
                self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-011", EventErrorPub.CRITICAL,
                                                "habd_api: connect_database: database name missing")
            else:
                psql_db = PostgresqlDatabase(db_name, user=user, password=password, host=host, port=port)
                if psql_db:
                    try:
                        psql_db.connect()
                        Log.logger.info(f'habd_api: database connection successful')
                        return psql_db
                    except Exception as e:
                        Log.logger.critical(f'habd_api: connect_database: {e}', exc_info=True)
                        self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-012", EventErrorPub.CRITICAL,
                                                        "habd_api: connect_database:" + str(e))
                        sys.exit(1)
                else:
                    return None

        except Exception as e:
            Log.logger.critical(f"habd_api: connect_database: Exception: {e}", exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-013", EventErrorPub.CRITICAL,
                                            "habd_api: connect_database: Exception: " + str(e))

    def insert_train_processed_info(self, data):
        '''insert train processed info in database table'''
        try:
            TrainProcessedInfo()
            json_data = json.loads(data)

            temp_diff = None
            max_left_temp = None
            max_right_temp = None
            max_temp_difference = None
            wheel_status_left = 1
            wheel_status_right = 1

            # Filter out invalid temperature readings (-1 values)
            valid_left_temps = [temp for temp in json_data.get("temp_lefts", []) if temp != -1]
            valid_right_temps = [temp for temp in json_data.get("temp_rights", []) if temp != -1]

            # Calculate the maximum values across all VALID axles
            max_left_temp = max(valid_left_temps) if valid_left_temps else None
            max_right_temp = max(valid_right_temps) if valid_right_temps else None

            # Calculate max temperature difference
            valid_temp_differences = []
            temp_lefts = json_data.get("temp_lefts", [])
            temp_rights = json_data.get("temp_rights", [])

            for left, right in zip(temp_lefts, temp_rights):
                if left != -1 and right != -1:
                    valid_temp_differences.append(abs(left - right))
                elif left != -1 and right == -1:
                    valid_temp_differences.append(abs(left))
                elif right != -1 and left == -1:
                    valid_temp_differences.append(abs(right))

            max_temp_difference = max(valid_temp_differences) if valid_temp_differences else None

            '''Check train_consolidated info is available for the given train_id'''
            train_record = self.select_train_consolidated_info(json_data["train_id"])

            if len(train_record) == 0:
                Log.logger.critical(f'Train consolidated info for {json_data["train_id"]} - not found')
            else:
                Log.logger.info(f'Train consolidated info for {json_data["train_id"]} - found')

                '''list'''
                list_tuple = []
                d = []
                for i in range(len(json_data["axle_ids"])):
                    
                    if i < len(json_data["temp_lefts"]) and i < len(json_data["temp_rights"]):
                        left_temp = json_data["temp_lefts"][i] if json_data["temp_lefts"][i] != -1 else None
                        right_temp = json_data["temp_rights"][i] if json_data["temp_rights"][i] != -1 else None

                        if left_temp is not None and right_temp is not None:
                            temp_diff = round(abs(left_temp - right_temp), 2)
                        elif left_temp is not None:
                            temp_diff = round(abs(left_temp), 2)
                        elif right_temp is not None:
                            temp_diff = round(abs(right_temp), 2)
                        else:
                            temp_diff = None

                    d.append(json_data["ts"])
                    d.append(json_data["train_id"])
                    '''dpu_id insert internally with every record'''
                    d.append(json_data["dpu_id"])
                    d.append(json_data["axle_ids"][i])
                    d.append(json_data["axle_speeds"][i])
                    d.append(json_data["rake_ids"][i])
                    d.append(wheel_status_left)
                    d.append(wheel_status_right)
                    d.append(json_data["temp_lefts"][i])
                    d.append(json_data["temp_rights"][i])
                    d.append(round(temp_diff, 2))
                    d.append(max_left_temp)
                    d.append(max_right_temp)
                    d.append(round(max_temp_difference, 2))
                    t = tuple(d)
                    list_tuple.append(t)
                    d.clear()

                # Log.logger.info(f'Insert data information: {list_tuple}')

                TrainProcessedInfo.insert_many(list_tuple, fields=[
                    TrainProcessedInfo.ts, TrainProcessedInfo.train_id, TrainProcessedInfo.dpu_id,
                    TrainProcessedInfo.axle_id, TrainProcessedInfo.axle_speed,
                    TrainProcessedInfo.rake_id,
                    TrainProcessedInfo.wheel_status_left, TrainProcessedInfo.wheel_status_right,
                    TrainProcessedInfo.left_temp, TrainProcessedInfo.right_temp, TrainProcessedInfo.temp_difference,
                    TrainProcessedInfo.max_left_temp, TrainProcessedInfo.max_right_temp,
                    TrainProcessedInfo.max_temp_difference
                        ]).execute()

                Log.logger.warning(f'Insert train_processed_info: {json_data["train_id"]} records inserted')
        except Exception as e:
            Log.logger.critical(f'insert_train_processed_info: {json_data["train_id"]} Exception raised: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-014", EventErrorPub.CRITICAL,
                                            "habd_api: insert_train_processed_info: Exception raised: " + str(e))

    def train_processed_info_mem_mgmt(self, train_id):
        '''Perform memory management of train_processed_info table'''
        try:
            query = TrainProcessedInfo.delete().where(TrainProcessedInfo.train_id == train_id)
            query.execute()
            Log.logger.info(f'Deleted {train_id} from train_processed_info table')

        except Exception as e:
            Log.logger.critical(f'habd_api: train_processed_info_mem_mgmt: exception: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-015", EventErrorPub.CRITICAL,
                                            "habd_api: train_processed_info_mem_mgmt: exception: " + str(e))

    def test_insert_train_processed_info(self):
        '''test insert operation with sample data'''
        try:
            tp_info_table = TrainProcessedInfo()
            tp_info_table.ts = time.time()
            tp_info_table.train_id = "T" + time.strftime("%Y%m%d%H%M%S")
            tp_info_table.dpu_id = "DPU_01"
            tp_info_table.axle_id = 1
            tp_info_table.axle_speed = 80
            tp_info_table.avg_dyn_load_left = 5.5
            tp_info_table.avg_dyn_load_right = 5.5
            tp_info_table.max_dyn_load_left = 10.5
            tp_info_table.max_dyn_load_right = 10.5
            tp_info_table.ilf_left = 1.9
            tp_info_table.ilf_right = 1.9
            tp_info_table.wheel_status_left = 1
            tp_info_table.wheel_status_right = 1
            tp_info_table.train_type = 'LHB'
            tp_info_table.save()
            Log.logger.info(
                f'habd_api: test_insert_train_processed_info: record inserted: {tp_info_table.train_id}')
        except Exception as e:
            Log.logger.critical(f"habd_api: test_insert_train_processed_info : Exception generated: {e}", exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-016", EventErrorPub.CRITICAL,
                                            "habd_api: test_insert_train_processed_info : exception: " + str(e))

    def select_train_processed_info(self, train_id):
        '''Get store records from train_processed_info'''
        try:
            model_name = TrainProcessedInfo()
            records = model_name.select().where(train_id == train_id)
            return records
        except Exception as e:
            Log.logger.critical("habd_api: select_train_processed_info : exception : {e}", exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-017", EventErrorPub.CRITICAL,
                                            "habd_api: select_train_processed_info : exception : " + str(e))

    def insert_train_consolidated_info(self, data):
        '''insert train consolidated info in train_consolidated_info table'''
        try:
            json_data = json.loads(data)

            train_table = TrainConsolidatedInfo()
            train_table.train_id = json_data["train_id"]
            '''insert dpu_id internally with every record'''
            train_table.dpu_id = self.dpu_id
            train_table.entry_time = json_data["train_entry_time"]
            train_table.exit_time = json_data["train_exit_time"]
            train_table.total_axles = json_data["total_axles"]
            train_table.total_wheels = json_data["total_wheels"]
            train_table.direction = json_data["direction"]
            train_table.train_speed = json_data["train_speed"]
            train_table.train_type = json_data["train_type"]
            train_table.train_processed = json_data["train_processed"]
            train_table.remark = json_data["remark"]
            train_table.max_left_temp = json_data["max_left_temp"]
            train_table.max_right_temp = json_data["max_right_temp"]
            train_table.max_temp_difference = json_data["max_temp_difference"]
            
            train_table.save()
            ''' perform memory management '''
            self.train_consolidated_info_mem_mgmt()
            Log.logger.warning(f'Insert_train_consolidated_info: {json_data["train_id"]} record inserted')
        except Exception as e:
            Log.logger.critical(f'habd_api: insert_train_consolidated_info: {json_data["train_id"]} exception: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-018", EventErrorPub.CRITICAL,
                                            "habd_api: insert_train_consolidated_info : exception : " + str(e))

    def train_consolidated_info_mem_mgmt(self):
        '''Perform memory management of train_consolidated_info table'''
        try:
            records = (
                TrainConsolidatedInfo.select(TrainConsolidatedInfo.train_id).order_by(TrainConsolidatedInfo.train_id))
            total_records = len(records)
            Log.logger.info(f'No.of records in train_consolidated_info table: {total_records}')

            if total_records > 5000:
                Log.logger.info(records[0].train_id)
                query = TrainConsolidatedInfo.delete().where(
                    TrainConsolidatedInfo.train_id == records[0].train_id)
                query.execute()
                Log.logger.info(f'Deleted first record in train_consolidated_info table')

                '''train_processed_info memory management'''
                self.train_processed_info_mem_mgmt(records[0].train_id)

        except Exception as e:
            Log.logger.critical(f'habd_api: train_consolidated_info_mem_mgmt : exception: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-019", EventErrorPub.CRITICAL,
                                            "habd_api: train_consolidated_info_mem_mgmt : exception : " + str(e))

    def select_train_consolidated_info(self, train_id):
        '''Get train_consolidated_info records'''
        try:
            Log.logger.info(f'select_train_consolidated_info of {train_id}')
            records = TrainConsolidatedInfo.select().where(
                TrainConsolidatedInfo.train_id == train_id).order_by(TrainConsolidatedInfo.train_id)
            return records
        except Exception as e:
            Log.logger.critical("habd_api: select_train_consolidated_info: exception : {e}", exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-020", EventErrorPub.CRITICAL,
                                            "habd_api: select_train_consolidated_info : exception : " + str(e))

    def insert_habd_error_info(self, data):
        ''' Insert error info in table '''
        try:
            json_data = json.loads(data)

            error_info = ErrorInfo()
            error_info.ts = json_data["ts"]
            error_info.dpu_id = self.dpu_id
            error_info.msg_id = json_data["msg_id"]
            error_info.error_id = json_data["error_id"]
            error_info.error_severity = json_data["error_severity"]
            error_info.error_desc = json_data["error_desc"]
            error_info.save()

            '''perform memory management'''
            self.error_info_mem_mgmt()
            Log.logger.warning(f'Insert_habd_error_info: record inserted')
        except Exception as e:
            Log.logger.critical(f'habd_api: insert_habd_error_info: exception: {e}', exc_info=True)
            input_json_data = json.loads(data)
            Log.logger.warning(f'error_info: Message: {json.dumps(input_json_data, indent = 3)}')

    def insert_habd_event_info(self, data):
        ''' Insert event info in table '''
        try:
            json_data = json.loads(data)

            event_info = EventInfo()
            event_info.ts = json_data["ts"]
            event_info.msg_id = json_data["msg_id"]
            event_info.dpu_id = self.dpu_id
            event_info.event_id = json_data["event_id"]
            event_info.event_desc = json_data["event_desc"]
            event_info.save()

            '''perform memory management'''
            self.event_info_mem_mgmt()
            Log.logger.warning(f'Insert_habd_event_info: record inserted')
        except Exception as e:
            Log.logger.critical(f'habd_api: insert_habd_event_info: exception : {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-022", EventErrorPub.CRITICAL,
                                            "habd_api: insert_habd_event_info : exception : " + str(e))

    def event_info_mem_mgmt(self):
        '''keep 6 months events data'''
        try:
            event_query = (EventInfo.delete().where(
                fn.to_timestamp(EventInfo.ts) < datetime.now() + timedelta(days=-180, hours=0)))
            event_query.execute()
        except Exception as e:
            Log.logger.critical(f'habd_api: event_info_mem_mgmt : exception : {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-023", EventErrorPub.CRITICAL,
                                            "habd_api: event_info_mem_mgmt : exception : " + str(e))

    def error_info_mem_mgmt(self):
        '''keep 6 months data'''
        try:
            error_query = (ErrorInfo.delete().where(
                fn.to_timestamp(ErrorInfo.ts) < datetime.now() + timedelta(days=-180, hours=0)))
            error_query.execute()
        except Exception as e:
            Log.logger.critical(f'habd_api: error_info_mem_mgmt: exception : {e}', exc_info=True)


    def insert_habd_health_info(self, data):
        ''' Insert health info in table '''
        try:
            json_data = json.loads(data)

            health_info = HealthInfo()
            health_info.ts = json_data["ts"]
            health_info.dpu_id = self.dpu_id
            health_info.comm_link = json_data["comm_link"]
            health_info.interrogator_link = json_data["interrogator_link"]
            health_info.s1_link = json_data["S1"]
            health_info.s2_link = json_data["S2"]
            health_info.s3_link = json_data["S3"]
            health_info.s4_link = json_data["S4"]
            health_info.s5_link = json_data["S5"]
            health_info.s6_link = json_data["S6"]
            health_info.s7_link = json_data["S7"]
            health_info.s8_link = json_data["S8"]
            health_info.s9_link = json_data["S9"]
            health_info.s10_link = json_data["S10"]
            health_info.s11_link = json_data["S11"]
            health_info.s12_link = json_data["S12"]
            health_info.t1_link = json_data["T1"]
            health_info.t2_link = json_data["T2"]
            health_info.t3_link = json_data["T3"]
            health_info.t4_link = json_data["T4"]
            health_info.save()

            '''perform memory management'''
            self.health_info_mem_mgmt()
            Log.logger.info(f'habd_api: insert_habd_health_info: record inserted')
        except Exception as e:
            Log.logger.critical(f'habd_api: insert_habd_health_info: exception : {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-025", EventErrorPub.CRITICAL,
                                            "habd_api: insert_habd_health_info : exception : " + str(e))

    def health_info_mem_mgmt(self):
        '''keep 6 months data'''
        try:
            health_query = (HealthInfo.delete().where(
                fn.to_timestamp(HealthInfo.ts) < datetime.now() + timedelta(days=-180, hours=0)))
            health_query.execute()
        except Exception as e:
            Log.logger.critical(f'habd_api: health_info_mem_mgmt: exception : {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-026", EventErrorPub.CRITICAL,
                                            "habd_api: health_info_mem_mgmt : exception : " + str(e))


if __name__ == '__main__':
    if Log.logger is None:
        my_log = Log("habd_api")

    cfg = HabdDlmConfRead()
    cfg.read_cfg('../config/habd_dlm.conf')

    mqtt_client = MqttClient("127.0.0.1", 1883, "dlm-api", '', '', 'dlm-api')
    mqtt_client.connect()

    habd_api = HabdAPI(cfg, mqtt_client)

    db_conn = habd_api.connect_database(cfg)

    if db_conn:
        # habd_api.test_insert_train_processed_info()
        habd_api.train_consolidated_info_mem_mgmt()
        habd_api.event_info_mem_mgmt()
        habd_api.error_info_mem_mgmt()
    else:
        pass
