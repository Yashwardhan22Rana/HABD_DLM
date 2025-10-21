'''
*****************************************************************************
* File        : habd_api.py
* Module      : habd_dlm
* Version     : 1.1
* Date        : 28/04/2025
* Description : HABD Processing API for Data Logging Module
* Author      : Akash B, Kausthubha N K
* Copyright   : Copyright 2025, Lab to Market Innovations Private Limited
*               Bangalore
* Change Log  :
*   Version   Date        Author          Description
*   -------   ----------  --------------  ----------------------------------------
*   1.0       28/04/2025  Akash B           Initial version
*   1.1       10/09/2025  Kausthubha N K    Final Reviewer
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
from habd_event_error_pub import EventErrorPub
from habd_log import Log
from mqtt_client import *
sys.path.insert(1, "/home/l2m/habd-v1/src/habd_common")


class HabdAPI:
    '''HABD Database operations such as Select, Insert, Delete records'''

    def __init__(self, cfg_obj, mq_client):
        self.mqtt_client = mq_client
        self.event_msg_id = 0
        self.error_msg_id = 0
        self.dpu_id = cfg_obj.dpu_id
        self.dlm_pub = EventErrorPub(mq_client, self.dpu_id)
        self.psql_db = None  # Initialize psql_db

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
                self.psql_db = PostgresqlDatabase(db_name, user=user, password=password, host=host, port=port)
                if self.psql_db:
                    try:
                        self.psql_db.connect()
                        Log.logger.info(f'habd_api: database connection successful')
                        return self.psql_db
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
            json_data = json.loads(data)

            Log.logger.warning(f'=== TRAIN PROCESSED INFO ===')
            Log.logger.warning(f'Train ID: {json_data["train_id"]}')
            Log.logger.warning(f'Number of axle_ids: {len(json_data["axle_ids"])}')

            # Check if temperature data is available in this message
            has_temp_data = "temp_lefts" in json_data and "temp_rights" in json_data
            Log.logger.warning(f'Temperature data available: {has_temp_data}')

            # Default values for required fields
            wheel_status_left = 1
            wheel_status_right = 1

            # Use get() method with default empty lists for temperature fields
            temp_lefts = json_data.get("temp_lefts", [])
            temp_rights = json_data.get("temp_rights", [])
            
            # Get rake_ids - handle both "rake_id" and "rake_ids" keys
            rake_ids = json_data.get("rake_id", json_data.get("rake_ids", []))

            Log.logger.warning(f'Temperature data - Left: {len(temp_lefts)}, Right: {len(temp_rights)}')
            Log.logger.warning(f'Rake IDs available: {len(rake_ids)}')

            inserted_count = 0
            updated_count = 0
            
            # Use transaction for atomic operations
            with self.psql_db.atomic():
                for i in range(len(json_data["axle_ids"])):
                    axle_id = json_data["axle_ids"][i]
                    rake_id = rake_ids[i] if i < len(rake_ids) else None
                    
                    # Log rake_id for debugging (first 5 axles)
                    if i < 5:
                        Log.logger.warning(f'Axle {axle_id} rake_id: {rake_id}')

                    # Handle temperature data properly
                    if i < len(temp_lefts) and temp_lefts[i] is not None and temp_lefts[i] != -1:
                        left_temp = float(temp_lefts[i])
                    else:
                        left_temp = None

                    if i < len(temp_rights) and temp_rights[i] is not None and temp_rights[i] != -1:
                        right_temp = float(temp_rights[i])
                    else:
                        right_temp = None

                    # Calculate temperature difference
                    if left_temp is not None and right_temp is not None:
                        temp_difference = abs(left_temp - right_temp)
                    else:
                        temp_difference = None

                    try:
                        # Use INSERT ON CONFLICT (UPSERT) with all required fields including rake_id
                        query = '''
                            INSERT INTO train_processed_info 
                            (ts, train_id, dpu_id, axle_id, axle_speed, rake_id, 
                             left_temp, right_temp, wheel_status_left, wheel_status_right, 
                             temp_difference)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (train_id, axle_id) 
                            DO UPDATE SET
                                ts = EXCLUDED.ts,
                                axle_speed = COALESCE(EXCLUDED.axle_speed, train_processed_info.axle_speed),
                                rake_id = COALESCE(EXCLUDED.rake_id, train_processed_info.rake_id),
                                left_temp = COALESCE(EXCLUDED.left_temp, train_processed_info.left_temp),
                                right_temp = COALESCE(EXCLUDED.right_temp, train_processed_info.right_temp),
                                temp_difference = COALESCE(EXCLUDED.temp_difference, train_processed_info.temp_difference)
                        '''
                        
                        params = (
                            json_data["ts"],
                            json_data["train_id"],
                            json_data["dpu_id"],
                            axle_id,
                            json_data["axle_speeds"][i] if i < len(json_data.get("axle_speeds", [])) else None,
                            rake_id,
                            left_temp,
                            right_temp,
                            wheel_status_left,
                            wheel_status_right,
                            temp_difference
                        )
                        
                        cursor = self.psql_db.execute_sql(query, params)
                        
                        if cursor.rowcount == 1:
                            # For logging purposes
                            if has_temp_data and (left_temp is not None or right_temp is not None):
                                updated_count += 1
                            else:
                                inserted_count += 1
                                
                    except Exception as e:
                        if 'duplicate' in str(e).lower() or 'unique' in str(e).lower():
                            Log.logger.warning(f'Duplicate detected for train {json_data["train_id"]} axle {axle_id}, attempting update')
                            # Try to update the existing record with ALL fields including rake_id
                            try:
                                update_query = '''
                                    UPDATE train_processed_info 
                                    SET ts = %s,
                                        axle_speed = COALESCE(%s, axle_speed),
                                        rake_id = COALESCE(%s, rake_id),
                                        left_temp = COALESCE(%s, left_temp),
                                        right_temp = COALESCE(%s, right_temp),
                                        temp_difference = COALESCE(%s, temp_difference)
                                    WHERE train_id = %s AND axle_id = %s
                                '''
                                update_params = (
                                    json_data["ts"],
                                    json_data["axle_speeds"][i] if i < len(json_data.get("axle_speeds", [])) else None,
                                    rake_id,
                                    left_temp,
                                    right_temp,
                                    temp_difference,
                                    json_data["train_id"],
                                    axle_id
                                )
                                cursor = self.psql_db.execute_sql(update_query, update_params)
                                if cursor.rowcount > 0:
                                    updated_count += 1
                                    Log.logger.debug(f'Updated existing record for axle {axle_id} with rake_id: {rake_id}')
                            except Exception as update_error:
                                Log.logger.error(f'Failed to update duplicate record for axle {axle_id}: {update_error}')
                        else:
                            Log.logger.error(f'Failed to process record for axle {axle_id}: {e}')

            Log.logger.warning(f'Train processed info: {json_data["train_id"]} - {inserted_count} inserted, {updated_count} updated')
                
        except Exception as e:
            Log.logger.critical(f'insert_train_processed_info: Exception raised: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-014", EventErrorPub.CRITICAL,
                                            "habd_api: insert_train_processed_info: Exception raised: " + str(e))

    def insert_habd_temp_info(self, data):
        '''Insert temperature data from HABD info message'''
        try:
            json_data = json.loads(data)
            train_id = json_data["train_id"]
            
            Log.logger.warning(f'=== HABD TEMPERATURE INFO ===')
            Log.logger.warning(f'Train ID: {train_id}')
            Log.logger.warning(f'Number of axle_ids: {len(json_data["axle_ids"])}')

            updated_count = 0
            
            # Use transaction for atomic operations
            with self.psql_db.atomic():
                for i in range(len(json_data["axle_ids"])):
                    axle_id = json_data["axle_ids"][i]
                    
                    # Get temperature values
                    left_temp = float(json_data["temp_lefts"][i]) if i < len(json_data["temp_lefts"]) else None
                    right_temp = float(json_data["temp_rights"][i]) if i < len(json_data["temp_rights"]) else None
                    
                    # Calculate temperature difference
                    if left_temp is not None and right_temp is not None:
                        temp_difference = abs(left_temp - right_temp)
                    else:
                        temp_difference = None

                    try:
                        # First, try to UPDATE existing record - DON'T create new ones
                        update_query = '''
                            UPDATE train_processed_info 
                            SET left_temp = %s, right_temp = %s, temp_difference = %s
                            WHERE train_id = %s AND axle_id = %s
                        '''
                        
                        params = (left_temp, right_temp, temp_difference, train_id, axle_id)
                        cursor = self.psql_db.execute_sql(update_query, params)
                        
                        if cursor.rowcount > 0:
                            updated_count += 1
                            Log.logger.debug(f'Updated temperature for axle {axle_id}: L={left_temp}, R={right_temp}')
                        else:
                            # If no record was updated, log warning but don't create new record
                            Log.logger.warning(f'No existing record found for train {train_id} axle {axle_id} to update temperatures')
                            # The record should be created by train_processed_info message
                            
                    except Exception as e:
                        Log.logger.error(f'Failed to update temperature data for axle {axle_id}: {e}')

            Log.logger.warning(f'HABD temp info: {train_id} - {updated_count} records updated')
            
            # Update consolidated info with max temperatures
            self.update_consolidated_temperatures(train_id)
            
        except Exception as e:
            Log.logger.critical(f'insert_habd_temp_info: Exception raised: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-027", EventErrorPub.CRITICAL,
                                            "habd_api: insert_habd_temp_info: Exception raised: " + str(e))

    def update_consolidated_temperatures(self, train_id):
        '''Update max temperatures in consolidated info'''
        try:
            # Calculate max temperatures from processed data
            processed_records = self.select_train_processed_info(train_id)
            max_left_temp = None
            max_right_temp = None
            max_temp_difference = None

            if processed_records:
                left_temps = [r.left_temp for r in processed_records if r.left_temp is not None]
                right_temps = [r.right_temp for r in processed_records if r.right_temp is not None]
                temp_diffs = [r.temp_difference for r in processed_records if r.temp_difference is not None]
                
                if left_temps:
                    max_left_temp = max(left_temps)
                if right_temps:
                    max_right_temp = max(right_temps)
                if temp_diffs:
                    max_temp_difference = max(temp_diffs)

            # Update consolidated record using direct SQL
            try:
                # First try to update existing record
                update_query = '''
                    UPDATE train_consolidated_info 
                    SET max_left_temp = %s, max_right_temp = %s, max_temp_difference = %s
                    WHERE train_id = %s
                '''
                params = (max_left_temp, max_right_temp, max_temp_difference, train_id)
                cursor = self.psql_db.execute_sql(update_query, params)
                
                if cursor.rowcount == 0:
                    # No record exists, we'll skip creating one here
                    Log.logger.warning(f'No consolidated record found for {train_id} to update temperatures')
                else:
                    Log.logger.warning(f'Updated max temps for {train_id}: L={max_left_temp}, R={max_right_temp}, Diff={max_temp_difference}')
                    
            except Exception as e:
                Log.logger.error(f'Error updating consolidated temperatures for {train_id}: {e}')
                
        except Exception as e:
            Log.logger.error(f'Error updating consolidated temperatures for {train_id}: {e}')

    def insert_train_consolidated_info(self, data):
        '''insert train consolidated info in train_consolidated_info table'''
        try:
            json_data = json.loads(data)

            # Calculate max temperatures from processed data
            processed_records = self.select_train_processed_info(json_data["train_id"])
            max_left_temp = None
            max_right_temp = None
            max_temp_difference = None

            if processed_records:
                left_temps = [r.left_temp for r in processed_records if r.left_temp is not None]
                right_temps = [r.right_temp for r in processed_records if r.right_temp is not None]
                temp_diffs = [r.temp_difference for r in processed_records if r.temp_difference is not None]
                
                if left_temps:
                    max_left_temp = max(left_temps)
                if right_temps:
                    max_right_temp = max(right_temps)
                if temp_diffs:
                    max_temp_difference = max(temp_diffs)

            try:
                # Use direct SQL to avoid ON CONFLICT issues
                # First check if record exists
                check_query = "SELECT 1 FROM train_consolidated_info WHERE train_id = %s"
                cursor = self.psql_db.execute_sql(check_query, (json_data["train_id"],))
                exists = cursor.fetchone() is not None
                
                if exists:
                    # Update existing record
                    update_query = '''
                        UPDATE train_consolidated_info 
                        SET dpu_id = %s, entry_time = %s, exit_time = %s, total_axles = %s,
                            total_wheels = %s, direction = %s, train_speed = %s, train_type = %s,
                            train_processed = %s, remark = %s, max_left_temp = %s,
                            max_right_temp = %s, max_temp_difference = %s
                        WHERE train_id = %s
                    '''
                    params = (
                        self.dpu_id,
                        json_data["train_entry_time"],
                        json_data["train_exit_time"],
                        json_data["total_axles"],
                        json_data["total_wheels"],
                        json_data["direction"],
                        json_data["train_speed"],
                        json_data["train_type"],
                        json_data["train_processed"],
                        json_data["remark"],
                        max_left_temp,
                        max_right_temp,
                        max_temp_difference,
                        json_data["train_id"]
                    )
                    self.psql_db.execute_sql(update_query, params)
                    Log.logger.warning(f'Updated consolidated info: {json_data["train_id"]}')
                else:
                    # Insert new record
                    insert_query = '''
                        INSERT INTO train_consolidated_info 
                        (train_id, dpu_id, entry_time, exit_time, total_axles, total_wheels,
                         direction, train_speed, train_type, train_processed, remark,
                         max_left_temp, max_right_temp, max_temp_difference)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    params = (
                        json_data["train_id"],
                        self.dpu_id,
                        json_data["train_entry_time"],
                        json_data["train_exit_time"],
                        json_data["total_axles"],
                        json_data["total_wheels"],
                        json_data["direction"],
                        json_data["train_speed"],
                        json_data["train_type"],
                        json_data["train_processed"],
                        json_data["remark"],
                        max_left_temp,
                        max_right_temp,
                        max_temp_difference
                    )
                    self.psql_db.execute_sql(insert_query, params)
                    Log.logger.warning(f'Inserted consolidated info: {json_data["train_id"]}')
                
                ''' perform memory management '''
                self.train_consolidated_info_mem_mgmt()
                
            except Exception as e:
                Log.logger.error(f'Error in consolidated info for {json_data["train_id"]}: {e}')
                
        except Exception as e:
            Log.logger.critical(f'habd_api: insert_train_consolidated_info: {json_data["train_id"]} exception: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-018", EventErrorPub.CRITICAL,
                                            "habd_api: insert_train_consolidated_info : exception : " + str(e))

    def train_processed_info_mem_mgmt(self, train_id):
        '''Perform memory management of train_processed_info table'''
        try:
            query = TrainProcessedInfo.delete().where(TrainProcessedInfo.train_id == train_id)
            deleted_count = query.execute()
            Log.logger.info(f'Deleted {deleted_count} records for {train_id} from train_processed_info table')

        except Exception as e:
            Log.logger.critical(f'habd_api: train_processed_info_mem_mgmt: exception: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-015", EventErrorPub.CRITICAL,
                                            "habd_api: train_processed_info_mem_mgmt: exception: " + str(e))

    def select_train_processed_info(self, train_id):
        '''Get store records from train_processed_info'''
        try:
            records = TrainProcessedInfo.select().where(TrainProcessedInfo.train_id == train_id)
            return list(records)
        except Exception as e:
            Log.logger.critical(f"habd_api: select_train_processed_info : exception : {e}", exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-017", EventErrorPub.CRITICAL,
                                            "habd_api: select_train_processed_info : exception : " + str(e))
            return []

    def train_consolidated_info_mem_mgmt(self):
        '''Perform memory management of train_consolidated_info table'''
        try:
            records = (
                TrainConsolidatedInfo.select(TrainConsolidatedInfo.train_id).order_by(TrainConsolidatedInfo.train_id))
            total_records = len(records)
            Log.logger.info(f'No.of records in train_consolidated_info table: {total_records}')

            if total_records > 5000:
                oldest_train_id = records[0].train_id
                Log.logger.info(f'Deleting oldest record: {oldest_train_id}')
                query = TrainConsolidatedInfo.delete().where(
                    TrainConsolidatedInfo.train_id == oldest_train_id)
                query.execute()
                Log.logger.info(f'Deleted first record in train_consolidated_info table')

                '''train_processed_info memory management'''
                self.train_processed_info_mem_mgmt(oldest_train_id)

        except Exception as e:
            Log.logger.critical(f'habd_api: train_consolidated_info_mem_mgmt : exception: {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-019", EventErrorPub.CRITICAL,
                                            "habd_api: train_consolidated_info_mem_mgmt : exception : " + str(e))

    def select_train_consolidated_info(self, train_id):
        '''Get train_consolidated_info records'''
        try:
            Log.logger.info(f'select_train_consolidated_info of {train_id}')
            records = TrainConsolidatedInfo.select().where(
                TrainConsolidatedInfo.train_id == train_id)
            return list(records)
        except Exception as e:
            Log.logger.critical(f"habd_api: select_train_consolidated_info: exception : {e}", exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-020", EventErrorPub.CRITICAL,
                                            "habd_api: select_train_consolidated_info : exception : " + str(e))
            return []

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
            deleted_count = event_query.execute()
            Log.logger.info(f'Deleted {deleted_count} old event records')
        except Exception as e:
            Log.logger.critical(f'habd_api: event_info_mem_mgmt : exception : {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-023", EventErrorPub.CRITICAL,
                                            "habd_api: event_info_mem_mgmt : exception : " + str(e))

    def error_info_mem_mgmt(self):
        '''keep 6 months data'''
        try:
            error_query = (ErrorInfo.delete().where(
                fn.to_timestamp(ErrorInfo.ts) < datetime.now() + timedelta(days=-180, hours=0)))
            deleted_count = error_query.execute()
            Log.logger.info(f'Deleted {deleted_count} old error records')
        except Exception as e:
            Log.logger.critical(f'habd_api: error_info_mem_mgmt: exception : {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-024", EventErrorPub.CRITICAL,
                                            "habd_api: error_info_mem_mgmt : exception : " + str(e))

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
            deleted_count = health_query.execute()
            Log.logger.info(f'Deleted {deleted_count} old health records')
        except Exception as e:
            Log.logger.critical(f'habd_api: health_info_mem_mgmt: exception : {e}', exc_info=True)
            self.dlm_pub.publish_error_info("dlm", "DLM-ERROR-026", EventErrorPub.CRITICAL,
                                            "habd_api: health_info_mem_mgmt : exception : " + str(e))


if __name__ == '__main__':
    if Log.logger is None:
        my_log = Log("dlm")

    cfg = HabdDlmConfRead()
    cfg.read_cfg('/home/l2m/habd-v1/config/habd_dlm.conf')

    mqtt_client = MqttClient("127.0.0.1", 1883, "dlm-api", '', '', 'dlm-api')
    mqtt_client.connect()

    habd_api = HabdAPI(cfg, mqtt_client)

    db_conn = habd_api.connect_database(cfg)

    if db_conn:
        habd_api.train_consolidated_info_mem_mgmt()
        habd_api.event_info_mem_mgmt()
        habd_api.error_info_mem_mgmt()
    else:
        Log.logger.error("Failed to connect to database")