'''
*****************************************************************************
* File        : habd_model.py
* Module      : habd_dlm
* Version     : 1.1
* Date        : 28/04/2025
* Description : PostgreSQL Database Model Classes for HABD Data Logging Module
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

# '''Import python module'''
from peewee import *
import sys
sys.path.append("..")  # parent folder where habd_common lives
# '''Import wild module'''
from habd_dlm_conf import HabdDlmConfRead
from habd_log import Log

if Log.logger is None:
    Log('dlm')

'''read configuration file'''
cfg = HabdDlmConfRead()
cfg.read_cfg('/home/l2m/habd-v1/config/habd_dlm.conf')

db_name = cfg.database.DB_NAME
user = cfg.database.USER
password = cfg.database.PASSWORD
host = cfg.database.HOST
port = 5432
psql_db = None

try:
    psql_db = PostgresqlDatabase(db_name, user=user, password=password, host=host, port=port)
    if psql_db is not None:
        psql_db.connect()
except Exception as e:
    Log.logger.critical(f'habd_dlm_model: Exception: {e}', exc_info=True)


class WildModel(Model):
    # """A base model that will use our Postgresql database"""

    class Meta:
        database = psql_db


class TrainProcessedInfo(WildModel):
    # ''' Train processed information table '''
    id = AutoField()  # Keep auto ID for compatibility
    ts = FloatField()
    train_id = CharField()
    dpu_id = CharField()
    axle_id = IntegerField()
    axle_speed = FloatField(null=True)
    rake_id = CharField(null=True)
    left_temp = FloatField(null=True)
    right_temp = FloatField(null=True)
    wheel_status_left = SmallIntegerField(null=True)
    wheel_status_right = SmallIntegerField(null=True)
    temp_difference = FloatField(null=True)

    class Meta:
        table_name = "train_processed_info"
        # Use unique constraint instead of composite primary key
        indexes = (
            (('train_id', 'axle_id'), True),  # Unique index
        )


class TrainConsolidatedInfo(WildModel):
    # ''' Train consolidated information table '''
    train_id = CharField(primary_key=True)
    dpu_id = CharField()
    entry_time = FloatField(null=True)
    exit_time = FloatField(null=True)
    total_axles = SmallIntegerField(null=True)
    total_wheels = SmallIntegerField(null=True)
    direction = CharField(null=True)
    train_speed = FloatField(null=True)
    train_type = CharField(null=True)
    train_processed = BooleanField()
    remark = CharField(null=True)
    max_left_temp = FloatField(null=True)
    max_right_temp = FloatField(null=True)
    max_temp_difference = FloatField(null=True)

    class Meta:
        table_name = "train_consolidated_info"


class EventInfo(WildModel):
    # ''' Event information table '''
    ts = FloatField()
    dpu_id = CharField()
    msg_id = IntegerField(null=True)
    event_id = CharField(null=True)
    event_desc = TextField(null=True)

    class Meta:
        table_name = "event_info"


class ErrorInfo(WildModel):
    # ''' Error information table '''
    ts = FloatField()
    dpu_id = CharField()
    msg_id = IntegerField(null=True)
    error_id = CharField(null=True)
    error_severity = IntegerField(null=True)
    error_desc = TextField(null=True)

    class Meta:
        table_name = "error_info"


class HealthInfo(WildModel):
    # ''' Health information table '''
    ts = FloatField()
    dpu_id = CharField()
    comm_link = CharField(null=True)
    interrogator_link = CharField(null=True)
    s1_link = CharField(null=True)
    s2_link = CharField(null=True)
    s3_link = CharField(null=True)
    s4_link = CharField(null=True)
    s5_link = CharField(null=True)
    s6_link = CharField(null=True)
    s7_link = CharField(null=True)
    s8_link = CharField(null=True)
    s9_link = CharField(null=True)
    s10_link = CharField(null=True)
    s11_link = CharField(null=True)
    s12_link = CharField(null=True)
    t1_link = CharField(null=True)
    t2_link = CharField(null=True)
    t3_link = CharField(null=True)
    t4_link = CharField(null=True)

    class Meta:
        table_name = "health_info"


if __name__ == '__main__':
    if Log.logger is None:
        my_log = Log('dlm')
    Log.logger.info("habd_model: main program")

    # First, manually drop and recreate the unique constraint
    try:
        with psql_db.atomic():
            # Drop the table and recreate it
            psql_db.drop_tables([TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo])
            Log.logger.info("Dropped existing tables")
    except Exception as e:
        Log.logger.warning(f"Could not drop tables: {e}")
    
    # Create tables with proper constraints
    psql_db.create_tables([TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo])
    Log.logger.info("Created new tables with proper constraints")
    
    # Manually create unique constraint if needed
    try:
        with psql_db.atomic():
            psql_db.execute_sql('''
                ALTER TABLE train_processed_info 
                ADD CONSTRAINT unique_train_axle 
                UNIQUE (train_id, axle_id)
            ''')
            Log.logger.info("Created unique constraint on (train_id, axle_id)")
    except Exception as e:
        Log.logger.warning(f"Could not create unique constraint: {e}")