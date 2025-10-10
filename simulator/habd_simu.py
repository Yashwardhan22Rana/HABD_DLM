import time
import random
import json
import paho.mqtt.client as mqtt
from datetime import datetime


# MQTT Broker settings
BROKER = "localhost"  # Change if needed
PORT = 1883


# MQTT Topics
PROCESSED_INFO_TOPIC = "habd_pm/train_processed_info"
CONSOLIDATED_TOPIC = "habd_pm/train_consolidated_info"
RAKE_INFO_TOPIC = "habd_pm/rake_info"
OUTPUT_TOPIC = "habd/habd_info"


# Configuration
DPU_ID = "DPU_01"
SAMPLING_TIME = 0.001
REF_SENSOR = "T2"


LOCO_TYPES = ["WAP7", "WAP5", "WDP4D", "WDP4", "WAG5", "WAG7", "WAG9"]
COACH_TYPES = ["ICF", "LHB"]


def generate_train_id():
    current_time = datetime.now()
    return current_time.strftime("T%Y%m%d%H%M%S")


def get_current_timestamp():
    return int(time.time())


def get_train_composition():
    while True:
        try:
            # Get the train composition string
            print("Enter train composition (e.g., 'L2 C50 V1 L1 C32 V1'): ")
            composition_str = input().strip().upper()
            
            if not composition_str:
                print("Please enter a valid composition!")
                continue
            
            # Parse the composition string - handle complex sequences
            parts = composition_str.split()
            rake_sequence = []  # Store the sequence of rakes with type and count
            total_lead_locos = 0
            total_mid_locos = 0
            total_coaches = 0
            total_brakevans = 0
            
            for part in parts:
                if part.startswith('L'):
                    count = int(part[1:])
                    rake_sequence.append(('L', count))
                    # First L is lead, subsequent L's are mid
                    if total_lead_locos == 0:
                        total_lead_locos += count
                    else:
                        total_mid_locos += count
                elif part.startswith('C'):
                    count = int(part[1:])
                    rake_sequence.append(('C', count))
                    total_coaches += count
                elif part.startswith('V'):
                    count = int(part[1:])
                    rake_sequence.append(('V', count))
                    total_brakevans += count
            
            # Check if at least some rakes are present
            if total_lead_locos == 0 and total_mid_locos == 0 and total_coaches == 0 and total_brakevans == 0:
                print("At least one locomotive, coach, or brake van must be present!")
                continue

            total_axles = (total_lead_locos * 6) + (total_mid_locos * 6) + (total_coaches * 4) + (total_brakevans * 4)

            print(f"\nParsed composition: {composition_str}")
            print(f"Lead Locos: {total_lead_locos}, Mid Locos: {total_mid_locos}, Coaches: {total_coaches}, Brake Vans: {total_brakevans}")
            print(f"Calculated TOTAL axles = {total_axles} "
                  f"(LeadL:{total_lead_locos * 6} + MidL:{total_mid_locos * 6} + Coaches:{total_coaches * 4} + BrakeV:{total_brakevans * 4})\n")

            return total_axles, total_lead_locos, total_mid_locos, total_coaches, total_brakevans, rake_sequence

        except ValueError:
            print("Please enter valid format (e.g., 'L2 C50 V1 L1 C32 V1')!")
        except Exception as e:
            print(f"Error parsing input: {e}")


def generate_realistic_temperature_pair():
    """
    Generate realistic left and right temperature pairs where:
    - Left temp is base temperature
    - Right temp is correlated with left temp but with some variation
    - Occasionally generates outliers for realistic scenarios
    """
    # Generate base left temperature
    temp_left = round(random.uniform(25, 100), 2)
    
    # 10% chance for outlier behavior (significant difference)
    if random.random() < 0.1:
        # Outlier case - larger difference (could indicate bearing issues, etc.)
        temp_right = temp_left + random.uniform(-15, 15)
    else:
        # Normal case - small variation around left temperature
        # Most real-world scenarios have small differences
        temp_right = temp_left + random.uniform(-5, 5)
    
    # Ensure right temperature stays within realistic bounds
    temp_right = max(25, min(100, temp_right))
    temp_right = round(temp_right, 2)
    
    return temp_left, temp_right


def generate_train_data():
    train_id = generate_train_id()
    train_entry_time = get_current_timestamp()
    train_exit_time = train_entry_time + 120

    total_axles, lead_locos, mid_locos, num_coaches, num_brakevans, rake_sequence = get_train_composition()

    loco_type = random.choice(LOCO_TYPES)
    coach_type = random.choice(COACH_TYPES)
    train_type = f"{loco_type}/{coach_type}"

    axle_ids = list(range(1, total_axles + 1))
    axle_speeds = [round(random.uniform(50, 100), 2) for _ in axle_ids]
    avg_speed = round(sum(axle_speeds) / len(axle_speeds), 2)

    # Generate realistic temperature data using the new function
    temp_lefts = []
    temp_rights = []
    temp_differences = []
    
    for _ in range(total_axles):
        left_temp, right_temp = generate_realistic_temperature_pair()
        temp_lefts.append(left_temp)
        temp_rights.append(right_temp)
        temp_differences.append(round(abs(left_temp - right_temp), 2))

    # ------------------------
    # Generate detailed and simple rake IDs based on the sequence
    # ------------------------
    rake_id_list_simple = []  # For rake_info - simple names (e.g. "L1", "C1")
    rake_id_list_detailed = []  # For processed_info - detailed axle IDs (e.g. "L1-1", "C2-4")

    axles_per_loco = 6
    axles_per_coach = 4
    axles_per_brakevan = 4

    # Counters for each type
    loco_counter = 1
    coach_counter = 1
    brakevan_counter = 1

    # Process the rake sequence in order
    for rake_type, count in rake_sequence:
        if rake_type == 'L':
            for i in range(count):
                rake_id_list_simple.append(f"L{loco_counter}")
                for axle_num in range(1, axles_per_loco + 1):
                    rake_id_list_detailed.append(f"L{loco_counter}-{axle_num}")
                loco_counter += 1
        elif rake_type == 'C':
            for i in range(count):
                rake_id_list_simple.append(f"C{coach_counter}")
                for axle_num in range(1, axles_per_coach + 1):
                    rake_id_list_detailed.append(f"C{coach_counter}-{axle_num}")
                coach_counter += 1
        elif rake_type == 'V':
            for i in range(count):
                rake_id_list_simple.append(f"V{brakevan_counter}")
                for axle_num in range(1, axles_per_brakevan + 1):
                    rake_id_list_detailed.append(f"V{brakevan_counter}-{axle_num}")
                brakevan_counter += 1

    # ------------------------
    # Calculate wheel arrival and departure timestamps
    # ------------------------
    wheel_arr_ts = []
    wheel_dep_ts = []
    start_ms = int(train_entry_time * 1000)
    step_ms = int(120000 / total_axles)
    for i in range(total_axles):
        arr_ts = start_ms + i * step_ms
        dep_ts = arr_ts + step_ms // 2
        if arr_ts % 2 == 0:
            arr_ts += 1
        if dep_ts % 2 != 0:
            dep_ts += 1
        wheel_arr_ts.append(arr_ts / 1000.0)
        wheel_dep_ts.append(dep_ts / 1000.0)

    # ------------------------
    # Prepare MQTT messages
    # ------------------------
    processed_info = {
        "train_id": train_id,
        "dpu_id": DPU_ID,
        "ts": float(train_entry_time),
        "axle_ids": axle_ids,
        "axle_speeds": axle_speeds,
        "temp_lefts": temp_lefts,
        "temp_rights": temp_rights,
        "temp_differences": temp_differences,
        "rake_ids": rake_id_list_detailed,
        "remark": "SUCCESS"
    }

    consolidated_info = {
        "dpu_id": DPU_ID,
        "train_id": train_id,
        "train_entry_time": train_entry_time,
        "train_exit_time": float(train_exit_time),
        "total_axles": total_axles,
        "total_wheels": total_axles * 2,
        "direction": "DBU -> YNK",
        "train_speed": avg_speed,
        "max_left_temp": max(temp_lefts),
        "max_right_temp": max(temp_rights),
        "max_temp_difference": max(temp_differences),
        "train_type": train_type,
        "train_processed": True,
        "remark": "SUCCESS"
    }

    rake_info = {
        "train_id": train_id,
        "train_entry_time": train_entry_time,
        "train_exit_time": float(train_exit_time),
        "sampling_time": SAMPLING_TIME,
        "ref_sensor": REF_SENSOR,
        "remark": "SUCCESS",
        "no_of_axles": total_axles,
        "no_of_rakes": lead_locos + mid_locos + num_coaches + num_brakevans,
        "no_of_locos": lead_locos + mid_locos,
        "no_of_coach_wagon": num_coaches,
        "no_of_brake_vans": num_brakevans,
        "rake_ids": rake_id_list_simple,
        "wheel_arr_ts": wheel_arr_ts,
        "wheel_dep_ts": wheel_dep_ts
    }

    output_info = {
        "train_id": train_id,
        "axle_ids": axle_ids,
        "temp_rights": temp_rights,
        "temp_lefts": temp_lefts
    }

    # Print train composition for verification
    print(f"\nTrain Composition:")
    print(f"Lead Locomotives: {lead_locos}")
    print(f"Intermediate Locomotives: {mid_locos}")
    print(f"Coaches: {num_coaches}")
    print(f"Brake Vans: {num_brakevans}")
    print(f"Total Rakes: {len(rake_id_list_simple)}")
    print(f"Rake Order: {' -> '.join(rake_id_list_simple)}")

    return processed_info, consolidated_info, rake_info, output_info


def on_connect(client, userdata, flags, reasonCode, properties=None):
    print(f"[MQTT] Connected to broker with reason code {reasonCode}")


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.connect(BROKER, PORT, 60)
    client.loop_start()

    processed_info, consolidated_info, rake_info, output_info = generate_train_data()
    
    # Publish all messages
    client.publish(PROCESSED_INFO_TOPIC, json.dumps(processed_info))
    client.publish(CONSOLIDATED_TOPIC, json.dumps(consolidated_info))
    client.publish(RAKE_INFO_TOPIC, json.dumps(rake_info))
    client.publish(OUTPUT_TOPIC, json.dumps(output_info))

    print(f"[MQTT] Published train {processed_info['train_id']} to all topics.")
    print(f"Processed info contains {len(processed_info['axle_ids'])} axles")
    
    # Display the actual temperature data that was sent
    print("\nTemperature Data (for verification):")
    print("Axle ID | Left Temp | Right Temp | Difference")
    print("-" * 50)
    outlier_count = 0
    for i in range(len(processed_info['axle_ids'])):
        diff = processed_info['temp_differences'][i]
        if diff > 10:
            outlier_count += 1
        print(f"{processed_info['axle_ids'][i]:7} | {processed_info['temp_lefts'][i]:9.2f} | {processed_info['temp_rights'][i]:10.2f} | {diff:10.2f}")
    
    print(f"\nTemperature Statistics:")
    print(f"Total axles: {len(processed_info['axle_ids'])}")
    print(f"Outliers (diff > 10Â°): {outlier_count}")
    print(f"Max left temp: {consolidated_info['max_left_temp']}")
    print(f"Max right temp: {consolidated_info['max_right_temp']}")
    print(f"Max temp difference: {consolidated_info['max_temp_difference']}")
    
    time.sleep(5)


if __name__ == "__main__":
    main()
