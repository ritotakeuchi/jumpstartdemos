# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080",
# META       "default_lakehouse_name": "ManufacturingData",
# META       "default_lakehouse_workspace_id": "ce753ac1-7233-4889-b54d-f0ca9df04e06",
# META       "known_lakehouses": [
# META         {
# META           "id": "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

%pip install msfabricpysdkcore -q
%pip install azure-eventhub -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
importlib.invalidate_caches()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SENSOR_BIAS = False  
BIAS = True
from time import sleep, time

start_time_nb = time()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import asyncio
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import pandas as pd
from msfabricpysdkcore import FabricClientCore
import time
import ssl
from time import sleep, time
import base64
import uuid
fcc = FabricClientCore()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ws_id = notebookutils.runtime.context["currentWorkspaceId"]
manu_lh = "08cf1da1-4282-4f3d-bbb8-bfaa5e15d080"
manu_data = notebookutils.lakehouse.getWithProperties(manu_lh).properties["abfsPath"]
manufacturing_data = f"{manu_data}/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Event Hub configuration
es = fcc.get_eventstream(ws_id, eventstream_name="SensorDataEvents")
topology = fcc.get_eventstream_topology(ws_id, es.id)
for source in topology["sources"]:
    if source["name"] == 'CustomEndpoint-Source':
        source_id = source["id"]
        break
source_id = source_id if source_id else None
if source_id:
    custom_endpoint_info = fcc.get_eventstream_source_connection(ws_id,es.id, source_id)
    EVENT_HUB_NAME = custom_endpoint_info["eventHubName"]
    EVENT_HUB_CONNECTION_STR = custom_endpoint_info["accessKeys"]["primaryConnectionString"]

es = fcc.get_eventstream(ws_id, eventstream_name="QualityDataEvents")
topology = fcc.get_eventstream_topology(ws_id, es.id)
for source in topology["sources"]:
    if source["name"] == 'CustomEndpointSource':
        source_id = source["id"]
        break
source_id = source_id if source_id else None
if source_id:
    custom_endpoint_info = fcc.get_eventstream_source_connection(ws_id,es.id, source_id)
    EVENT_HUB_NAME_MQTT = custom_endpoint_info["eventHubName"]
    EVENT_HUB_CONNECTION_STR_MQTT = custom_endpoint_info["accessKeys"]["primaryConnectionString"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df = spark.read.format("delta").load(f"{manufacturing_data}/masterdata/machines")
machines_df = df.toPandas()
df = spark.read.format("delta").load(f"{manufacturing_data}/masterdata/products")
products_df = df.toPandas()
machines = machines_df['machine_id'].astype(int).tolist()
products = products_df['product_id'].astype(int).tolist()
sites = machines_df['site_id'].astype(int).tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_sensor_data(machine_id: str, site_id: str, SENSOR_BIAS: bool, timestamp = None) -> Dict[str, Any]:
    """Generate machine sensor data in the specified JSON format."""
    
    sensors = {}
    temperature_bias = 0
    vibration_bias = 0
    if SENSOR_BIAS and machine_id == 103:
    # Temperature sensor
        temperature_bias = 21
        vibration_bias = 3.5

    timestamp = timestamp if timestamp else datetime.utcnow().isoformat()

    sensors["temp_001"] = {
        "sensor_type": "temperature",
        "measure_unit": "Celsius",
        "sensor_values": {
            "value": round(random.uniform(30, 50), 2) + temperature_bias,
            "timestamp": timestamp
        }
    }
    
    # Pressure sensor
    sensors["pressure_001"] = {
        "sensor_type": "pressure",
        "measure_unit": "bar",
        "sensor_values": {
            "value": round(random.uniform(1.0, 5.0), 2),
            "timestamp": timestamp
        }
    }
    
    # Vibration sensor
    sensors["vibration_001"] = {
        "sensor_type": "vibration",
        "measure_unit": "mm/s",
        "sensor_values": {
            "value": round(random.uniform(0.5, 1.8), 2)  + vibration_bias,
            "timestamp": timestamp
        }
    }
    
    return {
        "machine_id": machine_id,
        "site_id": site_id,
        "sensors": sensors
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


async def send_sensor_data(SENSOR_BIAS=False, timestamp = None):
    """Send machine sensor data to Event Hub."""
    
    # Load machines and sites from CSV files

    # Create a mapping of site_id to sitename
    
    # Create machines list from CSV data
    machines_ = []
    for _, row in machines_df.iterrows():
        machines_.append({
            "machine_id": row['machine_id'],
            "site_id": row['site_id']})
        
    
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, 
        eventhub_name=EVENT_HUB_NAME
    )
    
    async with producer:
        # Create a batch
        event_data_batch = await producer.create_batch()
        
        # Generate and add sensor data for each machine
        for machine in machines_:
            
            sensor_data = generate_sensor_data(machine["machine_id"], machine["site_id"], SENSOR_BIAS, timestamp)
            event_data_batch.add(EventData(json.dumps(sensor_data)))
        
        # Send the batch
        await producer.send_batch(event_data_batch)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_quality_data(machine_id, site, BIAS=False, timestamp = None, k=0):

    # Send multiple FPY messages with randomness
    test_names = ["dimensional_check", "surface_quality", "electrical_test", "performance_test"]
    

    product_id = random.choice(products)
    production_line= f"LINE-{random.randint(1, 3)}"


    # Generate random test results (85% pass rate)
    test_results = {}
    defects = []
    passed_tests = 0
    
    for test in test_names:
        is_pass = random.random() < 0.97  # 97% probability of passing
        if BIAS and test == "surface_quality" and machine_id == 103:
            is_pass = random.random() < 0.70  # 80% probability of passing for this biased case
        test_results[test] = "PASS" if is_pass else "FAIL"
        if is_pass:
            passed_tests += 1
        else:
            defects.append(f"{test}_defect")
    
    first_pass_yield = len(defects) == 0
    defect_count = len(defects)
    pass_rate = (passed_tests / len(test_names)) * 100
    
    if machine_id == 103 and BIAS:
        cycle_time_seconds = max(4, round(random.uniform(5, 8), 2))
    else:
        cycle_time_seconds = max(4, round(random.uniform(3, 5), 2))

    timestamp = timestamp if timestamp else datetime.utcnow().isoformat()

    payload = {
        # Production Identifiers
        "product_id": product_id,
        "batch_id": f"BATCH-20260114-{k+1:03d}",
        "machine_id": machine_id,
        "production_line": production_line,
        
        # Timestamp
        "timestamp": timestamp,
        "cycle_time_seconds": cycle_time_seconds,
        
        # First-Pass-Yield Metrics
        "test_results": test_results,
        "first_pass_yield": first_pass_yield,
        "defects_found": defects,
        "rework_required": not first_pass_yield,
        
        # Quality Details
        "quality_metrics": {
            "defect_count": defect_count,
            "inspection_count": len(test_names),
            "pass_rate_percentage": round(pass_rate, 2)
        },
        
        # Production Info
        "component_ids": [f"COMP-{random.randint(1, 10):03d}" for _ in range(random.randint(2, 4))],
        "operator_id": f"OP-{random.randint(100, 199)}",
        "site_id": f"{site}"
    }

            # Wrap in CloudEvents envelope
    cloud_event = {
        "specversion": "1.0",
        "source": f"contosotopics/topic1/machine/{machine_id}",
        "subject": f"fpy/site/{site}",
        "type": "com.contoso.manufacturing.fpy",
        "id": str(uuid.uuid4()),
        "datacontenttype": "application/json",
        "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "data_base64": base64.b64encode(json.dumps(payload).encode()).decode()
    }
    
    return cloud_event



async def send_quality_data(BIAS=False, timestamp = None):

    producer_mqtt = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR_MQTT, 
        eventhub_name=EVENT_HUB_NAME_MQTT
    )
    
    async with producer_mqtt:
        # Create a batch
        event_data_batch = await producer_mqtt.create_batch()
        
        # Generate and add sensor data for each machine
        for i, machine_id in enumerate(machines):
            site = sites[i]
            
            quality_data = generate_quality_data(machine_id, site, BIAS, timestamp)
            event_data_batch.add(EventData(json.dumps(quality_data)))
        
        # Send the batch
        await producer_mqtt.send_batch(event_data_batch)

async def reset_(end_time, start_time):
    
    current_time = start_time
    while current_time <= end_time:
        print(current_time)
        current_time_str = current_time.isoformat()
        await send_quality_data(False, timestamp=current_time_str)
        await send_sensor_data(False, timestamp=current_time_str)
        current_time += timedelta(seconds=10)
    end_time = datetime.now()
    start_time = end_time

    return end_time, start_time

async def reset(): 
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=10)

    while end_time >= start_time + timedelta(minutes=1):
        end_time, start_time = await reset_(end_time, start_time)
        print(f"{end_time} | {start_time}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

await reset()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SENSOR_BIAS_countdown = time()
last_quality_data = time()

while True:
    if BIAS and ((time() - last_quality_data) > 21):
        await send_quality_data(BIAS)
        last_quality_data = time()
    else:
        await send_quality_data(BIAS)
        last_quality_data = time()
    await send_sensor_data(SENSOR_BIAS)
    #if (time() - SENSOR_BIAS_countdown) > 3600:
    #    BIAS = True
    sleep(0.1)
    if time() - start_time_nb > 7200:
        break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
