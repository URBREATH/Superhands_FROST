import os
import json
import logging
import urllib.parse
import datetime
from datetime import datetime as dt, timezone

from flask import Flask, request, jsonify
import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- CONFIG ---

# FROST
FROST_SERVER = os.getenv("FROST_SERVER", "https://frost-dev.urbreath.tech/FROST-Server/v1.1")

# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio-api-dev.urbreath.tech")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "sensordata")
MINIO_OBJECT_PATH_PREFIX = os.getenv("MINIO_OBJECT_PATH_PREFIX", "tallinn/superhands/")

# --- OBSERVED PROPERTY & DATASTREAM DEFINITIONS ---

OBSERVED_PROPERTIES_DEFINITIONS = {
    "PeopleCountTotal": {
        "name": "People Count Total",
        "description": "Total number of people counted (in + out) in a time interval",
        "definition": "urn:custom:def:phenomenon:people_count_total",
        "unitOfMeasurement": {
            "name": "people",
            "symbol": "ppl",
            "definition": "http://example.org/units/people"
        }
    },
    "PeopleCountIn": {
        "name": "People Count In",
        "description": "Number of people counted as entering (left to right) in a time interval",
        "definition": "urn:custom:def:phenomenon:people_count_in",
        "unitOfMeasurement": {
            "name": "people",
            "symbol": "ppl",
            "definition": "http://example.org/units/people"
        }
    },
    "PeopleCountOut": {
        "name": "People Count Out",
        "description": "Number of people counted as exiting (right to left) in a time interval",
        "definition": "urn:custom:def:phenomenon:people_count_out",
        "unitOfMeasurement": {
            "name": "people",
            "symbol": "ppl",
            "definition": "http://example.org/units/people"
        }
    },
    "Temperature": {
        "name": "Temperature",
        "description": "Internal temperature of the device in degrees Celsius",
        "definition": "http://www.opengis.net/def/param-name/OGC/1.0/airTemperature",
        "unitOfMeasurement": {
            "name": "degree Celsius",
            "symbol": "°C",
            "definition": "http://unitsofmeasure.org/ucum.html#para-30"
        }
    },
    "BatteryLevel": {
        "name": "Battery Level",
        "description": "Device battery level in Volts",
        "definition": "urn:custom:def:phenomenon:batterylevel",
        "unitOfMeasurement": {
            "name": "Volt",
            "symbol": "V",
            "definition": "http://unitsofmeasure.org/ucum.html#para-28"
        }
    },
    "Rssi": {
        "name": "RSSI",
        "description": "Received Signal Strength Indicator",
        "definition": "urn:custom:def:phenomenon:rssi",
        "unitOfMeasurement": {
            "name": "decibel-milliwatts",
            "symbol": "dBm",
            "definition": "http://unitsofmeasure.org/ucum.html#para-33"
        }
    },
    "RssiOk": {
        "name": "RSSI OK",
        "description": "Network availability flag based on RSSI",
        "definition": "urn:custom:def:phenomenon:rssi_ok",
        "unitOfMeasurement": {
            "name": "boolean",
            "symbol": "1",
            "definition": "urn:ogc:def:dataType:OGC:1.1:boolean"
        }
    }
}

resources = {
    "thing_id": None,
    "sensor_id": None,
    "feature_of_interest_id": None,
    "observed_properties": {},  # { "PeopleCountTotal": id, ... }
    "datastreams": {}           # { "PeopleCountTotal": ds_id, ... }
}

# --- MinIO CLIENT ---
try:
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(signature_version="s3v4"),
    )
    logging.info(f"✅ Connected to MinIO: {MINIO_ENDPOINT}")
except Exception as e:
    logging.error(f"🚨 Error during MinIO client initialization: {e}")
    s3_client = None

# --- FLASK APP ---
app = Flask(__name__)

# ---------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------
def upload_raw_data_to_minio(payload_bytes: bytes, device_id: str, received_at_str: str) -> bool:
    """
    Carica il JSON grezzo su MinIO, con path:
    <PREFIX>/<YYYYMMDD>/<device>_<timestamp>.json
    """
    if not s3_client:
        logging.error("MinIO client not initialized. Upload skipped.")
        return False

    try:
        try:
            dt_object = datetime.datetime.fromisoformat(received_at_str.replace("Z", "+00:00"))
        except ValueError:
            dt_object = datetime.datetime.utcnow()

        timestamp_for_filename = dt_object.strftime("%Y%m%d_%H%M%S_%f")
        date_folder_str = dt_object.strftime("%Y%m%d")

        base_path = MINIO_OBJECT_PATH_PREFIX or ""
        if base_path and not base_path.endswith("/"):
            base_path += "/"

        object_name = f"{base_path}{date_folder_str}/{device_id}_{timestamp_for_filename}.json"

        s3_client.put_object(
            Bucket=MINIO_BUCKET_NAME,
            Key=object_name,
            Body=payload_bytes,
            ContentType="application/json",
        )
        logging.info(f"📦 Raw data uploaded to MinIO: s3://{MINIO_BUCKET_NAME}/{object_name}")
        return True
    except Exception as e:
        logging.error(f"🚨 Generic error during MinIO upload: {e}", exc_info=True)
        return False

# ---------------------------------------------------------------------
# FROST helpers
# ---------------------------------------------------------------------
def post_to_frost(endpoint: str, data: dict):
    """POST generico verso FROST, con estrazione @iot.id / Location header."""
    url = f"{FROST_SERVER}/{endpoint}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    logging.info(f"POSTing to {url}")
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()

        if response.status_code == 201:  # Created
            location_header = response.headers.get("Location")
            if location_header:
                try:
                    id_value = location_header.split("(")[-1].split(")")[0]
                    return int(id_value) if id_value.isdigit() else id_value
                except (IndexError, ValueError):
                    logging.warning(f"Could not extract ID from Location header: {location_header}")
            if endpoint.lower() == "observations":
                return True

        response_data = response.json()
        return response_data.get("@iot.id")
    except requests.exceptions.HTTPError as e:
        logging.error(f"🚨 HTTP error: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logging.error(f"🚨 FROST request error: {e}")
        return None

def fetch_id_by_name(endpoint: str, name: str):
    """Cerca un'entità FROST per nome e restituisce il suo @iot.id se esiste."""
    try:
        # Filtro OData sicuro per URL
        safe_name = urllib.parse.quote(name)
        url = f"{FROST_SERVER}/{endpoint}?$filter=name eq '{safe_name}'&$select=@iot.id"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data.get("value") and len(data["value"]) > 0:
            return data["value"][0]["@iot.id"]
    except Exception as e:
        logging.warning(f"⚠️ Could not fetch existing ID for '{name}' from {endpoint}: {e}")
    return None

# ---------------------------------------------------------------------
# FROST entity creation (Superhands / SuperSpot specific)
# ---------------------------------------------------------------------
def get_location_from_payload(payload: dict):
    """Per Superhands: location nel campo payload['location'] se presente."""
    loc = payload.get("location")
    if not loc:
        logging.warning("Location not found in payload.")
        return None
    try:
        return {
            "longitude": loc["lng"],
            "latitude": loc["lat"],
            "altitude": 0  # non fornito, mettiamo 0
        }
    except Exception as e:
        logging.error(f"🚨 Error extracting location: {e}")
        return None

def create_sensor():
    """Crea il Sensor (se non esiste ancora)."""
    if resources["sensor_id"]:
        return
    logging.info("Attempting to create Sensor...")
    
    sensor_name = "SuperSpot People Counter"
    existing_id = fetch_id_by_name("Sensors", sensor_name)
    if existing_id:
        resources["sensor_id"] = existing_id
        logging.info(f"✅ Found existing Sensor: {existing_id}")
        return

    sensor_data = {
        "name": sensor_name,
        "description": "Superhands SuperSpot PIR people counter",
        "encodingType": "application/json",
        "metadata": "{}"
    }
    result = post_to_frost("Sensors", sensor_data)
    if result:
        resources["sensor_id"] = result
        logging.info(f"✅ Sensor created with ID: {result}")
    else:
        logging.error("🚨 Failed to create Sensor.")

def create_observed_properties():
    """Crea tutte le ObservedProperties definite (se non esistono ancora)."""
    if len(resources["observed_properties"]) == len(OBSERVED_PROPERTIES_DEFINITIONS):
        return

    logging.info("Attempting to create ObservedProperties...")
    for key, prop_data in OBSERVED_PROPERTIES_DEFINITIONS.items():
        if key not in resources["observed_properties"]:
            # Check if exists
            existing_id = fetch_id_by_name("ObservedProperties", prop_data["name"])
            if existing_id:
                resources["observed_properties"][key] = existing_id
                continue

            body = {
                "name": prop_data["name"],
                "description": prop_data["description"],
                "definition": prop_data["definition"]
            }
            result = post_to_frost("ObservedProperties", body)
            if result:
                resources["observed_properties"][key] = result
                logging.info(f"✅ ObservedProperty '{key}' created with ID: {result}")
            else:
                logging.error(f"🚨 Failed to create ObservedProperty '{key}'.")
                return

def create_feature_of_interest(payload: dict):
    """FOI basato sulla location del device."""
    if resources["feature_of_interest_id"]:
        return
    logging.info("Attempting to create FeatureOfInterest...")

    location_coords = get_location_from_payload(payload)
    if not location_coords:
        logging.error("🚨 Cannot create FeatureOfInterest: location not found.")
        return

    imei = payload.get("imei", "unknown")
    foi_name = f"Location of device {imei}"
    
    existing_id = fetch_id_by_name("FeaturesOfInterest", foi_name)
    if existing_id:
        resources["feature_of_interest_id"] = existing_id
        return

    foi_data = {
        "name": foi_name,
        "description": "Physical location of the SuperSpot sensor.",
        "encodingType": "application/vnd.geo+json",
        "feature": {
            "type": "Point",
            "coordinates": [
                location_coords["longitude"],
                location_coords["latitude"],
                location_coords["altitude"]
            ]
        }
    }
    result = post_to_frost("FeaturesOfInterest", foi_data)
    if result:
        resources["feature_of_interest_id"] = result
        logging.info(f"✅ FeatureOfInterest created with ID: {result}")
    else:
        logging.error("🚨 Failed to create FeatureOfInterest.")

def recover_datastreams_for_thing(thing_id):
    """Recupera i Datastream esistenti collegati a una Thing."""
    try:
        # Ottieni i datastream della Thing espandendo la ObservedProperty per capire di cosa si tratta
        url = f"{FROST_SERVER}/Things({thing_id})/Datastreams?$expand=ObservedProperty"
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            for ds in data.get("value", []):
                op = ds.get("ObservedProperty")
                if op:
                    # Cerchiamo di mappare il nome della ObservedProperty con le nostre chiavi
                    for key, defs in OBSERVED_PROPERTIES_DEFINITIONS.items():
                        if defs["name"] == op.get("name"):
                            resources["datastreams"][key] = ds["@iot.id"]
                            logging.info(f"✅ Recovered Datastream for '{key}': {ds['@iot.id']}")
                            break
    except Exception as e:
        logging.error(f"🚨 Error recovering datastreams: {e}")

def create_datastreams():
    """Crea un Datastream per ogni ObservedProperty."""
    if len(resources["datastreams"]) == len(OBSERVED_PROPERTIES_DEFINITIONS):
        return
    logging.info("Attempting to create Datastreams...")

    required_ids = ["thing_id", "sensor_id", "feature_of_interest_id"]
    if any(resources.get(key) is None for key in required_ids):
        logging.error("🚨 Cannot create Datastreams: missing Thing, Sensor, or FoI IDs.")
        return

    # Prima proviamo a recuperare quelli esistenti se la Thing esisteva già
    if not resources["datastreams"]:
        recover_datastreams_for_thing(resources["thing_id"])

    for key, prop_id in resources["observed_properties"].items():
        if key not in resources["datastreams"]:
            prop_def = OBSERVED_PROPERTIES_DEFINITIONS[key]
            datastream_data = {
                "name": f"{key} readings from SuperSpot",
                "description": f"Datastream for {prop_def['name']} from Superhands SuperSpot device.",
                "unitOfMeasurement": prop_def["unitOfMeasurement"],
                "observationType": (
                    "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CountObservation"
                    if key.startswith("PeopleCount")
                    else "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Observation"
                ),
                "Thing": {"@iot.id": resources["thing_id"]},
                "Sensor": {"@iot.id": resources["sensor_id"]},
                "ObservedProperty": {"@iot.id": prop_id}
            }
            result = post_to_frost("Datastreams", datastream_data)
            if result:
                resources["datastreams"][key] = result
                logging.info(f"✅ Datastream '{key}' created with ID: {result}")
            else:
                logging.error(f"🚨 Failed to create Datastream for '{key}'.")

def create_thing_and_related_entities(payload: dict):
    """Crea Thing + Sensor + ObservedProperties + FoI + Datastreams."""
    if resources["thing_id"]:
        return
    logging.info("Attempting to create the Thing and related entities...")

    imei = payload.get("imei", "unknown")
    name = payload.get("name", f"SuperSpot Device {imei}")
    location_coords = get_location_from_payload(payload)

    # 1. Check if Thing exists
    existing_id = fetch_id_by_name("Things", name)
    if existing_id:
        resources["thing_id"] = existing_id
        logging.info(f"✅ Found existing Thing: {existing_id}")
        # Procediamo a recuperare/creare il resto
    
    # 2. Create Thing if not found
    if not resources["thing_id"]:
        thing_data = {
            "name": name,
            "description": "Superhands SuperSpot people counter",
            "properties": {
                "imei": imei,
                "type": payload.get("type"),
                "rssi": payload.get("rssi"),
                "rssiOk": payload.get("rssiOk"),
            }
        }

        if location_coords:
            thing_data["Locations"] = [{
                "name": "Device Location",
                "description": "Last reported device location",
                "encodingType": "application/vnd.geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [
                        location_coords["longitude"],
                        location_coords["latitude"],
                        location_coords["altitude"]
                    ]
                }
            }]

        result = post_to_frost("Things", thing_data)
        if result:
            resources["thing_id"] = result
            logging.info(f"✅ Thing created with ID: {result}.")
        else:
            logging.error("🚨 Failed to create Thing. FROST operations halted.")
            return

    create_sensor()
    create_observed_properties()
    create_feature_of_interest(payload)
    create_datastreams()

def create_observations(payload: dict):
    """
    Crea Observations per tutte le proprietà note:
    - PeopleCountTotal (PIR)
    - PeopleCountIn (in)
    - PeopleCountOut (out)
    - Temperature (temp)
    - BatteryLevel (battery, root)
    - Rssi (rssi, root)
    - RssiOk (rssiOk, root)
    """
    data_array = payload.get("data")
    if not data_array or not isinstance(data_array, list):
        logging.warning("No 'data' array found in payload, cannot create Observations.")
        return

    if not resources["datastreams"]:
        logging.error("🚨 No Datastreams; cannot create Observations.")
        return

    foi_id = resources["feature_of_interest_id"]

    logging.info("Attempting to create Observations...")

    # Valori root che valgono per il pacchetto (battery, rssi, rssiOk)
    battery = payload.get("battery")
    rssi = payload.get("rssi")
    rssi_ok = payload.get("rssiOk")

    for m in data_array:
        ts = m.get("ts")
        if ts is None:
            logging.warning(f"Measurement without 'ts' skipped: {m}")
            continue

        in_count = m.get("in")
        out_count = m.get("out")
        total = m.get("PIR", (in_count or 0) + (out_count or 0))
        temp = m.get("temp")

        phenomenon_time = dt.fromtimestamp(ts, tz=timezone.utc).isoformat()
        result_time = dt.utcnow().replace(tzinfo=timezone.utc).isoformat()

        # Mappa logica: prop_key -> valore
        measurement_values = {
            "PeopleCountTotal": total,
            "PeopleCountIn": in_count,
            "PeopleCountOut": out_count,
            "Temperature": temp,
            "BatteryLevel": battery,
            "Rssi": rssi,
            "RssiOk": rssi_ok,
        }

        for key, value in measurement_values.items():
            if value is None:
                continue  # skip se non c'è il valore

            ds_id = resources["datastreams"].get(key)
            if not ds_id:
                logging.warning(f"No Datastream for key '{key}', skipping.")
                continue

            observation_data = {
                "phenomenonTime": phenomenon_time,
                "resultTime": result_time,
                "result": value,
                "Datastream": {"@iot.id": ds_id},
            }
            if foi_id:
                observation_data["FeatureOfInterest"] = {"@iot.id": foi_id}

            success = post_to_frost("Observations", observation_data)
            if success:
                logging.info(f"✅ Observation created for '{key}' ts={ts} value={value} on Datastream {ds_id}")
            else:
                logging.error(f"🚨 Failed to create Observation for '{key}' ts={ts}")

# ---------------------------------------------------------------------
# Flask endpoint
# ---------------------------------------------------------------------
@app.route("/superhands/data", methods=["POST"])
def receive_superhands_data():
    """
    1. parse JSON
    2. upload raw JSON su MinIO
    3. crea struttura FROST (Thing, Sensor, ObservedProperties, FoI, Datastreams)
    4. crea Observations
    """
    try:
        payload = request.get_json(force=True)
    except Exception:
        return jsonify({"error": "Invalid JSON"}), 400

    if not payload or "data" not in payload or not isinstance(payload["data"], list):
        return jsonify({"error": "Invalid payload: missing data array"}), 400

    imei = payload.get("imei", "unknown")

    # usa il primo ts come "received_at" per MinIO
    first_ts = None
    if payload["data"]:
        first_ts = payload["data"][0].get("ts")

    if first_ts is not None:
        received_at = dt.fromtimestamp(first_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    else:
        received_at = dt.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    # 1) upload raw JSON su MinIO (sempre, come backup)
    if s3_client:
        try:
            raw_bytes = json.dumps(payload).encode("utf-8")
            upload_raw_data_to_minio(raw_bytes, imei, received_at)
        except Exception as e:
            logging.error(f"🚨 Error uploading raw data to MinIO: {e}", exc_info=True)
    else:
        logging.warning("MinIO client not available. Raw data upload skipped.")

    # 2) crea struttura base su FROST se manca
    if not resources["thing_id"]:
        create_thing_and_related_entities(payload)

    # 3) se struttura completa, crea Observations
    if (
        resources.get("thing_id")
        and resources.get("datastreams")
        and len(resources["datastreams"]) == len(OBSERVED_PROPERTIES_DEFINITIONS)
    ):
        create_observations(payload)
        return jsonify({"status": "ok"}), 200
    else:
        logging.error("🚨 Cannot create Observations: the base structure on FROST is not complete.")
        return jsonify({"status": "error", "reason": "FROST structure incomplete"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded_=True)
