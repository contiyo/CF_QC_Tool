import json
import logging
import os
import requests
import sys
import time
import traceback
from datetime import datetime
import arcgis
import psycopg2
from arcgis.gis import GIS
from arcgis.mapping import WebMap
from utils import get_wkid, write_lists_to_excel, send_email2, delete_file, write_list_to_excel_new


# Setup logging
def setup_logging():
    year, month, day = datetime.now().strftime("%Y/%m/%d").split("/")
    log_dir = f"./logs/{year}/{month}/{day}"
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=f"{log_dir}/the.log",
        filemode="a",
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
        datefmt="%d-%b-%y %H:%M:%S",
    )

    # disable logging from arcgis module
    logging.getLogger("arcgis").setLevel(logging.CRITICAL)

    # disable logging from requests module
    logging.getLogger("requests").setLevel(logging.CRITICAL)

    # disable logging from urllib3 module
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)

    # disable logging from requests_oauthlib module
    logging.getLogger("requests_oauthlib").setLevel(logging.CRITICAL)

    return logging.getLogger(__name__)


# Setup connection
def setup_connections():
    username = "Entegro_Ireland"
    password = "Entegro@123"
    gis = GIS("https://www.arcgis.com", username, password)

    return gis


# Fetch layers from a web map
def fetch_webmap_layers(gis, item_id):
    item = gis.content.get(item_id)
    web_map = WebMap(item)
    return web_map.layers


def extract_correct_layers_with_id(layers):
    layers_dict = {
        "Poles": None,
        "Power Lines": None,
        "Electrical Crossing": None,
        "Exclusion Zone": None,
        "Existing Aerial Span": None,
        "Existing BT Ducts": None,
        "Proposed UG Route": None,
        "Chambers": None,
        "Proposed Aerial Span": None,
        "Armoured Cables Fed": None,
        "Toby Locations": None,
        "New Demand Points": None,
        "MDU": None,
        "Cabinets": None,
        "New Constructions": None,
        "LOC": None,
        "Planner Awareness Data": None,
        "Design Risk": None,
        "SED": None,
        "Planner Route": None,
        "Proposed Alternative UG Route": None,
        "City Fibre QC Point": None,
    }
    for layer in layers:
        for key in layers_dict.keys():
            # TODO - Check if the QC layer will always be the same
            if key == layer.title:
                layers_dict[key] = layer.itemId
    return layers_dict


def get_feature_geometry_line(layer_type, feature):
    if layer_type == "line":
        x1 = feature.geometry["paths"][0][0][0]
        x2 = feature.geometry["paths"][0][1][0]
        y1 = feature.geometry["paths"][0][0][1]
        y2 = feature.geometry["paths"][0][1][1]
        new_x = round((x1 + x2) / 2, 6)
        new_y = round((y1 + y2) / 2, 6)
        return (new_x, new_y)
    elif layer_type == "point":
        rounded_x = round(feature.geometry["x"], 6)
        rounded_y = round(feature.geometry["y"], 6)
        return (rounded_x, rounded_y)
    elif layer_type == "polygon":
        rings = feature.geometry["rings"][0]  # Access the first ring of the polygon
        num_points = len(rings)
        # Summing up all x and y coordinates
        sum_x = sum(point[0] for point in rings)
        sum_y = sum(point[1] for point in rings)
        # Calculating the average (centroid)
        center_x = sum_x / num_points
        center_y = sum_y / num_points
        return (center_x, center_y)


def get_error_type(layer):
    error_type_domain = {
        "poles": 1,
        "power_lines": 2,
        "electrical_crossing": 3,
        "exclusion_zone": 4,
        "existing_aerial_span": 5,
        "existing_bt_ducts": 6,
        "chambers": 7,
        "proposed_ug_route": 8,
        "proposed_aerial_span": 9,
        "armoured_cables_fed": 10,
        "toby_location": 11,
        "new_demand_points": 12,
        "mdu": 13,
        "cabinets": 14,
        "new_constructions": 15,
        "loc": 16,
        "planner_awareness": 17,
        "design_risk": 18,
        "sed": 19,
        "planned_route": 20,
        "proposed_alternative_ug_route": 21,
    }
    return error_type_domain[layer]


def fetch_webmaps_to_process():
    pass


def get_qc_layer_as_custom_json(gis, qc_layer_layer_id):
    custom_json = {}
    qc_layer = gis.content.get(qc_layer_layer_id)
    qc_fset = qc_layer.layers[0].query()
    for feature in qc_fset:
        custom_json[feature.attributes["related_gid"]] = {
            "OBJECTID": feature.attributes["OBJECTID"],
            "GlobalID": feature.attributes["GlobalID"],
            "Error_list": feature.attributes["error_description"],
            "error_type": feature.attributes["error_type"],
            "QC_Status": feature.attributes["QC_Status"],
            "QC_User": feature.attributes["QC_User"],
        }
    return custom_json


class LayerProcessor:
    def __init__(self, qc_layer_json, qc_layer_layer):
        self.qc_layer_json = qc_layer_json
        self.qc_layer_layer = qc_layer_layer
        self.poles_list_mail = []

    def process_poles(self, poles_features, layer, olt):
        attachment_list = self.attachment_list_downloader(layer)
        for pole in poles_features:
            check_id = (
                pole.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("point", pole)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing pole with {check_id}")
            try:
                if (
                        pole.attributes["status"] == 0
                        and pole.attributes["surface"] is None
                ):
                    error_list.append(
                        "2 - If 'Status' is 'Planned' (0) then 'Surface' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] == 0
                        and pole.attributes["private_land"] is None
                ):
                    error_list.append(
                        "3 - If 'Status' is 'Planned' (0) then 'Private Land' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] == 0
                        and pole.attributes["np_7m_from_lv"] is None
                ):
                    error_list.append(
                        "4 - If 'Status' is 'Planned' (0) then 'New proposed pole 7m away from LV electric pole/wire' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] == 0
                        and pole.attributes["np_7m_from_hv"] is None
                ):
                    error_list.append(
                        "5 - If 'Status' is 'Planned' (0) then 'New proposed pole 7m away from HV electric pole/wire' can not be blank"
                    )
                    priority_list.append(5)
                if pole.attributes["status"] == 0 and pole.attributes["surveyed"] != 1:
                    error_list.append(
                        "6 - If 'Status' is 'Planned' (0) then 'Surveyed' must be 'Yes'"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["plant_item"] is None
                ):
                    error_list.append(
                        "7 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Plant Item' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["owner"] == 0
                ):
                    error_list.append(
                        "8 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Owner' can not be 'CityFibre'"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["pole_age"] is None
                ):
                    error_list.append(
                        "9 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Pole Age' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["test_date"] is None
                ):
                    error_list.append(
                        "10 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Test Date' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["bt_id"] is None
                ):
                    error_list.append(
                        "11 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'BT ID' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["pole_a1024"] is None
                ):
                    error_list.append(
                        "12 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Pole A1024' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["hazards"] is None
                ):
                    error_list.append(
                        "13 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Hazards' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["capping"] is None
                ):
                    error_list.append(
                        "14 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Capping' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["exist_wire"] is None
                ):
                    error_list.append(
                        "15 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Existing Wire count' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["ring_head"] is None
                ):
                    error_list.append(
                        "16 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Ring head present' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["wires_ringhead"] is None
                ):
                    error_list.append(
                        "17 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Wires hosted on ringhead' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["radial"] is None
                ):
                    error_list.append(
                        "18 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Radial distribution' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["free_space"] is None
                ):
                    error_list.append(
                        "19 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Space to host an ASN' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["free_space_dist"] is None
                ):
                    error_list.append(
                        "20 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Space to host a distribution joint at lower envelope' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["p2p_spans"] is None
                ):
                    error_list.append(
                        "21 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Existing span count' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["los"] is None
                ):
                    error_list.append(
                        "22 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'LOS' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["existing_lowdrop_wires"] is None
                ):
                    error_list.append(
                        "23 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Existing low drop wires' can not be blank"
                    )
                    priority_list.append(5)
                if pole.attributes["comments"] is None:
                    error_list.append("25 - 'Comments' can not be blank")
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["road_edge"] is None
                ):
                    error_list.append(
                        "26 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then '1m from edge of road to front of pole achieved' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["mewp_access"] is None
                ):
                    error_list.append(
                        "27 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'MEWP access' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["pole_stay"] is None
                ):
                    error_list.append(
                        "28 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Pole stay' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["surface"] is None
                ):
                    error_list.append(
                        "29 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Surface' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["private_land"] is None
                ):
                    error_list.append(
                        "30 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Private Land' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["access_issue"] is None
                ):
                    error_list.append(
                        "31 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Accessibility issue?' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["foliage"] is None
                ):
                    error_list.append(
                        "32 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Foliage on pole?' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["status"] != 0
                        and pole.attributes["surveyed"] == 1
                        and pole.attributes["space_unb_joint"] is None
                ):
                    error_list.append(
                        "35 - If 'Status' is not 'Planned' and 'Surveyed' is 'Yes' then 'Space for unbundling Joint?' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        pole.attributes["surveyed"] is None
                        or pole.attributes["surveyed"] != 1
                ):
                    error_list.append("36 - 'Surveyed' must be 'Yes'")
                    priority_list.append(5)
                if pole.attributes["status"] != 0 and pole.attributes["surveyed"] == 1:
                    attach_exists = next(
                        (
                            x
                            for x in attachment_list
                            if x == pole.attributes["GlobalID"]
                        ),
                        None,
                    )
                    if not attach_exists:
                        error_list.append("37 - Attachments missing")
                        priority_list.append(5)
            except Exception as e:
                logging.error(f"Error processing pole {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                poles_dict = {
                    "DA": olt,
                    "Layer": "Poles",
                    "OBJECTID": pole.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(poles_dict)
            max_priority = max(priority_list)

            self.process_feature_on_qc_layer(
                check_id,
                "poles",
                geom,
                error_list,
                max_priority,
                pole.attributes["Editor"],
                pole.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_power_lines(self, power_lines_features, layer, olt):
        attachment_list = self.attachment_list_downloader(layer)
        for power_line in power_lines_features:
            check_id = (
                power_line.attributes["GlobalID"]
                .replace("{", "")
                .replace("}", "")
                .lower()
            )
            geom = get_feature_geometry_line("line", power_line)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing power line with {check_id}")
            try:
                if (
                        power_line.attributes["voltage"] is None
                        and power_line.attributes["comments"] is None
                ):
                    error_list.append(
                        "1 - If 'Voltage' is blank then 'Comments' can not be blank"
                    )
                    priority_list.append(5)
                attach_exists = next(
                    (
                        True
                        for x in attachment_list
                        if x == power_line.attributes["GlobalID"]
                    ),
                    None,
                )
                if not attach_exists:
                    error_list.append("2 - Attachments missing")
                    priority_list.append(3)
            except Exception as e:
                logging.error(f"Error processing power line {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                power_line_dict = {
                    "DA": olt,
                    "Layer": "Power Lines",
                    "OBJECTID": power_line.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(power_line_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "power_lines",
                geom,
                error_list,
                max_priority,
                power_line.attributes["Editor"],
                power_line.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_electrical_crossing(self, electrical_crossing_features, olt):
        for crossing in electrical_crossing_features:
            check_id = (
                crossing.attributes["GlobalID"]
                .replace("{", "")
                .replace("}", "")
                .lower()
            )
            geom = get_feature_geometry_line("point", crossing)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing electrical crossing with {check_id}")
            try:
                if crossing.attributes["status"] is None:
                    error_list.append("1 - 'Status' can not be blank")
                    priority_list.append(5)
                if crossing.attributes["voltage"] is None:
                    error_list.append("2 - 'Voltage' can not be blank")
                    priority_list.append(5)
                if (
                        crossing.attributes["status"] == 2
                        and crossing.attributes["clearance"] is None
                ):
                    error_list.append(
                        "3 - If 'Status' is 'Measured by Survey' then 'Clearance' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        crossing.attributes["sur_status"] == "Unable to measure"
                        and crossing.attributes["comments"] is None
                ):
                    error_list.append(
                        "4 - If 'Survey Status' is 'Unable to measure' then 'Comments' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        crossing.attributes["redesign_req"] is None
                        and crossing.attributes["status"] == 2
                ):
                    error_list.append(
                        "5 - If 'Status' is 'Measured by Survey' then 'Redesign Required' can not be blank"
                    )
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing electrical crossing {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                crossing_dict = {
                    "DA": olt,
                    "Layer": "Electrical Crossing",
                    "OBJECTID": crossing.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(crossing_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "electrical_crossing",
                geom,
                error_list,
                max_priority,
                crossing.attributes["Editor"],
                crossing.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_exclusion_zone(self, exclusion_zone_features, layer, olt):
        attachment_list = self.attachment_list_downloader(layer)
        for exclusion in exclusion_zone_features:
            check_id = (
                exclusion.attributes["GlobalID"]
                .replace("{", "")
                .replace("}", "")
                .lower()
            )
            geom = get_feature_geometry_line("point", exclusion)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing exclusion zone with {check_id}")
            try:
                if exclusion.attributes["status"] is None:
                    error_list.append("1 - 'Status' can not be blank")
                    priority_list.append(5)
                if exclusion.attributes["excl_zone"] is None:
                    error_list.append("2 - 'Exclusion Zone' can not be blank")
                    priority_list.append(5)
                if (
                        exclusion.attributes["excl_zone"] == "Unknown"
                        and exclusion.attributes["comments"] is None
                ):
                    error_list.append(
                        "3 - If 'Exclusion Zone' is 'Unknown' then 'Comments' can not be blank"
                    )
                    priority_list.append(5)
                if exclusion.attributes["p_infrig"] is None:
                    error_list.append("6 - 'Planned Infrastructure' can not be blank")
                    priority_list.append(5)
                if (
                        exclusion.attributes["excl_zone"] == "BT Pole <11KV-33KV-3m"
                        and exclusion.attributes["ladder_mewp_360"] is None
                ):
                    error_list.append(
                        "7 - If 'Exclusion Zone' is 'BT Pole <11KV-33KV-3m' then 'Ladder/MEWP 360' can not be blank"
                    )
                    priority_list.append(5)
                if exclusion.attributes["sur_status"] is None:
                    error_list.append("8 - 'Survey Status' can not be blank")
                    priority_list.append(5)
                if (
                        exclusion.attributes["status"] == "Measured  by Survey"
                        and exclusion.attributes["rede_req"] is None
                ):
                    error_list.append(
                        "9 - If 'Status' is 'Measured by Survey' then 'Redesign Required' can not be blank"
                    )
                    priority_list.append(5)

                attach_exists = next(
                    (
                        x
                        for x in attachment_list
                        if x == exclusion.attributes["GlobalID"]
                    ),
                    None,
                )
                if not attach_exists:
                    error_list.append("10 - Attachments missing")
                    priority_list.append(3)
            except Exception as e:
                logging.error(f"Error processing exclusion zone {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                exclusion_dict = {
                    "DA": olt,
                    "Layer": "Exclusion Zone",
                    "OBJECTID": exclusion.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(exclusion_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "exclusion_zone",
                geom,
                error_list,
                max_priority,
                exclusion.attributes["Editor"],
                exclusion.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_aerial_spans(self, aerial_spans_features, olt):
        for span in aerial_spans_features:
            check_id = (
                span.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("line", span)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing aerial span with {check_id}")
            try:
                if span.attributes["cable_count"] is None:
                    error_list.append("1 - 'Number of cables' can not be blank")
                    priority_list.append(5)
                if span.attributes["hv_crossing"] is None:
                    error_list.append("3 - 'HV Crossing' can not be blank")
                    priority_list.append(5)
                if span.attributes["lv_network"] is None:
                    error_list.append(
                        "4 - 'LV Network with 1m below/above?' can not be blank"
                    )
                    priority_list.append(5)
                if span.attributes["span_bellow_abowe"] is None:
                    error_list.append("5 - 'Span Above/Below LV?' can not be blank")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing aerial span {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                span_dict = {
                    "DA": olt,
                    "Layer": "Aerial Spans",
                    "OBJECTID": span.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(span_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "existing_aerial_span",
                geom,
                error_list,
                max_priority,
                span.attributes["Editor"],
                span.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_bt_ducts(self, bt_ducts_features, olt):
        for duct in bt_ducts_features:
            check_id = (
                duct.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("line", duct)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing bt duct with {check_id}")
            try:
                if duct.attributes["duct_cap"] is None:
                    error_list.append("1 - 'Duct Capacity' can not be blank")
                    priority_list.append(5)
                if duct.attributes["num_ways"] is None:
                    error_list.append("2 - 'Number of Ways' can not be blank")
                    priority_list.append(5)
                if duct.attributes["remspace_bt"] is None:
                    error_list.append("3 - 'Remaining Space for BT' can not be blank")
                    priority_list.append(5)
                if duct.attributes["status"] is None:
                    error_list.append("5 - 'Status' can not be blank")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing BT duct {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                duct_dict = {
                    "DA": olt,
                    "Layer": "BT Ducts",
                    "OBJECTID": duct.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(duct_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "existing_bt_ducts",
                geom,
                error_list,
                max_priority,
                duct.attributes["Editor"],
                duct.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_proposed_ug_route(self, proposed_ug_route_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for route in proposed_ug_route_features:
            check_id = (
                route.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("line", route)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing proposed_ug_route with {check_id}")
            try:
                if route.attributes["comments"] is None:
                    error_list.append("1 - 'Comments' can not be blank")
                    priority_list.append(2)
                if route.attributes["surface_type"] is None:
                    error_list.append("2 - 'Surface Type' can not be blank")
                    priority_list.append(5)

                attach_exists = next(
                    (x for x in attachments if x == route.attributes["GlobalID"]), None
                )
                if not attach_exists:
                    error_list.append("3 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing proposed_ug_route {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                route_dict = {
                    "DA": olt,
                    "Layer": "Proposed UG Route",
                    "OBJECTID": route.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(route_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "proposed_ug_route",
                geom,
                error_list,
                max_priority,
                route.attributes["Editor"],
                route.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_chambers(self, chambers_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for chamber in chambers_features:
            check_id = (
                chamber.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("point", chamber)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing chamber with {check_id}")
            try:
                if chamber.attributes["surveyed"] is None:
                    error_list.append("1 - 'Surveyed' can not be blank")
                    priority_list.append(5)
                if (
                        chamber.attributes["surveyed"] == 1
                        and chamber.attributes["chamber_loc"] is None
                ):
                    error_list.append(
                        "2 - If 'Surveyed' is 'Yes' then 'Chamber Location' can not be blank"
                    )
                    priority_list.append(3)
                if chamber.attributes["status"] is None:
                    error_list.append("3 - 'Status' can not be blank")
                    priority_list.append(5)
                if chamber.attributes["owner"] is None:
                    error_list.append("4 - 'Owner' can not be blank")
                    priority_list.append(3)
                if (
                        chamber.attributes["surveyed"] == 1
                        and chamber.attributes["space_cf"] is None
                ):
                    error_list.append(
                        "5 - If 'Surveyed' is 'Yes' then 'Space to host CF joint' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        chamber.attributes["surveyed"] == 1
                        and chamber.attributes["hole_type"] is None
                ):
                    error_list.append(
                        "6 - If 'Surveyed' is 'Yes' then 'Chamber Type' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        chamber.attributes["surveyed"] == 1
                        and chamber.attributes["mobra_fit"] is None
                ):
                    error_list.append(
                        "7 - If 'Surveyed' is 'Yes' then 'MOBRA fitted' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        chamber.attributes["surveyed"] == 1
                        and chamber.attributes["surface"] is None
                ):
                    error_list.append(
                        "8 - If 'Surveyed' is 'Yes' then 'Surface' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        chamber.attributes["data_collection"] is None
                        and chamber.attributes["comments"] is None
                ):
                    error_list.append(
                        "9 - If 'Data Collection' is blank then 'Comments' can not be blank"
                    )
                    priority_list.append(5)

                attach_exists = next(
                    (x for x in attachments if x == chamber.attributes["GlobalID"]),
                    None,
                )
                if not attach_exists:
                    error_list.append("11 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing chamber {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                chamber_dict = {
                    "DA": olt,
                    "Layer": "Chambers",
                    "OBJECTID": chamber.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(chamber_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "chambers",
                geom,
                error_list,
                max_priority,
                chamber.attributes["Editor"],
                chamber.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_proposed_aerial_spans(self, proposed_aerial_spans_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for span in proposed_aerial_spans_features:
            check_id = (
                span.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("line", span)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing proposed aerial span with {check_id}")
            try:
                if span.attributes["tree_len"] is None:
                    error_list.append("2 - 'Tree Length' can not be blank")
                    priority_list.append(5)

                attach_exists = next(
                    (x for x in attachments if x == span.attributes["GlobalID"]), None
                )
                if not attach_exists:
                    error_list.append("3 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing proposed aerial span {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                span_dict = {
                    "DA": olt,
                    "Layer": "Proposed Aerial Spans",
                    "OBJECTID": span.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(span_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "proposed_aerial_span",
                geom,
                error_list,
                max_priority,
                span.attributes["Editor"],
                span.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_armoured_cable(self, armoured_cable_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for cable in armoured_cable_features:
            check_id = (
                cable.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("line", cable)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing armoured cable with {check_id}")
            try:
                attach_exists = next(
                    (x for x in attachments if x == cable.attributes["GlobalID"]), None
                )
                if not attach_exists:
                    error_list.append("2 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing armoured cable {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                cable_dict = {
                    "DA": olt,
                    "Layer": "Armoured Cable",
                    "OBJECTID": cable.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(cable_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "armoured_cables_fed",
                geom,
                error_list,
                max_priority,
                cable.attributes["Editor"],
                cable.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_toby(self, toby_type_features, layer, olt):
        for toby in toby_type_features:
            check_id = (
                toby.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("point", toby)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing toby with {check_id}")
            try:
                if toby.attributes["toby_type"] is None:
                    error_list.append("1 - 'Toby Type' can not be blank")
                    priority_list.append(5)
                if toby.attributes["status"] is None:
                    error_list.append("2 - 'Status' can not be blank")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing toby {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                toby_dict = {
                    "DA": olt,
                    "Layer": "Toby",
                    "OBJECTID": toby.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(toby_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "toby_location",
                geom,
                error_list,
                max_priority,
                toby.attributes["Editor"],
                toby.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_new_demand_points(self, new_demand_points_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for point in new_demand_points_features:
            check_id = (
                point.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("point", point)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing new demand point with {check_id}")
            try:
                if (
                        point.attributes["home_count"] is None
                        and point.attributes["comments"] is None
                ):
                    error_list.append(
                        "1 - If 'Home Count' is blank then 'Comments' can not be blank"
                    )
                    priority_list.append(4)
                if (
                        point.attributes["property_type"] is None
                        and point.attributes["comments"] is None
                ):
                    error_list.append(
                        "2 - If 'Property Type' is blank then 'Comments' can not be blank"
                    )
                    priority_list.append(4)
                if (
                        point.attributes["street_name"] is None
                        and point.attributes["comments"] is None
                ):
                    error_list.append(
                        "3 - If 'Street Name' is blank then 'Comments' can not be blank"
                    )
                    priority_list.append(4)

                attach_exists = next(
                    (x for x in attachments if x == point.attributes["GlobalID"]), None
                )
                if not attach_exists:
                    error_list.append("4 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing new demand point {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                point_dict = {
                    "DA": olt,
                    "Layer": "New Demand Points",
                    "OBJECTID": point.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(point_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "new_demand_points",
                geom,
                error_list,
                max_priority,
                point.attributes["Editor"],
                point.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_mdu(self, mdu_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for mdu in mdu_features:
            check_id = (
                mdu.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("point", mdu)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing mdu with {check_id}")
            try:
                if mdu.attributes["unit_type"] is None:
                    error_list.append("1 - 'Unit Type' can not be blank")
                    priority_list.append(5)
                if mdu.attributes["mdu_type"] is None:
                    error_list.append("2 - 'MDU Type' can not be blank")
                    priority_list.append(5)
                if mdu.attributes["unit_count"] is None:
                    error_list.append("3 - 'Unit Count' can not be blank")
                    priority_list.append(5)

                attach_exists = next(
                    (x for x in attachments if x == mdu.attributes["GlobalID"]), None
                )
                if not attach_exists:
                    error_list.append("5 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing mdu {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                mdu_dict = {
                    "DA": olt,
                    "Layer": "MDU",
                    "OBJECTID": mdu.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(mdu_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "mdu",
                geom,
                error_list,
                max_priority,
                mdu.attributes["Editor"],
                mdu.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_cabinets(self, cabinets_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for cabinet in cabinets_features:
            check_id = (
                cabinet.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("point", cabinet)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing cabinet with {check_id}")
            try:
                if cabinet.attributes["cab_type"] is None:
                    error_list.append("1 - 'Cabinet Type' can not be blank")
                    priority_list.append(5)
                if cabinet.attributes["surface"] is None:
                    error_list.append("2 - 'Surface' can not be blank")
                    priority_list.append(5)
                if (
                        cabinet.attributes["surface"] == "Footway"
                        and cabinet.attributes["footway_width"] is None
                ):
                    error_list.append(
                        "3 - If 'Surface' is 'Footway' then 'Footway Width' can not be blank"
                    )
                    priority_list.append(5)
                if (
                        cabinet.attributes["surface"] == "Grass verge"
                        and cabinet.attributes["grassverge_width"] is None
                ):
                    error_list.append(
                        "4 - If 'Surface' is 'Grass verge' then 'Grass verge Width' can not be blank"
                    )
                    priority_list.append(5)
                if cabinet.attributes["comments"] is None:
                    error_list.append("17 - 'Comments' can not be blank")
                    priority_list.append(3)

                attach_exists = next(
                    (x for x in attachments if x == cabinet.attributes["GlobalID"]),
                    None,
                )
                if not attach_exists:
                    error_list.append("16 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing cabinet {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                cabinet_dict = {
                    "DA": olt,
                    "Layer": "Cabinets",
                    "OBJECTID": cabinet.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(cabinet_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "cabinets",
                geom,
                error_list,
                max_priority,
                cabinet.attributes["Editor"],
                cabinet.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_new_constructions(self, new_constructions_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for construction in new_constructions_features:
            check_id = (
                construction.attributes["GlobalID"]
                .replace("{", "")
                .replace("}", "")
                .lower()
            )
            geom = get_feature_geometry_line("point", construction)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing new construction with {check_id}")
            try:
                if construction.attributes["comments"] is None:
                    error_list.append("1 - 'Comments' can not be blank")
                    priority_list.append(2)
                if construction.attributes["cons_type"] is None:
                    error_list.append("2 - 'Construction Type' can not be blank")
                    priority_list.append(5)

                attach_exists = next(
                    (
                        x
                        for x in attachments
                        if x == construction.attributes["GlobalID"]
                    ),
                    None,
                )
                if not attach_exists:
                    error_list.append("3 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing new construction {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                construction_dict = {
                    "DA": olt,
                    "Layer": "New Constructions",
                    "OBJECTID": construction.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(construction_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "new_constructions",
                geom,
                error_list,
                max_priority,
                construction.attributes["Editor"],
                construction.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_loc(self, loc_features, layer, olt):
        for loc in loc_features:
            check_id = (
                loc.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("polygon", loc)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing loc with {check_id}")
            try:
                if loc.attributes["loc_reason"] is None:
                    error_list.append("1 - 'LOC Reason' can not be blank")
                    priority_list.append(5)
                if (
                        loc.attributes["loc_reason"] == "Other"
                        and loc.attributes["comments"] is None
                ):
                    error_list.append(
                        "2 - If 'LOC Reason' is 'Other' then 'Comments' can not be blank"
                    )
                    priority_list.append(4)

            except Exception as e:
                logging.error(f"Error processing loc {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                loc_dict = {
                    "DA": olt,
                    "Layer": "LOC",
                    "OBJECTID": loc.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(loc_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "loc",
                geom,
                error_list,
                max_priority,
                loc.attributes["Editor"],
                loc.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_planner_awareness(self, planner_awareness_features, layer, olt):
        for planner in planner_awareness_features:
            check_id = (
                planner.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("point", planner)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing planner awareness with {check_id}")
            try:
                if planner.attributes["notes"] is None:
                    error_list.append("8 - 'Notes' can not be blank")
                    priority_list.append(3)
                else:
                    if (
                            planner.attributes["notes"] == "Other Notes"
                            and planner.attributes["comments"] is None
                    ):
                        error_list.append(
                            "9 - If 'Notes' is 'Other Notes' then 'Comments' can not be blank"
                        )
                        priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing planner awareness {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                planner_dict = {
                    "DA": olt,
                    "Layer": "Planner Awareness",
                    "OBJECTID": planner.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(planner_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "planner_awareness",
                geom,
                error_list,
                max_priority,
                planner.attributes["Editor"],
                planner.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_design_risk(self, design_risk_features, layer, olt):
        for risk in design_risk_features:
            check_id = (
                risk.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("point", risk)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing design risk with {check_id}")
            try:
                if risk.attributes["hazard_type"] is None:
                    error_list.append("1 - 'Hazard type' can not be blank")
                    priority_list.append(5)
                elif (
                        risk.attributes["hazard_type"] == "Other"
                        and risk.attributes["comments"] is None
                ):
                    error_list.append(
                        "3 - If 'Hazard type' is 'Other' then 'Comments' can not be blank"
                    )
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing design risk {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                risk_dict = {
                    "DA": olt,
                    "Layer": "Design Risk",
                    "OBJECTID": risk.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(risk_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "design_risk",
                geom,
                error_list,
                max_priority,
                risk.attributes["Editor"],
                risk.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_sed(self, sed_features, layer, olt):
        # TODO - Add logic to process SED layer once the data is available
        pass

    def process_planned_route(self, planner_route_feature, layer, olt):
        for route in planner_route_feature:
            check_id = (
                route.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("line", route)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing planned route with {check_id}")
            try:
                if route.attributes['enough_cap'] is None and route.attributes['owner'] == 10:
                    error_list.append("1 - 'Enough Capacity' can not be blank if Owner is 'BT Openreach'")
                    priority_list.append(5)
                if route.attributes['num_ways'] is None and route.attributes['owner'] == 10:
                    error_list.append("2 - 'Number of Ways' can not be blank if Owner is 'BT Openreach'")
                    priority_list.append(5)
                if route.attributes['rem_space'] is None and route.attributes['owner'] == 10:
                    error_list.append("3 - 'Remaining space in BT duct' can not be blank if Owner is 'BT Openreach'")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing planned route {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                route_dict = {
                    "DA": olt,
                    "Layer": "Planned Route",
                    "OBJECTID": route.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(route_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "planned_route",
                geom,
                error_list,
                max_priority,
                route.attributes["Editor"],
                route.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )

    def process_proposed_alternative_ug_route(self, proposed_alternative_ug_route_features, layer, olt):
        attachments = self.attachment_list_downloader(layer)
        for route in proposed_alternative_ug_route_features:
            check_id = (
                route.attributes["GlobalID"].replace("{", "").replace("}", "").lower()
            )
            geom = get_feature_geometry_line("line", route)
            priority_list = [0]
            error_list = []
            logging.debug(f"Processing proposed alternative ug route with {check_id}")
            try:
                #TODO - Re-work logic with Chikku
                if route.attributes["comments"] is None:
                    error_list.append("1 - 'Comments' can not be blank")
                    priority_list.append(2)
                if route.attributes["surface_type"] is None:
                    error_list.append("2 - 'Surface Type' can not be blank")
                    priority_list.append(5)

                attach_exists = next(
                    (x for x in attachments if x == route.attributes["GlobalID"]), None
                )
                if not attach_exists:
                    error_list.append("3 - Attachments missing")
                    priority_list.append(5)

            except Exception as e:
                logging.error(f"Error processing proposed alternative ug route {check_id}")
                logging.error(e, exc_info=True)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_info = "".join(traceback.format_tb(exc_traceback))
                output = traceback_info.split("<module>")[-1]
                route_dict = {
                    "DA": olt,
                    "Layer": "Proposed Alternative UG Route",
                    "OBJECTID": route.attributes["OBJECTID"],
                    "Error Crashing the Algorithms": output,
                }
                self.poles_list_mail.append(route_dict)

            max_priority = max(priority_list)
            self.process_feature_on_qc_layer(
                check_id,
                "proposed_alternative_ug_route",
                geom,
                error_list,
                max_priority,
                route.attributes["Editor"],
                route.attributes["EditDate"],
                self.qc_layer_json,
                self.qc_layer_layer,
            )



    def process_feature_on_qc_layer(
            self,
            global_id,
            layer,
            geometry,
            errors,
            priority,
            last_editor,
            last_edit_date,
            qc_layer_json,
            qc_layer_layer,
    ):
        if global_id not in qc_layer_json.keys():
            logging.debug(f"Error not found for {global_id} on {layer} layer")
            logging.debug(f"Errors found are {errors}")
            if errors:
                errors_concat = ", ".join(errors)
                if len(errors_concat) > 1000:
                    errors_concat = errors_concat[:999]
                new_line = {
                    "attributes": {
                        "QC_Status": 5,
                        "qc_priority": priority,
                        "error_type": get_error_type(layer),
                        "error_description": errors_concat,
                        "Number_of_errors": len(errors),
                        "CreationDate": datetime.now(),
                        "EditDate": datetime.now(),
                        "Creator": "Entegro_Ireland",
                        "Editor": "Entegro_Ireland",
                        "QC_created_date": datetime.now(),
                        "QC_User": "CF QC Automation",
                        "resolver_name": last_editor,
                        "updated_error": None,
                        "related_gid": global_id,
                    },
                    "geometry": {
                        "spatialReference": {
                            "wkid": 102100,
                            "latestWkid": 3857,
                        },
                        "x": geometry[0],
                        "y": geometry[1],
                    },
                }
                new_error_add = qc_layer_layer.edit_features(adds=[new_line])
                logging.debug(new_error_add)
                logging.info(f"Added error for {global_id} on {layer} layer")
            else:
                logging.debug(f"No errors found for {global_id} on {layer} layer")
            qc_layer_json[global_id] = {
                "error_type": get_error_type(layer),
                "QC_Status": 1,
                "qc_priority": priority,
                "error_description": errors,
                "QC_User": "CF QC Automation",
                "QC_created_date": datetime.now(),
                "related_gid": global_id,
            }
        else:
            logging.debug(f"Error already exists for {global_id} on {layer} layer")
            logging.debug(
                f"Existing error type is {qc_layer_json[global_id]['error_type']} and new error type is {get_error_type(layer)}"
            )
            logging.debug(
                f"Existing QC_Status is {qc_layer_json[global_id]['QC_Status']}"
            )
            if errors:
                errors_concat = ", ".join(errors)
                logging.debug(f"Error found for {global_id} on {layer} layer")
                logging.debug(
                    f"Existing error type is {qc_layer_json[global_id]['error_type']} and new error type is {get_error_type(layer)}"
                )
                logging.debug(
                    f"Existing QC_User is {qc_layer_json[global_id]['QC_User']} and we are expecting CF QC Automation"
                )

                if (
                        qc_layer_json[global_id]["error_type"] == str(get_error_type(layer))
                        and qc_layer_json[global_id]["QC_User"] == "CF QC Automation"
                ):
                    logging.debug(
                        f"Error already exists for {global_id} on {layer} layer"
                    )
                    if errors_concat != qc_layer_json[global_id]["Error_list"]:
                        logging.debug(
                            f"Error description changed for {global_id} on {layer} layer"
                        )
                        new_line = {
                            "attributes": {
                                "OBJECTID": qc_layer_json[global_id]["OBJECTID"],
                                "GlobalID": qc_layer_json[global_id]["GlobalID"],
                                "QC_Status": 5,
                                "qc_priority": priority,
                                "updated_error": errors_concat,
                                "EditDate": datetime.now(),
                                "Editor": "CF QC Automation",
                            },
                            "geometry": {
                                "spatialReference": {
                                    "wkid": 102100,
                                    "latestWkid": 3857,
                                },
                                "x": geometry[0],
                                "y": geometry[1],
                            },
                        }
                        qc_layer_layer.edit_features(updates=[new_line])
                        logging.info(f"Updated error for {global_id} on {layer} layer")
                    else:
                        logging.debug(
                            f"Error description same for {global_id} on {layer} layer"
                        )
            else:
                logging.debug(
                    f"No errors found for {global_id} on {layer} layer - Resolving the error"
                )
                if (
                        qc_layer_json[global_id]["error_type"] == str(get_error_type(layer))
                        and qc_layer_json[global_id]["QC_Status"] != 3
                ):
                    new_line = {
                        "attributes": {
                            "OBJECTID": qc_layer_json[global_id]["OBJECTID"],
                            "GlobalID": qc_layer_json[global_id]["GlobalID"],
                            "QC_Status": 3,
                            "EditDate": datetime.now(),
                            "Editor": "CF QC Automation",
                            "QC_resolved_date": last_edit_date,
                            "QC_fixed_approved_date": datetime.now(),
                            "QC_name_approved": "CF QC Automation",
                        },
                        "geometry": {
                            "spatialReference": {
                                "wkid": 102100,
                                "latestWkid": 3857,
                            },
                            "x": geometry[0],
                            "y": geometry[1],
                        },
                    }
                    qc_layer_layer.edit_features(updates=[new_line])
                    logging.info(f"Resolved error for {global_id} on {layer} layer")

    def attachment_list_downloader(self, layer):
        attachment_list = layer.attachments.search(as_df=True)
        try:
            object_ids_with_attachments = (
                attachment_list["PARENTGLOBALID"].drop_duplicates().tolist()
            )
        except Exception as e:
            logging.debug(f"No attachments found for {layer.properties.name}")
            object_ids_with_attachments = []
        return object_ids_with_attachments


# Main processing function
def main():
    try:
        logger = setup_logging()
        gis = setup_connections()
        mailing_list_errors = []

        # TODO - Add logic to fetch correct maps, for now using the test webmap
        webmap_list = ["3e4c917c03dc4b8f967cfc3b05799c77"]
        for webmap in webmap_list:
            item_to_check = gis.content.get(webmap)
            olt = item_to_check.title.split("_")[-1]
            layers = fetch_webmap_layers(gis, webmap)
            layers_dict = extract_correct_layers_with_id(layers)
            qc_layer = gis.content.get(layers_dict["City Fibre QC Point"]).layers[0]
            qc_layer_json = get_qc_layer_as_custom_json(
                gis, layers_dict["City Fibre QC Point"]
            )
            qc_check = LayerProcessor(qc_layer_json, qc_layer)
            for layer in layers_dict:
                if layers_dict[layer] is not None:
                    sublayer = gis.content.get(layers_dict[layer]).layers[0]
                    fset = sublayer.query()
                    if fset:
                        if layer == "Poles":
                            qc_check.process_poles(fset, sublayer, olt)
                        elif layer == "Exclusion Zone":
                            qc_check.process_exclusion_zone(fset, sublayer, olt)
                        elif layer == "Power Lines":
                            qc_check.process_power_lines(fset, sublayer, olt)
                        elif layer == "Electrical Crossing":
                            qc_check.process_electrical_crossing(fset, olt)
                        elif layer == "Existing Aerial Span":
                            qc_check.process_aerial_spans(fset, olt)
                        elif layer == "Existing BT Ducts":
                            qc_check.process_bt_ducts(fset, olt)
                        elif layer == "Proposed UG Route":
                            qc_check.process_proposed_ug_route(fset, sublayer, olt)
                        elif layer == "Chambers":
                            qc_check.process_chambers(fset, sublayer, olt)
                        elif layer == "Proposed Aerial Span":
                            qc_check.process_proposed_aerial_spans(fset, sublayer, olt)
                        elif layer == "Armoured Cables Fed":
                            qc_check.process_armoured_cable(fset, sublayer, olt)
                        elif layer == "Toby Locations":
                            qc_check.process_toby(fset, sublayer, olt)
                        elif layer == "New Demand Points":
                            qc_check.process_new_demand_points(fset, sublayer, olt)
                        elif layer == "MDU":
                            qc_check.process_mdu(fset, sublayer, olt)
                        elif layer == "Cabinets":
                            qc_check.process_cabinets(fset, sublayer, olt)
                        # TODO - Pending template modification as per Chikku. Putting on hold
                        # elif layer == "New Constructions":
                        #     qc_check.process_new_constructions(fset, sublayer, olt)
                        elif layer == "LOC":
                            qc_check.process_loc(fset, sublayer, olt)
                        elif layer == "Planner Awareness Data":
                            qc_check.process_planner_awareness(fset, sublayer, olt)
                        elif layer == "Design Risk":
                            qc_check.process_design_risk(fset, sublayer, olt)
                        elif layer == "SED":
                            qc_check.process_sed(fset, sublayer, olt)
                        elif layer == "Planned Route":
                            qc_check.process_planned_route(fset, sublayer, olt)
                        elif layer == "Proposed Alternative UG Route":
                            qc_check.process_proposed_alternative_ug_route(fset, sublayer, olt)
                        else:
                            logging.error(f"Layer {layer} not found in the processing list")
                    else:
                        logging.debug(f"No features found in {layer} layer")
                else:
                    logging.error(f"Layer {layer} not found in the webmap")
            if qc_check.poles_list_mail:
                mailing_list_errors.extend(qc_check.poles_list_mail)

        logger.info("Processing completed")

        mailing_list_errors.append(
            {"DA": "Test", "Layer": "Test", "OBJECTID": "Test", "Error Crashing the Algorithms": "Test"})

        if mailing_list_errors:
            today = datetime.now().strftime("%d %m %Y")
            today_for_file = datetime.now().strftime("%d_%m_%Y")
            my_excel_path = write_list_to_excel_new(mailing_list_errors,
                                                    f"CF_QC_Automation_Errors_{today_for_file}.xlsx")
            email_recipients = [
                "kpikos@entegro.ie",
            ]
            email_subject = f"City Fibre QC Automation Errors Report {today}"
            email_body = (f"Please find attached the QC Automation Errors Report for {today}. Please review and take "
                          f"necessary actions. \n\n Regards, \n Entegro Automation Team")
            send_email2(email_recipients, email_subject, email_body, my_excel_path)

            time.sleep(10)
            delete_file(my_excel_path)






    except Exception as e:
        logging.error("Error in main function")
        logging.error(e, exc_info=True)


if __name__ == "__main__":
    main()
