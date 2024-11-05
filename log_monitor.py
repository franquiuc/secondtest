import re
import requests
import json
from logger import DailyLogger


class LogMonitor:
    LOG_PATH = "/var/sharetec/python/prtg_monitor/logs"
    LOG_NAME = "prtg_monitor_push_data"
    LOG_ID = "prtg_log_monitor"
    def __init__(self, log_path, base_url, inventory_hostname, debug=False):
        self.base_url = base_url
        self.vendor = ""
        self.debug = debug
        self.log_path = log_path
        self.status = ""
        self.status_hand_check = ""
        self.total_stf = 0
        self.count_hancheck = 0
        self.channel_list = []
        self.sensor_list = []
        self.channels_in_stf = 0
        self.downtime = 0
        self.message = "Ok"
        self.inventory_hostname = inventory_hostname
        self.data_log = ""
        self.channel_limit_mode = {
                    "limitmode": 1,
                    "LimitMaxWarning": 99,
                    "LimitWarningMsg": "in STF",
                    "unit": "Percent",
                    "custom": 1
            }
        self.vendors = {
            "tfd": {
                "code_handshake": "0800",
                "index_handshake": 2
            },
            "sfd": {
                "code_handshake": "0312",
                "index_handshake": 2
            },
            "tfnd": {
                "code_handshake": "0800",
                "index_handshake": 2
            },
            "fis": {
                "code_handshake": "0800",
                "index_handshake": 2
            },
            "cop": {
                "code_handshake": "0800",
                "index_handshake": 2
            },
            "eds": {
                "code_handshake": None, # "IN :1804" index 0,
            },
            "shz": {
                "code_handshake": None, #"1804" index 2,
            },
            "cusc": {
                "code_handshake": "0800",
                "index_handshake": 1
            },
            "ngn": {
                "code_handshake": "0800",
                "index_handshake": 2
            },
            "stp": {
                "code_handshake": None
            },
            "mba": {
                "code_handshake": None
            },
            "art": {
                "code_handshake": None
            },
            "ofxapi": {
                "code_handshake": None
            },
            "ofx": {
                "code_handshake": None
            },
            "eln": {
                "code_handshake": None
            },
            "default": {
                "code_handshake": None
            }
        }

        self.logging = DailyLogger.get_logger(self.LOG_ID , self.LOG_PATH, self.LOG_NAME)


    def is_not_handshake(self, line_in):
        vendor_attrs = self.vendors.get(self.vendor) if self.vendor in self.vendors else self.vendors.get("default")
        code_hand_shake = vendor_attrs.get("code_handshake", None)
        if code_hand_shake:
            hand_check_idx = vendor_attrs.get("index_handshake")
            if line_in[hand_check_idx] == code_hand_shake:
                self.status = 'STF' if 'ST&F' == line_in[-1] else line_in[-1]
                self.count_hancheck += 1
                return False
        return True

    def verify_format(self, network_id, line):
        if network_id == 'eds':
            return True
        elif network_id == 'cop' and ("ISOIN:" in line or "ISOOUT:" in line ):
            return True
        else:
            return False

    def process_data(self):
        count = 0
        for line in self.data:
            line_dic = line.split(',') if self.verify_format(self.vendor, line) else line.split()
            fst_item = line_dic[0]
            if (fst_item == "IN" or "IN :" in fst_item or "ISOOUT:" in fst_item) and self.is_not_handshake(line_dic):
                if 'ST&F' == line_dic[-1]:
                    self.total_stf += 1
                    self.status = "STF"
                else:
                    self.status = line_dic[-1]
                count += 1

        if self.status == 'STF':
            self.channels_in_stf += 1
            self.add_channel('STF', 100, self.channel_limit_mode)
            self.message = "Service is In Store and Forward"
        elif self.status == 'Live':
            self.add_channel('STF', 0, self.channel_limit_mode)
        else:
            self.add_channel('STF', 0, self.channel_limit_mode)
            self.downtime += 1

        self.add_channel("LIVE", count)
        self.add_channel('TOTAL', count + self.count_hancheck)

        # reset values
        self.status_hand_check = ""
        self.status = ""
        self.count_hancheck = 0
        count = 0

    def add_channel(self, vendor, value, channel_limit=None):
        channel = channel_limit or {}
        channel["Channel"] = vendor
        channel["Value"] = value
        channel["Mode"] = "Absolute"
        self.channel_list.append(channel.copy())
        channel = None

    def reset_properties_values(self):
        self.sensor_list.append(self.channel_list)
        self.downtime = 0
        self.total_stf = 0
        self.channels_in_stf = 0
        self.channel_list = []

    def build_downtime(self):
        if self.downtime:
            self.add_channel('SERVICE AVAILABILITY', 0)
        else:
            self.add_channel('SERVICE AVAILABILITY', 100)

    def build_general_channels(self):
        # TODO: It'll be removed, but probably come back after
        # self.add_channel('Store & Forward', self.channels_in_stf)

        self.build_downtime()

    def send_request(self, sensor):
        if self.channel_list:
            data = {
                "prtg": {
                    "Result": self.channel_list,
                    "Text": self.message
                }
            }

            headers = {
                "Content-Type": "application/json"
            }

            session = requests.Session()

            response = session.post(self.base_url+sensor, headers=headers, data=json.dumps(data))

            session.close()
            return response

    def run(self, vendor, data: dict):
        self.vendor = vendor
        self.data = data['data']
        self.message = data.get("message")  if data.get("message") else self.message
        if self.data:
            self.process_data()
        else:
            self.add_channel('STF', 0, self.channel_limit_mode)
            self.add_channel("LIVE", 0)
            self.add_channel("TOTAL", 0)
            self.logging.info(f"DATA EMPTY - IS_DOWN: {data['is_down']}")
            self.downtime = 1 if data['is_down'] else 0

    def format_transactions_data(self):
        with open(self.log_path, 'r') as tran_data_file:
            tran_data = tran_data_file.read()
        self.logging.info(f'DATA RECEIVED: {tran_data}')
        clean_data = re.sub(r'[ ]+', ' ', tran_data).strip()
        data = json.loads(clean_data)
        # data = ast.literal_eval(clean_data)        
        # self.data_log = data
        # TODO: It'll be removed, but probably come back after
        # mix_dict = {}
        for group in data.values():
            if group:
                for key, value in group.items():
                    splited_value = value['data'].splitlines()
                    group[key]['data'] = splited_value
                    # TODO: probably come back after
                    # mix_dict[key] = splited_value
        # TODO: probably come back after
        # data[self.stf_token] = mix_dict
        output = {}

        if data.get("services"):
            for network, transactions_data in data.get("services").items():
                sensor_token = f"HM-{network}-{self.inventory_hostname}"
                output[sensor_token] = {network: transactions_data}

        if data.get("vendors"):
            for network, transactions_data in data.get("vendors").items():
                sensor_token = f"Debit-{network}-{self.inventory_hostname}"
                output[sensor_token] = {network: transactions_data}

        self.data_log = output
        return output

    def debug_test_data(self):
        if self.debug:
            for sensor in self.sensor_list:
                request = {
                    "prtg": {
                        "Result": sensor,
                        "Text": self.message
                    }
                }
                self.logging.info("/////------ data ------/////")               
                self.logging.info(json.dumps(self.data, indent=4))
                self.logging.info(f'NETWORK_ID: {self.vendor} - CHANNELS: {json.dumps(request, indent=4)}')

    def execute(self):
        try:
            data_dic = self.format_transactions_data()
            # self.logging.info(f'DATA DICT: {data_dic}')
            # print(data_dic)
            for sensor, data_d in data_dic.items():
                if data_d:
                    for process, data in data_d.items():
                        self.logging.set_prefix(process)
                        self.logging.info(f'DATA DICT: {data_d}')                        
                        self.run(process, data)

                    self.build_general_channels()
                    response = self.send_request(sensor)
                    self.reset_properties_values()
                    self.logging.info("Sensor response: %s, %s, %s", sensor, response.status_code, response.json())
            self.debug_test_data()
            self.logging.set_prefix('')

        except FileNotFoundError as ex:
            self.logging.error(f"Error FileNotFoundError: {ex}, data processing {self.data_log}")
        except TypeError as ex:
            self.logging.error(f"Error TypeError: {ex}, data processing {self.data_log}")
        except KeyError as ex:
            self.logging.error(f"Error TypeError: {ex}, data processing {self.data_log}")
        except Exception as ex:
            self.logging.error(f"Error unknown: {ex}, data processing {self.data_log}")
