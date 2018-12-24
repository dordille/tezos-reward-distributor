import json
import os
from config.config_manger import STANDARD_FEE_KEY, SPECIALS_MAP_KEY, FOUNDERS_MAP_KEY, SUPPORTERS_KEY, \
    EXCLUDED_DELEGATORS_KEY, OWNERS_MAP_KEY, MIN_DELEGATION_KEY
from config.json_config_manager import JsonConfigManager
from log_config import main_logger

logger = main_logger


class BusinessJsonConfigManager(JsonConfigManager):
    def __init__(self, config_file):
        super(BusinessJsonConfigManager, self).__init__(config_file)

    def migrate(self):
        logger.warn("Business config file '{}' is not present. Generating it from BusinessConfig.py "
                    .format(self.config_file))

        business_config_dict = {"baking_address": "{}","paying_address": "{}", FOUNDERS_MAP_KEY: "{}",
                                STANDARD_FEE_KEY: "5",MIN_DELEGATION_KEY: "10", SPECIALS_MAP_KEY: "{}",
                                OWNERS_MAP_KEY: "{}", SUPPORTERS_KEY: "{}",
                                EXCLUDED_DELEGATORS_KEY: "{}"}

        with open(self.config_file, "wt") as json_config:
            json.dump(business_config_dict, json_config)

        logger.warn("Business config file '{}' is created. BusinessConfig.py is not used any more"
                    .format(self.config_file))

    def load(self):
        # config file is not present, call migrate
        if not os.path.isfile(self.config_file):
            self.migrate()

        super(BusinessJsonConfigManager, self).load()

        # create maps for non-existing attributes
        if not SPECIALS_MAP_KEY in self.cfg:
            self.cfg[SPECIALS_MAP_KEY] = {}

        # create lists for non-existing attributes
        if not SUPPORTERS_KEY in self.cfg:
            self.cfg[SUPPORTERS_KEY] = []
        if not EXCLUDED_DELEGATORS_KEY in self.cfg:
            self.cfg[EXCLUDED_DELEGATORS_KEY] = []

        # Merge all supporters. Use set objects to avoid duplicates.
        self.cfg["supporters"] = list(set(self.cfg[SUPPORTERS_KEY]) |
                                      set(self.cfg[FOUNDERS_MAP_KEY].keys()) |
                                      set(self.cfg[OWNERS_MAP_KEY].keys())
                                      )

    def validate(self):
        if not self.cfg:
            raise Exception("Configuration is not loaded. Run load first.")

        validate_map_share_sum(self.cfg, "founders_map")
        validate_map_share_sum(self.cfg, "owners_map")


# all shares in the map must sum up to 1
def validate_map_share_sum(config, map_name):
    if abs(100 - sum(config[map_name].values()) > 1e-4):  # a zero check actually
        raise Exception("Map '{}' shares does not sum up to 100!".format(map_name))


def test_business_json_config_manager():
    businessConfigManager = BusinessJsonConfigManager("business.json")
    businessConfigManager.load()
    businessConfigManager.validate()

    print(json.dumps(businessConfigManager.cfg, indent=4, sort_keys=True))
