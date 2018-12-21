
from abc import ABC, abstractmethod

BAKING_ADDRESS_KEY = "BAKING_ADDRESS"
STANDARD_FEE_KEY = "standard_baking_fee"
FOUNDERS_MAP_KEY = "founders_map"
SPECIALS_MAP_KEY = "specials_map"
SUPPORTERS_KEY = "supporters"
EXCLUDED_DELEGATORS_KEY = "excluded_delegators"
OWNERS_MAP_KEY = "owners_map"

class ConfigManger(ABC):
    def __init__(self, config_file):
        super(ConfigManger, self).__init__()
        self.config_file = config_file

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def migrate(self):
        pass

