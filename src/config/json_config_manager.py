import json

from config.config_manger import ConfigManger


class JsonConfigManager(ConfigManger):
    def __init__(self, config_file):
        super(JsonConfigManager, self).__init__(config_file)
        self.cfg = None

    def load(self):
        with open(self.config_file, 'r') as json_file:
            json_string = json_file.read()

            self.cfg = json.loads(json_string)

        return self.cfg

    def migrate(self):
        pass

    def validate(self):
        pass
