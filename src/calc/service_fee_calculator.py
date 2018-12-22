from config.config_manger import SUPPORTERS_KEY, SPECIALS_MAP_KEY, STANDARD_FEE_KEY


class ServiceFeeCalculator:
    def __init__(self,supporters_set,specials_map,standard_fee):
        self.supporters_set = supporters_set
        self.specials_map = specials_map
        self.standard_fee = standard_fee

    @classmethod
    def from_dict(cls, conf_dict):
        "Initialize from business config dictionary"
        return cls(conf_dict[SUPPORTERS_KEY], conf_dict[SPECIALS_MAP_KEY], conf_dict[STANDARD_FEE_KEY])

    def calculate(self, ktAddress):
        service_fee=self.standard_fee

        if ktAddress in self.supporters_set:
            service_fee=0.0
        elif ktAddress in self.specials_map:
            service_fee=self.specials_map[ktAddress]

        return service_fee


