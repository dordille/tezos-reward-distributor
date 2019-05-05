from Constants import EXIT_PAYMENT_TYPE


class PaymentRecord():
    def __init__(self, cycle=None, address=None, ratio=None, fee_ratio=None, reward=None, fee=None, type=None,
                 payment=None, paid=False, hash="", pay=True, fee_rate=None):
        self.cycle = cycle
        self.address = address
        self.ratio = ratio
        self.fee_rate = fee_rate
        self.fee_ratio = fee_ratio
        self.reward = reward
        self.fee = fee
        self.type = type
        self.payment = payment
        self.paid = paid
        self.hash = hash
        self.pay = pay

    @staticmethod
    def BakerInstance(cycle, address, reward):
        return PaymentRecord(cycle, address, 1, 0, reward, 0, 'B', reward)

    @staticmethod
    def FounderInstance(cycle, address, ratio, payment):
        return PaymentRecord(cycle, address, ratio, 0, 0, 0, 'F', payment)

    @staticmethod
    def OwnerInstance(cycle, address, ratio, reward, payment):
        return PaymentRecord(cycle, address, ratio, 0, reward, 0, 'O', payment)

    @staticmethod
    def DelegatorInstance(cycle, address, ratio, fee_rate, reward, fee, payment):
        return PaymentRecord(cycle, address, ratio, fee_rate, reward, fee, 'D', payment)

    @staticmethod
    def ExitInstance():
        return PaymentRecord(type=EXIT_PAYMENT_TYPE)

    @staticmethod
    def FromPaymentCSVDictRow(row, cyle):
        try:
            paid = int(row["paid"])
            paid = paid > 0
        except ValueError as ve:
            raise Exception("Unable to read paid value.") from ve

        return PaymentRecord(cyle, row["address"], None, None, None, None, row["type"], float(row["payment"]), paid,
                             row["hash"])

    @staticmethod
    def ManualInstance(file_name, address, payment):
        return PaymentRecord(file_name, address, None, None, payment, 0, 'M', payment)

    @staticmethod
    def FromPaymentCSVDictRows(rows, cycle):
        items = []
        for row in rows:
            print(row)
            items.append(PaymentRecord.FromPaymentCSVDictRow(row, cycle))
        return items

    @staticmethod
    def owners_key():
        return "OWNERS"

    def __str__(self):
        return "cycle = {}, address={}, ratio={}, fee_rate={}, reward={}, fee={}, type={}, payment={}, paid={}, hash={} pay={}" \
            .format(self.cycle, self.address, self.ratio, self.fee_rate, self.reward, self.fee, self.type, self.payment,
                    self.paid, self.hash, self.pay)
