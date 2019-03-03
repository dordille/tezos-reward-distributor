from calc.calculate_phase_base import CalculatePhaseBase
from model.payment_log import PaymentRecord
from model.reward_log import RewardLog
from util.rounding_command import RoundingCommand

MUTEZ = 1e+6


class CalculatePhase2(CalculatePhaseBase):
    """
    At phase 2, share of the excluded delegators are distributed among other delegators. Total reward distributed remains the same.
    """

    def __init__(self, excluded_set, prcnt_rm=RoundingCommand(None)) -> None:
        super().__init__()

        self.prcnt_rm = prcnt_rm
        self.excluded_set = excluded_set
        self.phase = 2

    def calculate(self, reward_data1, total_amount):
        """
        :param reward_data1: reward data from phase 1
        :param total_amount: total amount of rewards.
        :return: tuple (reward_data1,total_amount)
        reward_data1 is generated by excluding requested addresses. Remaining ratios are adjusted.
        total_amount is the same as the input total_amount.
        """

        # rewards, total_amount = self.old_method(reward_data0, total_amount)
        rewards = []
        total_balance_excluded = 0
        total_balance = 0

        for rl1 in self.iterateskipped(reward_data1):
            # move skipped records to next phase
            rewards.append(rl1)

        # exclude requested addresses from reward list
        for rl1 in self.filterskipped(reward_data1):

            total_balance += rl1.balance

            if rl1.address in self.excluded_set:
                rl1.skip(desc="Skipped at phase 2", phase=self.phase)
                rewards.append(rl1)

                total_balance_excluded += rl1.balance
            else:
                # -1 will be replaced with actual ratio, read below
                rewards.append(rl1)

        new_total_balance = total_balance - total_balance_excluded

        # calculate new ratio using remaining balance
        for rl2 in self.filterskipped(rewards):
            rl2.ratio2 = self.prcnt_rm.round(rl2.balance / new_total_balance)

        # total reward amount remains the same
        return rewards, total_amount

    def old_method(self, reward_data, total_amount):
        total_excluded_ratio = 0.0
        rewards = []
        # calculate how rewards will be distributed
        for pr in reward_data:
            if pr.address in self.excluded_set:
                total_excluded_ratio += pr.ratio
            else:
                rewards.append(PaymentRecord(address=pr.address, ratio=pr.ratio, type=pr.type))

        # We need to distribute excluded ratios among remaining records
        # a,b,c,d -> b*(1+(a/1-a)), c*(1+(a/1-a)), d*(1+ (a/1-a)) -> (1+(a/1-a))*(b,c,d)

        # calculate 1+(a/1-a)
        multiplier = 1 + total_excluded_ratio / (1 - total_excluded_ratio)

        # for each record, calculate new ratio
        for pr in rewards:
            pr.ratio = self.prcnt_rm.round(pr.ratio * multiplier)

        return rewards, total_amount
