from api.reward_calculator_api import RewardCalculatorApi
from log_config import main_logger
from model.payment_log import PaymentRecord
from util.rounding_command import RoundingCommand

MUTEZ = 1000000


class TzScanRewardCalculatorApi(RewardCalculatorApi):
    # reward_data : payment map returned from tzscan
    def __init__(self, founders_map, min_delegation_amt, excluded_set, rc=RoundingCommand(None)):
        super(TzScanRewardCalculatorApi, self).__init__(founders_map, excluded_set)
        self.min_delegation_amt_mutez = min_delegation_amt * MUTEZ
        self.logger = main_logger
        self.rc = rc

    ##
    # return rewards    : tuple (list of PaymentRecord objects, total rewards)
    def calculate(self, reward_data):
        root = reward_data

        delegate_staking_balance = int(root["delegate_staking_balance"])
        blocks_rewards = int(root["blocks_rewards"])
        future_blocks_rewards = int(root["future_blocks_rewards"])
        endorsements_rewards = int(root["endorsements_rewards"])
        future_endorsements_rewards = int(root["future_endorsements_rewards"])
        lost_rewards_denounciation = int(root["lost_rewards_denounciation"])
        lost_fees_denounciation = int(root["lost_fees_denounciation"])
        fees = int(root["fees"])

        self.total_rewards = (blocks_rewards + endorsements_rewards + future_blocks_rewards +
                              future_endorsements_rewards + fees - lost_rewards_denounciation - lost_fees_denounciation) / MUTEZ

        delegators_balance = root["delegators_balance"]

        effective_delegate_staking_balance = delegate_staking_balance
        effective_delegators_balance = []

        # excluded addresses are processed
        for dbalance in delegators_balance:
            address = dbalance[0]["tz"]
            balance = int(dbalance[1])

            if address in self.excluded_set:
                effective_delegate_staking_balance -= balance
                continue
            effective_delegators_balance.append(dbalance)

        rewards = []
        # calculate how rewards will be distributed
        for dbalance in effective_delegators_balance:
            address = dbalance[0]["tz"]
            balance = int(dbalance[1])

            # Skip those that did not delegate minimum amount
            if balance < self.min_delegation_amt_mutez:
                self.logger.debug("Skipping '{}': Low delegation amount ({:.6f})".format(address, (balance / MUTEZ)))
                continue

            ratio = self.rc.round(balance / effective_delegate_staking_balance)
            reward = (self.total_rewards * ratio)

            reward_item = PaymentRecord(address=address, reward=reward, ratio=ratio)

            rewards.append(reward_item)

        return rewards, self.total_rewards
