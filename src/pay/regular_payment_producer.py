import _thread
import csv
import os
import threading
import time

from Constants import RunMode
from NetworkConfiguration import network_config_map
from calc.payment_calculator import PaymentCalculator
from config.config_manger import FOUNDERS_MAP_KEY, EXCLUDED_DELEGATORS_KEY, OWNERS_MAP_KEY
from log_config import main_logger
from model.payment_log import PaymentRecord
from pay.double_payment_check import check_past_payment
from tzscan.tzscan_block_api import TzScanBlockApiImpl
from tzscan.tzscan_reward_api import TzScanRewardApiImpl
from tzscan.tzscan_reward_calculator import TzScanRewardCalculatorApi
from util.dir_utils import get_calculation_report_file

logger = main_logger


class ProducerThread(threading.Thread):
    def __init__(self, queue, name, service_fee_calc, execution_config, business_config, life_cycle, calc_dir, paym_dir,
                 verbose=False):
        super(ProducerThread, self).__init__()

        self.payments_queue = queue
        self.name = name
        self.business_cfg = business_config
        self.exec_cfg = execution_config
        self.nw = network_config_map[execution_config.network]
        self.calc_dir = calc_dir
        self.payments_dir = paym_dir
        self.fee_calc = service_fee_calc
        self.verbose = verbose
        self.life_cycle = life_cycle

        self.exiting = False
        self.block_api = TzScanBlockApiImpl(self.nw)

        logger.debug('Producer started')

    def exit(self):
        if not self.exiting:
            self.payments_queue.put([self.create_exit_payment()])
            self.exiting = True

            _thread.interrupt_main()

    def run(self):
        current_cycle = self.block_api.get_current_cycle()
        payment_cycle = self.exec_cfg.initial_cycle

        # if non-positive initial_payment_cycle, set initial_payment_cycle to
        # 'current cycle - abs(initial_cycle) - (NB_FREEZE_CYCLE+1)'
        if self.exec_cfg.initial_cycle <= 0:
            payment_cycle = current_cycle - abs(self.exec_cfg.initial_payment_cycle) - \
                            (self.nw['NB_FREEZE_CYCLE'] + 1)
            logger.debug("Payment cycle is set to {}".format(payment_cycle))

        run_mode = self.exec_cfg.run_mode

        while self.life_cycle.is_running():

            # take a breath
            time.sleep(5)

            logger.debug("Trying payments for cycle {}".format(payment_cycle))

            current_level = self.block_api.get_current_level(verbose=self.verbose)
            current_cycle = self.block_api.level_to_cycle(current_level)

            # create reports dir
            self.create_reports_dir(self.calc_dir)

            logger.debug("Checking for pending payments : payment_cycle <= current_cycle - NB_FREEZE_CYCLE + 1) "
                         "- release_override")
            logger.debug("Checking for pending payments : checking {} <= {} - ({} + 1) - {}".
                         format(payment_cycle, current_cycle, self.nw['NB_FREEZE_CYCLE'],
                                self.exec_cfg.release_override))

            # payments should not pass beyond last released reward cycle
            if payment_cycle <= current_cycle - (self.nw['NB_FREEZE_CYCLE'] + 1) - self.exec_cfg.release_override:
                if not self.payments_queue.full():
                    try:

                        logger.info("Payment cycle is " + str(payment_cycle))

                        # 1- get reward data
                        reward_api = TzScanRewardApiImpl(self.nw, self.business_cfg["baking_address"])
                        reward_data = reward_api.get_rewards_for_cycle_map(payment_cycle, verbose=self.verbose)

                        # 2- make payment calculations from reward data
                        pymnt_logs, total_rewards = self.make_payment_calculations(payment_cycle, reward_data)

                        # 3- check for past payment evidence for current cycle
                        past_payment_state = check_past_payment(self.payments_dir, payment_cycle)
                        if total_rewards > 0 and past_payment_state:
                            logger.warn(past_payment_state)
                            total_rewards = 0

                        # 4- if total_rewards > 0, proceed with payment
                        if total_rewards > 0:
                            report_file_path = get_calculation_report_file(self.calc_dir, payment_cycle)

                            # 5- send to payment consumer
                            self.payments_queue.put(pymnt_logs)

                            # 6- create calculations report file. This file contains calculations details
                            self.create_calculations_report(payment_cycle, pymnt_logs, report_file_path, total_rewards)

                        # 7- next cycle
                        # processing of cycle is done
                        logger.info("Reward creation done for cycle %s", payment_cycle)

                        payment_cycle = payment_cycle + 1

                        # single run is done. Do not continue.
                        if run_mode == RunMode.ONETIME:
                            logger.info("Run mode ONETIME satisfied. Killing the thread ...")
                            self.exit()
                            break

                    except Exception as e:
                        logger.error("Error at reward calculation", exc_info=True)

                # end of queue size check
                else:
                    logger.debug("Wait a few minutes, queue is full")
                    # wait a few minutes to let payments done
                    time.sleep(60 * 3)
            # end of payment cycle check
            else:
                logger.debug(
                    "No pending payments for cycle {}, current cycle is {}".format(payment_cycle, current_cycle))

                # pending payments done. Do not wait any more.
                if run_mode == RunMode.PENDING:
                    logger.info("Run mode PENDING satisfied. Killing the thread ...")
                    self.exit()
                    break

                time.sleep(self.nw['BLOCK_TIME_IN_SEC'])

                # calculate number of blocks until end of current cycle
                nb_blocks_remaining = (current_cycle + 1) * self.nw['BLOCKS_PER_CYCLE'] - current_level
                # plus offset. cycle beginnings may be busy, move payments forward
                nb_blocks_remaining = nb_blocks_remaining + self.exec_cfg.payment_offset

                logger.debug("Wait until next cycle, for {} blocks".format(nb_blocks_remaining))

                # wait until current cycle ends
                self.waint_until_next_cycle(nb_blocks_remaining)

        # end of endless loop
        logger.info("Producer returning ...")

        # ensure consumer exits
        self.exit()

        return

    @staticmethod
    def create_reports_dir(calc_dir):
        if calc_dir and not os.path.exists(calc_dir):
            os.makedirs(calc_dir)

    def waint_until_next_cycle(self, nb_blocks_remaining):
        for x in range(nb_blocks_remaining):
            time.sleep(self.nw['BLOCK_TIME_IN_SEC'])

            # if shutting down, exit
            if not self.life_cycle.is_running():
                self.payments_queue.put([self.create_exit_payment()])
                break

    def create_calculations_report(self, payment_cycle, payment_logs, report_file_path, total_rewards):
        with open(report_file_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            # write headers and total rewards
            writer.writerow(["address", "type", "ratio", "reward", "fee_rate", "payment", "fee"])
            writer.writerow([self.business_cfg["baking_address"], "B", 1.0, total_rewards, 0, total_rewards, 0])

            for pymnt_log in payment_logs:
                # write row to csv file
                writer.writerow([pymnt_log.address, pymnt_log.type,
                                 "{0:f}".format(pymnt_log.ratio),
                                 "{0:f}".format(pymnt_log.reward),
                                 "{0:f}".format(pymnt_log.fee_rate),
                                 "{0:f}".format(pymnt_log.payment),
                                 "{0:f}".format(pymnt_log.fee)])

                logger.info("Reward created for cycle %s address %s amount %f fee %f tz type %s",
                            payment_cycle, pymnt_log.address, pymnt_log.payment, pymnt_log.fee,
                            pymnt_log.type)

    def make_payment_calculations(self, payment_cycle, reward_data):

        if reward_data["delegators_nb"] == 0:
            logger.warn("No delegators at cycle {}. Check your delegation status".format(payment_cycle))
            return [], 0

        reward_calc = TzScanRewardCalculatorApi(reward_data, self.business_cfg[FOUNDERS_MAP_KEY],
                                                self.business_cfg[EXCLUDED_DELEGATORS_KEY])

        rewards, total_rewards = reward_calc.calculate()

        logger.info("Total rewards={}".format(total_rewards))

        if total_rewards == 0: return [], 0
        fm = self.business_cfg[FOUNDERS_MAP_KEY]
        om = self.business_cfg[OWNERS_MAP_KEY]

        pymnt_calc = PaymentCalculator(fm, om, rewards, total_rewards, self.fee_calc, payment_cycle)
        payment_logs = pymnt_calc.calculate()

        return payment_logs, total_rewards

    @staticmethod
    def create_exit_payment():
        return PaymentRecord.ExitInstance()

