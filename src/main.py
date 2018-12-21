import _thread
import argparse
import csv
import os
import queue
import sys
import threading
import time

from BusinessConfiguration import BAKING_ADDRESS, founders_map, owners_map, specials_map, STANDARD_FEE, supporters_set
from BusinessConfigurationX import excluded_delegators_set
from Constants import RunMode
from NetworkConfiguration import network_config_map
from calc.payment_calculator import PaymentCalculator
from calc.service_fee_calculator import ServiceFeeCalculator
from log_config import main_logger
from model.payment_log import PaymentRecord
from pay.double_payment_check import check_past_payment
from pay.payment_consumer import PaymentConsumer
from tzscan.tzscan_block_api import TzScanBlockApiImpl
from tzscan.tzscan_reward_api import TzScanRewardApiImpl
from tzscan.tzscan_reward_calculator import TzScanRewardCalculatorApi
from util.client_utils import get_client_path
from util.dir_utils import PAYMENT_FAILED_DIR, PAYMENT_DONE_DIR, BUSY_FILE, remove_busy_file, get_payment_root, \
    get_calculations_root, get_successful_payments_dir, get_failed_payments_dir, get_calculation_report_file
from util.process_life_cycle import ProcessLifeCycle

NB_CONSUMERS = 1
BUF_SIZE = 50
payments_queue = queue.Queue(BUF_SIZE)
logger = main_logger

lifeCycle = ProcessLifeCycle()




def main(config):
    network_config = network_config_map[config.network]
    key = config.key

    dry_run = config.dry_run_no_payments or config.dry_run
    if config.dry_run_no_payments:
        global NB_CONSUMERS
        NB_CONSUMERS = 0

    reports_dir = os.path.expanduser(config.reports_dir)
    # if in dry run mode, do not create consumers
    # create reports in dry directory
    if dry_run:
        reports_dir = os.path.expanduser("./dry")

    payments_root = get_payment_root(reports_dir, create=True)
    calculations_root = get_calculations_root(reports_dir, create=True)
    get_successful_payments_dir(payments_root, create=True)
    get_failed_payments_dir(payments_root, create=True)

    run_mode = RunMode(config.run_mode)
    node_addr = config.node_addr
    payment_offset = config.payment_offset

    client_path = get_client_path([x.strip() for x in config.executable_dirs.split(',')], config.docker, network_config,
                                  config.verbose)
    logger.debug("Client command is {}".format(client_path))

    validate_map_share_sum(founders_map, "founders map")
    validate_map_share_sum(owners_map, "owners map")

    lifeCycle.start(not dry_run)

    global supporters_set
    global excluded_delegators_set

    if not supporters_set:  # empty sets are evaluated as dict
        supporters_set = set()

    if not excluded_delegators_set:  # empty sets are evaluated as dict
        excluded_delegators_set = set()

    full_supporters_set = supporters_set | set(founders_map.keys()) | set(owners_map.keys())

    service_fee_calc = ServiceFeeCalculator(supporters_set=full_supporters_set, specials_map=specials_map,
                                            standard_fee=STANDARD_FEE)

    if config.initial_cycle is None:
        recent = None
        if get_successful_payments_dir(payments_root):
            files = sorted([os.path.splitext(x)[0] for x in os.listdir(get_successful_payments_dir(payments_root))],
                           key=lambda x: int(x))
            recent = files[-1] if len(files) > 0 else None
        # if payment logs exists set initial cycle to following cycle
        # if payment logs does not exists, set initial cycle to 0, so that payment starts from last released rewards
        config.initial_cycle = 0 if recent is None else int(recent) + 1

        logger.info("initial_cycle set to {}".format(config.initial_cycle))

    p = ProducerThread(name='producer', initial_payment_cycle=config.initial_cycle, network_config=network_config,
                       payments_dir=payments_root, calculations_dir=calculations_root, run_mode=run_mode,
                       service_fee_calc=service_fee_calc, deposit_owners_map=owners_map,
                       baker_founders_map=founders_map, baking_address=BAKING_ADDRESS, batch=config.batch,
                       release_override=config.release_override, payment_offset=payment_offset,
                       excluded_delegators_set=excluded_delegators_set, verbose=config.verbose)
    p.start()

    for i in range(NB_CONSUMERS):
        c = PaymentConsumer(name='consumer' + str(i), payments_dir=payments_root, key_name=key,
                            client_path=client_path, payments_queue=payments_queue, node_addr=node_addr,
                            verbose=config.verbose, dry_run=dry_run)
        time.sleep(1)
        c.start()
    try:
        while lifeCycle.is_running(): time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Interrupted.")
        lifeCycle.stop()


if __name__ == '__main__':

    if sys.version_info[0] < 3:
        raise Exception("Must be using Python 3")

    parser = argparse.ArgumentParser()
    parser.add_argument("key", help="tezos address or alias to make payments")
    parser.add_argument("-N", "--network", help="network name", choices=['ZERONET', 'ALPHANET', 'MAINNET'],
                        default='MAINNET')
    parser.add_argument("-r", "--reports_dir", help="Directory to create reports", default='./reports')
    parser.add_argument("-A", "--node_addr", help="Node host:port pair", default='127.0.0.1:8732')
    parser.add_argument("-D", "--dry_run",
                        help="Run without injecting payments. Suitable for testing. Does not require locking.",
                        action="store_true")
    parser.add_argument("-Dn", "--dry_run_no_payments",
                        help="Run without doing any payments. Suitable for testing. Does not require locking.",
                        action="store_true")
    parser.add_argument("-B", "--batch",
                        help="Make batch payments.",
                        action="store_true")
    parser.add_argument("-E", "--executable_dirs",
                        help="Comma separated list of directories to search for client executable. Prefer single "
                             "location when setting client directory. If -D is set, poin to location where docker "
                             "script is found. Default value is given for minimum configuration effort.",
                        default='~/,~/tezos')
    parser.add_argument("-d", "--docker",
                        help="Docker installation flag. When set, docker script location should be set in -E",
                        action="store_true")
    parser.add_argument("-V", "--verbose",
                        help="Low level details.",
                        action="store_true")
    parser.add_argument("-M", "--run_mode",
                        help="Waiting decision after making pending payments. 1: default option. Run forever. "
                             "2: Run all pending payments and exit. 3: Run for one cycle and exit. "
                             "Suitable to use with -C option.",
                        default=1, choices=[1, 2, 3], type=int)
    parser.add_argument("-R", "--release_override",
                        help="Override NB_FREEZE_CYCLE value. last released payment cycle will be "
                             "(current_cycle-(NB_FREEZE_CYCLE+1)-release_override). Suitable for future payments. "
                             "For future payments give negative values. ",
                        default=0, type=int)
    parser.add_argument("-O", "--payment_offset",
                        help="Number of blocks to wait after a cycle starts before starting payments. "
                             "This can be useful because cycle beginnings may be bussy.",
                        default=0, type=int)
    parser.add_argument("-C", "--initial_cycle",
                        help="First cycle to start payment. For last released rewards, set to 0. Non-positive values "
                             "are interpreted as : current cycle - abs(initial_cycle) - (NB_FREEZE_CYCLE+1). "
                             "If not set application will continue from last payment made or last reward released.",
                        type=int)

    args = parser.parse_args()

    logger.info("Tezos Reward Distributor is Starting")
    logger.info("Current network is {}".format(args.network))
    logger.info("Baker address is {}".format(BAKING_ADDRESS))
    logger.info("Key name {}".format(args.key))
    logger.info("--------------------------------------------")
    logger.info("Copyright Hüseyin ABANOZ 2018")
    logger.info("huseyinabanox@gmail.com")
    logger.info("Please leave copyright information")
    logger.info("--------------------------------------------")
    if args.dry_run:
        logger.info("DRY RUN MODE")
        logger.info("--------------------------------------------")
    main(args)
