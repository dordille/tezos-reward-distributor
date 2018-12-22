import argparse
import os
import queue
import sys
import time

from NetworkConfiguration import network_config_map
from calc.service_fee_calculator import ServiceFeeCalculator
from config.business_json_config_manager import BusinessJsonConfigManager
from log_config import main_logger
from pay.payment_consumer import PaymentConsumer
from pay.regular_payment_producer import ProducerThread
from util.client_utils import get_client_path
from util.dir_utils import get_payment_root, \
    get_calculations_root, get_successful_payments_dir, get_failed_payments_dir
from util.process_life_cycle import ProcessLifeCycle

BUF_SIZE = 50
payments_queue = queue.Queue(BUF_SIZE)
logger = main_logger
lifeCycle = None


def main(config):
    nw_cfg_sel = network_config_map[config.network]
    payment_addr_key = config.key

    dry_run = config.dry_run_no_payments or config.dry_run
    if config.dry_run_no_payments:
        global NB_CONSUMERS
        NB_CONSUMERS = 0

    # Load business configuration and validate
    buss_conf_manager = BusinessJsonConfigManager("business.json")
    buss_conf_manager.load()
    buss_conf_manager.validate()
    buss_conf = buss_conf_manager.cfg

    baking_address = buss_conf["baking_address"]
    lifeCycle = ProcessLifeCycle()

    logger.info("--------------------------------------------")
    logger.info("Baking  Address is {}".format(baking_address))
    logger.info("Payment Address is {}".format(config.key))
    logger.info("--------------------------------------------")

    reports_dir = os.path.expanduser(config.reports_dir)
    # if in dry run mode, do not create consumers
    # create reports in dry directory
    if dry_run:
        reports_dir = os.path.expanduser("./dry")

    reports_dir = os.path.join(reports_dir, baking_address)

    payments_root = get_payment_root(reports_dir, create=True)
    calculations_root = get_calculations_root(reports_dir, create=True)
    get_successful_payments_dir(payments_root, create=True)
    get_failed_payments_dir(payments_root, create=True)
    node_addr = config.node_addr

    client_path = get_client_path(
        [x.strip() for x in config.executable_dirs.split(',')], config.docker, nw_cfg_sel, config.verbose)

    logger.debug("Client command is {}".format(client_path))

    lifeCycle.start(not (dry_run or config.no_lock_check))

    # initialize fee calculator, fee calculator is used for baking service fee calculations
    # not to be confused with network fee
    service_fee_calc = ServiceFeeCalculator.from_dict(buss_conf)

    # if initial cycle is not given, determine initial cycle to start
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

    producer = ProducerThread(payments_queue, 'producer', service_fee_calc=service_fee_calc, execution_config=config,
                              business_config=buss_conf, life_cycle=lifeCycle, verbose=config.verbose,
                              calc_dir=calculations_root, paym_dir=payments_root)
    producer.start()

    consumer = PaymentConsumer(name='consumer', payments_dir=payments_root, key_name=payment_addr_key,
                               client_path=client_path, payments_queue=payments_queue, node_addr=node_addr,
                               verbose=config.verbose, dry_run=dry_run)
    consumer.start()

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
    parser.add_argument("-r", "--reports_dir", help="Directory to create reports", default='~/tezos-payments-reports')
    parser.add_argument("-A", "--node_addr", help="Node host:port pair", default='127.0.0.1:8732')
    parser.add_argument("--no-lock-check", help="Do not check for lock file. Only suitable for testing. Use with care.",
                        action="store_true")
    parser.add_argument("-D", "--dry_run",
                        help="Run without injecting payments. Suitable for testing. Does not require locking.",
                        action="store_true")
    parser.add_argument("-Dn", "--dry_run_no_payments",
                        help="Run without doing any payments. Suitable for testing. Does not require locking.",
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
    logger.info("--------------------------------------------")
    if args.dry_run:
        logger.info("DRY RUN MODE")
        logger.info("***************************************")
    main(args)
