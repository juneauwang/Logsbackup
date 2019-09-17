import logging
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
from ElasticSearchUtil import ElasticSearch
from Config import Config
import argparse
import os
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)

def mainProcess():
    logger.info("Log backup process started at %s" % datetime.now())
    esutil= ElasticSearch(config)
    backupIndices = esutil.indiceFilter()
    esutil.createBackupSnapshot(backupIndices)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This logs backup tool helps to backup indices in ES to S3 bucket. Run everyday')
    parser.add_argument('-v', '--verbose', nargs='?', const=logging.INFO, default=logging.ERROR,
                        help='Lets you set the loglevel. Application default: ERROR. Option default: INFO')
    parser.add_argument('-c', '--config', nargs='?', const='/conf/logsbackup.ini',
                        default='/etc/logsbackup/logsbackup.ini',
                        help='Path to the configuration file. Application default: \'/conf/logsbackup.ini\'.'
                             'Option default: \'/etc/logsbackup/logsbackup.ini\'')
    #parser.add_argument('-e', '--external', action='store_true',
    #                   help='Make the auth-proxy available externally')
    args = parser.parse_args()
    config = Config(args.config)
    level = logging.getLevelName(config.LOG_LEVEL)
    formatter = logging.Formatter(config.LOG_FORMAT)
    max_rotated_files = int(config.LOG_ROTATE_BACKUP_COUNT)
    rotate_length_bytes = int(config.LOG_ROTATE_MAX_BYTES)
    server_log = os.path.join(config.LOG_PATH, config.LOG_SERVER_FILE_NAME)
    rotating_file_handler = RotatingFileHandler(filename=server_log, maxBytes=rotate_length_bytes,
                                                backupCount=max_rotated_files)
    rotating_file_handler.setLevel(level)
    rotating_file_handler.setFormatter(formatter)
    logging.getLogger('').setLevel(level)
    logging.getLogger('').addHandler(rotating_file_handler)


    scheduler = BlockingScheduler()
    scheduler.add_job(mainProcess, 'cron', hour='*', minute='*', second='*')

    try:
        scheduler.start()
    except (SystemExit):
        logger.error("Last round failed, will continue scheduler.")
        scheduler.resume()