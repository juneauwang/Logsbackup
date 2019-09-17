import ConfigParser
import logging
import os.path

logger = logging.getLogger(__name__)

class Config(object):

    def __init__(self, path_to_properties):
        if not os.path.exists(path_to_properties):
            logger.error('File "%s" does not exist!', path_to_properties)
            exit(1)

        self.cfg = ConfigParser.RawConfigParser()
        self.cfg.read(path_to_properties)


        self.ES_HOST = self.find('Common', 'es_host')
        self.BUCKET = self.find('Common', 'bucket')
        self.ROTATION_TIME = self.find('Common', 'rotation.time')
        self.GRAYLOG_HOST = self.find('Common', 'graylog_host')
        self.GRAYLOG_PASSWORD = self.find('Common', 'graylog_password')

        self.LOG_PATH = self.find('Log', 'path')
        self.LOG_LEVEL = self.find('Log', 'level')
        self.LOG_FORMAT = self.find('Log', 'format')
        self.LOG_ROTATE_MAX_BYTES = self.find('Log', 'rotate.max.bytes')
        self.LOG_ROTATE_BACKUP_COUNT = self.find('Log', 'rotate.backup.count')
        self.LOG_SERVER_FILE_NAME = self.find('Log', 'file.name')


    def find(self, section, key):
        if self.cfg.get(section, key):
            return self.cfg.get(section, key)
        else:
            logger.error('Property must be set: Section [%s], property [%s]', section, key)
