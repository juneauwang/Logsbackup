import urllib.request
import urllib.error
import logging
import json
from datetime import datetime
import time
from Config import Config
from HTTPUtil import *

logger = logging.getLogger(__name__)

bucketName=''
snapshotName=''
class ElasticSearch:
    def __init__(self,config):
        self.es_host = config.es_host
        self.snapshotName = config.snapshotName
        self.bucketName = config.bucketName
        self.header = {'Content-Type':'application/json'}
        self.indices=[]
    def catCurrentIndices(self):
        url = '_cat/indices?h=s,i,ss,creation.date.string'
        logger.info("Start to list all existing Indices in ES")
        try:
            response=HTTPGet(self.es_host+url,json=False)
            for a in response.readlines():
                b=a.split()
                self.indices.append(b)
        except Exception as e:
                logger.error("Unexpected error due to %s, exiting....." % e)
                exit(1)

    def indiceFilter(self):
        self.catCurrentIndices()
        today=datetime.now()
        backupIndice=[]
        logger.info("Start to filter all indices over 90 days")
        try:
            for a in self.indices:
                date=datetime.strptime(a[3],'%Y-%m-%dT%H:%M:%S.%fZ')
                if (today-date).days > 90:
                        logger.info("Found indice: %s" % a[1])
                        backupIndice.append(a[1])
        except Exception as e:
            logger.error("Unexpected error occurred due to %s, exiting....." % e)
            exit(1)
        return backupIndice

    def createBackupSnapshot(self,backupIndice):
        if not backupIndice:
            logger.info("No indice needs to be backup")
            exit(0)
        else:
            self.createSnapshot()
            for a in backupIndice:
                url='_snapshot/'+snapshotName+'/'+datetime.today().strftime('%Y-%m-%d')
                requestBody="{'indices': '%s','ignore_unavailable': 'true','include_global_state': false}" % a
                requestJson=json.dumps(requestBody).encode('utf8')
                try:
                    HTTPPut(url=self.es_host+url,header=self.header,body=requestJson,json=None)
                except Exception as e:
                    print (e)
                while self.checkSnapshotProgress(a) is True:
                    self.deleteIndice(a)
                    break


    def createSnapshot(self):
        try:
            logger.info("Check whether there is snapshot in ES")
            responseJson=HTTPGet(url=self.es_host+'_snapshot')
            if responseJson is None:
                requestBody="{'type':'s3','settings':{'bucket':"+self.bucketName+",'region':'eu-central-1'}}"
                requestJson=json.dumps(requestBody).encode('utf8')
                req=HTTPPut(url=self.es_host+'_snapshot/'+self.snapshotName+'?verify=false',header=self.header,body=requestJson,json=None)
        except Exception as e:
            print (e)
            exit(1)

    def checkSnapshotProgress(self, backupIndice):
        while True:
            try:
                logger.info("Indice Backup on-going")
                responseJson=HTTPGet(url=self.es_host+'_snapshot/'+self.snapshotName+'/%s' % backupIndice,json=True)
                if responseJson['snapshots']['state'] is "SUCCESS":
                    logger.info("Indice %s backup succeeded" % backupIndice)
                    return True
                    break
                elif responseJson['snapshots']['state'] is "FAILURE":
                    logger.info("Indice %s backup failed" % backupIndice)
                    break
                time.sleep(5)
            except Exception as e:
                print (e)
                exit(1)
        return False
    def deleteSnapshot(self, snapname):
        try:
            HTTPDelete(url=self.es_host+'_snapshot/'+self.snapshotName,json=None)
        except Exception as e:
            print (e)
            exit (1)

    def deleteIndice(self, indice):
        responseJson=HTTPDelete(self.es_host+'/%s' % indice,json=True)
        if "ackknowledge" in responseJson and responseJson['ackknowledge'] is "true":
            logger.info("indice deleted")
        else:
            logger.error("indice can't be deleted cause %s" % responseJson['status'])