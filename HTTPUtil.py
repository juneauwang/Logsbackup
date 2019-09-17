import urllib.request
import urllib.error
import logging

logger = logging.getLogger(__name__)


def HTTPPut(url, header, body, json=True):

    try:
        req = urllib.request.Request(url, headers=header, data=body, method='PUT')
        response = urllib.request.urlopen(req)
        if response.getcode() is not 200:
            raise urllib.error.HTTPError
        if json is True:
            responseJson = response.read().decode()
            return responseJson
        elif json is False:
            return response
        elif json is None:
            return None
    except Exception as e:
        logger.error("HTTP Put failed cause %s" % e)
        print(e)
        exit(1)


def HTTPGet(url, header, json=True):
    try:
        req = urllib.request.Request(url, headers=header)
        response = urllib.request.urlopen(req)
        if response.getcode() is not 200:
            raise urllib.error.HTTPError
        if json is True:
            responseJson = response.read().decode()
            return responseJson
        elif json is False:
            return response
        elif json is None:
            return None
    except Exception as e:
        logger.error("HTTP Get failed cause %s" % e)
        print(e)
        exit(1)


def HTTPDelete(url, header, json=None):
    try:
        req = urllib.request.Request(url, headers=header, method="DELETE")
        response = urllib.request.urlopen(req)
        if response.getcode() is not 200:
            raise urllib.error.HTTPError
        if json is True:
            responseJson = response.read().decode()
            return responseJson
        elif json is False:
            return response
        elif json is None:
            return None
    except Exception as e:
        logger.error("HTTP Delete failed cause %s" % e)
        print(e)
        exit(1)
