import os
import sys
import traceback
import zipfile
import logging
import urllib2
import json
import hashlib

def main(logFilePath, jarPath, jarUrlEnvName):
    logging.basicConfig(filename=logFilePath, filemode="a", format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%d-%M-%Y %H:%M:%S", level=logging.DEBUG)
    jarUrls = os.getenv(jarUrlEnvName)
    if not jarUrls or not jarUrls.strip():
        return
    urlList = json.loads(jarUrls)
    if not os.path.exists(jarPath):
        os.makedirs(jarPath)

    for jarUrl in urlList:
        try:
            url = jarUrl["url"]
            md5 = jarUrl["md5"]
            jarName = getNameFromUrl(url)
            destination = os.path.join(jarPath, jarName)
            downloaded = downloadJarFile(url, destination, md5)
            if ".zip" in jarName and downloaded:
                unzipFile(destination, jarPath)

        except Exception as e:
            logging.error("download " + url + " failed", exc_info=True)
            raise e

def downloadJarFile(url, destination, md5):
    logging.info("downloading " + url + " to local " + destination)
    if os.path.isfile(destination):
        checkFileMd5(md5, destination, url)
        logging.info("file " + url + " is already downloaded")
        return False
    consoleToken = ""
    consoleUrl = os.getenv("GEAFLOW_GW_ENDPOINT")
    if consoleUrl and consoleUrl.strip() and url.startswith(consoleUrl):
        consoleToken = os.getenv("GEAFLOW_CATALOG_TOKEN")
    tokenHeaders = {"geaflow-token": consoleToken}
    request = urllib2.Request(url, headers=tokenHeaders)
    logging.info(request.headers)
    f = urllib2.urlopen(request)
    data = f.read()
    with open(destination, "wb") as file:
        file.write(data)
        logging.info("file " + url + " download down")
    checkFileMd5(md5, destination, url)
    return True

def checkFileMd5(expectedMd5, filePath, url):
    if not expectedMd5 or not expectedMd5.strip():
        return
    with open(filePath, 'rb') as file:
        actualMd5 = hashlib.md5(file.read()).hexdigest()
        if actualMd5 != expectedMd5:
            raise Exception("file md5 not match: " + url + ". Expected: " + expectedMd5
            + ". Actual: " + actualMd5)

def getNameFromUrl(jarUrl):
    try:
        index = jarUrl.rindex("/")
    except Exception as e:
        logging.error("illegal url: " + jarUrl, exc_info=True)
        raise e
    return jarUrl[index + 1: len(jarUrl)]

def unzipFile(filePath, outputDir):
    try:
        file = zipfile.ZipFile(filePath)
        logging.info("start unzip file: " + filePath)
        file.extractall(outputDir)
        logging.info("unzip file success")
        file.close()
    except Exception as e:
        logging.error("unzip " + filePath + " failed", exc_info=True)
        raise e

if __name__ == '__main__':
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
