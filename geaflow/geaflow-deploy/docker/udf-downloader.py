import os
import sys
import traceback
import zipfile
import logging
import urllib2
import json
import hashlib
import time

def main(logFilePath, jarPath, jarUrlEnvName):
    logging.basicConfig(filename=logFilePath, filemode="a", format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%Y-%M-%d %H:%M:%S", level=logging.DEBUG)
    jarUrls = os.getenv(jarUrlEnvName)
    if not jarUrls or not jarUrls.strip():
        return
    urlList = json.loads(jarUrls)
    if not os.path.exists(jarPath):
        os.makedirs(jarPath)

    for jarUrl in urlList:
        try:
            url = jarUrl["url"]
            md5 = jarUrl["md5"] if jarUrl.has_key('md5') else ""
            jarName = getNameFromUrl(url)
            destination = os.path.join(jarPath, jarName)
            downloaded = downloadJarFile(url, destination, md5)
            if ".zip" in jarName and downloaded:
                unzipFile(destination, jarPath)

        except Exception as e:
            logging.error("Download file {} failed.".format(url), exc_info=True)
            raise e

def downloadJarFile(url, destination, md5):
    start = time.time()
    logging.info("Downloading {} to local {}.".format(url, destination))
    if os.path.isfile(destination):
        checkFileMd5(md5, destination, url)
        logging.info("File {} is already downloaded.".format(url))
        return False
    consoleToken = ""
    consoleUrl = os.getenv("GEAFLOW_GW_ENDPOINT")
    if consoleUrl and consoleUrl.strip() and url.startswith(consoleUrl):
        consoleToken = os.getenv("GEAFLOW_CATALOG_TOKEN")
    tokenHeaders = {"geaflow-token": consoleToken}
    request = urllib2.Request(url, headers=tokenHeaders)
    logging.info(request.headers)

    response = urllib2.urlopen(request)
    chunkSize = 1024 * 8
    with open(destination, "wb") as file:
      while True:
          tmp = response.read(chunkSize)
          if not tmp:
            break
          file.write(tmp)
      response.close()
    costSeconds = time.time() - start
    logging.info("File {} successfully downloaded. Cost: {:.5f} seconds.".format(url, costSeconds))
    checkFileMd5(md5, destination, url)
    return True

def checkFileMd5(expectedMd5, filePath, url):
    start = time.time()
    if not expectedMd5 or not expectedMd5.strip():
        return
    with open(filePath, 'rb') as file:
        tmpMd5 = hashlib.md5()
        chunkSize = 1024 * 8
        while True:
            data = file.read(chunkSize)
            if not data:
                break
            tmpMd5.update(data)
        actualMd5 = tmpMd5.hexdigest()
        if actualMd5 != expectedMd5:
            raise Exception("Md5 of file {} not match. Expected: {}. Actual: {}.".format(url, expectedMd5, actualMd5))
    costMills = 1000 * (time.time() -start)
    logging.info("Check md5 of file {} cost {:.2f} mill-seconds.".format(filePath, costMills))
def getNameFromUrl(jarUrl):
    try:
        index = jarUrl.rindex("/")
    except Exception as e:
        logging.error("Illegal url: {}".format(jarUrl), exc_info=True)
        raise e
    return jarUrl[index + 1: len(jarUrl)]

def unzipFile(filePath, outputDir):
    try:
        start = time.time()
        file = zipfile.ZipFile(filePath)
        logging.info("Start to unzip file: {}".format(filePath))
        file.extractall(outputDir)

        costMills = 1000 * (time.time() - start)
        logging.info("Unzip file {} success. Cost: {:.2f} mill-seconds.".format(filePath, costMills))
        file.close()
    except Exception as e:
        logging.error("Unzip file {} failed.".format(filePath), exc_info=True)
        raise e

if __name__ == '__main__':
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
