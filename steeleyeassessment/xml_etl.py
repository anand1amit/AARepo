import xmltodict
import requests
import zipfile
import pandas as pd
import logging
import boto3
from datetime import datetime
import pathlib
import os
import urllib3

urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO)

date = datetime.today().strftime('%Y%m%d')
s3_client = boto3.client('s3', verify=False)
bucketName = "steeleyeassessment"


def download_initial_xml():
    """This function downloads the XML file from the link provided by the requirements document"""

    logging.info('Downloading initial XML as feed.xml')

    url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_' \
          'date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

    response = requests.get(url)

    with open('feed.xml', 'wb') as file:
        file.write(response.content)


download_initial_xml()


def read_file(fileName):
    """A function to read an XML file and then convert to a dictionary to make parsing easier"""

    logging.info(f'Read {fileName} and convert to dictionary')

    xmlFile = open(fileName, 'r', encoding='utf-8')

    xmlContent = xmlFile.read()
    dictContent = xmltodict.parse(xmlContent)

    return dictContent


def parse_xml():

    """This function reads the XML file containing the download link to the data file and then converts to a json
    to retrieve the download link"""

    logging.info('Parsing xml and retrieving download link to xml data file')

    fileXml = read_file('feed.xml')

    doc = fileXml['response']['result']['doc'][0]

    for string in doc['str']:
        if string['@name'] == "download_link":
            downloadLink = string['#text']
            retrieve_xml(downloadLink)
        else:
            logging.error('Download link not found')


def retrieve_xml(url):
    """A function to download and extract the zip file from the download link"""

    logging.info('Download zip file containing XML file')

    response = requests.get(url)

    with open('DLTINS_20210117_01of01.zip', 'wb') as file:
        file.write(response.content)

    logging.info('Extract zip file containing XML file')

    with zipfile.ZipFile('DLTINS_20210117_01of01.zip', 'r') as zip_ref:
        zip_ref.extractall()


parse_xml()


def upload_to_s3_bucket(filename):
    """A function to upload the CSVs to the CSVs folder in the steeleyeassessment S3 bucket"""

    logging.info(f'Uploading {filename}.csv to S3 bucket')

    objectName = f"CSVs/{filename}_{date}.csv"

    file = os.path.join(pathlib.Path(__file__).parent.resolve(), f"{filename}.csv")

    s3_client.upload_file(file, bucketName, objectName)


def flatten_xml():
    """This function reads the XML data file, converts it to a dictionary and then iterates over the required
    values and keys. It then appends this data to a list of rows if there is a match in the key. If a row does not
    match, it will be appended to a separate list which will be converted to a CSV acting as an error log.

    Once fully iterated over, the lists will be converted to Dataframes, then to CSVs and then uploaded to S3 bucket."""

    logging.info('Read xml data file and converting xml to dictionary')

    xmlFile = open('DLTINS_20210117_01of01.xml', 'r', encoding='utf-8')

    xmlContent = xmlFile.read()
    dictContent = xmltodict.parse(xmlContent)

    logging.info('Creating column headers and lists')

    header = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp',
              'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']
    rowsNewRcrd = []
    rowsTermntdRcrd = []
    rowsModfdRcrd = []
    errorRows = []

    logging.info('Iterating through dictionary and appending rows to lists')

    finInstrm = dictContent['BizData']['Pyld']['Document']['FinInstrmRptgRefDataDltaRpt']['FinInstrm']

    for r in finInstrm:
        for key, value in r.items():
            try:
                if key == 'NewRcrd':
                    row = (r['NewRcrd']['FinInstrmGnlAttrbts']['Id'],
                           r['NewRcrd']['FinInstrmGnlAttrbts']['FullNm'],
                           r['NewRcrd']['FinInstrmGnlAttrbts']['ClssfctnTp'],
                           r['NewRcrd']['FinInstrmGnlAttrbts']['CmmdtyDerivInd'],
                           r['NewRcrd']['FinInstrmGnlAttrbts']['NtnlCcy'],
                           r['NewRcrd']['Issr'])

                    rowsNewRcrd.append(row)

                elif key == 'TermntdRcrd':

                    row = (r['TermntdRcrd']['FinInstrmGnlAttrbts']['Id'],
                           r['TermntdRcrd']['FinInstrmGnlAttrbts']['FullNm'],
                           r['TermntdRcrd']['FinInstrmGnlAttrbts']['ClssfctnTp'],
                           r['TermntdRcrd']['FinInstrmGnlAttrbts']['CmmdtyDerivInd'],
                           r['TermntdRcrd']['FinInstrmGnlAttrbts']['NtnlCcy'],
                           r['TermntdRcrd']['Issr'])

                    rowsTermntdRcrd.append(row)

                elif key == 'ModfdRcrd':

                    row = (r['ModfdRcrd']['FinInstrmGnlAttrbts']['Id'],
                           r['ModfdRcrd']['FinInstrmGnlAttrbts']['FullNm'],
                           r['ModfdRcrd']['FinInstrmGnlAttrbts']['ClssfctnTp'],
                           r['ModfdRcrd']['FinInstrmGnlAttrbts']['CmmdtyDerivInd'],
                           r['ModfdRcrd']['FinInstrmGnlAttrbts']['NtnlCcy'],
                           r['ModfdRcrd']['Issr'])

                    rowsModfdRcrd.append(row)
            except:
                errorRows.append(f"Error: key not found {r}")

    logging.info('Converting lists to DataFrames')

    dataNewRcrd = pd.DataFrame(rowsNewRcrd, columns=header)
    dataTermntdRcrd = pd.DataFrame(rowsTermntdRcrd, columns=header)
    dataModfdRcrd = pd.DataFrame(rowsModfdRcrd, columns=header)

    logging.info('Converting DataFrames to CSVs')

    dataNewRcrd.to_csv('dataNewRcrd.csv', index=False)
    dataTermntdRcrd.to_csv('dataTermntdRcrd.csv', index=False)
    dataModfdRcrd.to_csv('dataModfdRcrd.csv', index=False)

    logging.info('Uploading CSVs to S3 bucket')

    upload_to_s3_bucket('dataNewRcrd')
    upload_to_s3_bucket('dataTermntdRcrd')
    upload_to_s3_bucket('dataModfdRcrd')

    logging.info('Creating error log')

    error = pd.DataFrame(errorRows)
    error.to_csv('error_rows.csv')


flatten_xml()
