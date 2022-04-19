import xmltodict
import logging

logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.ERROR)


def read_file(fileName):
    """A function to read an XML file and then convert to a dictionary to make parsing easier"""

    xmlFile = open(fileName, 'r', encoding='utf-8')

    xmlContent = xmlFile.read()
    dictContent = xmltodict.parse(xmlContent)

    return dictContent


def parse_xml():
    """A function to retrieve the download link from feed.xml"""

    fileXml = read_file('feed.xml')

    doc = fileXml['response']['result']['doc'][0]

    for string in doc['str']:
        if string['@name'] == "download_link":
            downloadLink = string['#text']
        else:
            logging.error('Download link not found')

    return downloadLink

def test_parse_xml():
    """A function to test that the retrieval of the download link from feed.xml is correct"""

    url = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'

    assert url == parse_xml()
