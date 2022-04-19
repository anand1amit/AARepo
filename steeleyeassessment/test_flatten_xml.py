import xmltodict
import pandas as pd


def flatten_xml():
    """A function to read in the XML data file, convert to a dictionary and then retrieve the data"""

    xmlFile = open('DLTINS_20210117_01of01.xml', 'r', encoding='utf-8')

    xmlContent = xmlFile.read()
    dictContent = xmltodict.parse(xmlContent)

    finInstrm = dictContent['BizData']['Pyld']['Document']['FinInstrmRptgRefDataDltaRpt']['FinInstrm']

    return len(finInstrm)


def test_flatten_xml():
    """A function to test the flatten_xml function by comparing the number of rows in the original
    dictionary file with the total number of rows in all the output CSVs"""

    rowsNewRcrd = pd.read_csv('dataNewRcrd.csv')
    rowsModfdRcrd = pd.read_csv('dataModfdRcrd.csv')
    rowsTermntdRcrd = pd.read_csv('dataTermntdRcrd.csv')

    assert flatten_xml() == len(rowsNewRcrd) + len(rowsTermntdRcrd) + len(rowsModfdRcrd)
