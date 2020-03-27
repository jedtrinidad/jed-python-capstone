import xml.etree.ElementTree as ET
import xmltodict

with open('sample-files/doe-a-deer.xml') as fd:
    doc = xmltodict.parse(fd.read())
    print(doc)