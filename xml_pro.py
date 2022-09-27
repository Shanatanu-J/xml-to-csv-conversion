import logging
import xml.etree.ElementTree as ET
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w",
                    format="%(asctime)s - %(levelname)s - %(message)s")

logging.info('START')

def parse_xml(xml_file):
    """parse_xml function takes the xml file as an input and parses through
       the data, Returns root Element."""
    etree_data = ET.parse(xml_file)
    root = etree_data.getroot()
    logging.info('Parsed given XML, Return the root.')
    return root

def get_req_data(root_object):
    """get_req_data function takes the root Element as input and parses through
       the data to return a list of atrributes in the result tag."""
    data = []
    for childs in root_object:
        if childs.tag == 'result':
            for doc in childs:
                req_dict = {}
                for att in doc:
                    req_dict[att.attrib['name']] = att.text
                data.append(req_dict)
    logging.info('Parse through to the first download link whose file_type is DLTINS.')
    return data

def download_req_file(data_object):
    """
        Download_req_file function takes data list as input and parses through
        to the first download link whose file_type is DLTINS and download the zip.
    """
    fname = ''
    for element in data_object:
        if element['file_type'] == 'DLTINS':
            req = requests.get(element["download_link"])
            req_zip = ZipFile(BytesIO(req.content))
            fname = req_zip.namelist()[0]
            req_zip.extractall()
            break
    logging.info('Downloaded the zip from the download link and extracted the zip ,Return the required XML.')
    return fname


if __name__ == "__main__":
    req_root = parse_xml('select.xml')
    req_data = get_req_data(req_root)
    req_fname = download_req_file(req_data)
    new_root = parse_xml(req_fname)

    data_ls, issr_ls = [], []

    for child in new_root.iter():
        temp, issr_temp = {}, {}
        if "FinInstrmGnlAttrbts" in child.tag:
            for ele in child:
                temp[ele.tag.split("}")[-1]] = ele.text
            data_ls.append(temp)

        if child.tag.endswith("}Issr"):
            issr_temp[child.tag.split("}")[-1]] = child.text
            issr_ls.append(issr_temp)

    logging.info('Parsed FinInstrmGnAttribs and Issr attributes.')

    data_ls_df = pd.DataFrame(data_ls)
    data_ls_df.drop(['ShrtNm'], axis=1, inplace=True)

    logging.info('Converted the FinInstrmGnAttribs attributes to Pandas DataFrame.')

    issr_ls_df = pd.DataFrame(issr_ls)

    logging.info('Converted the Issr attributes to Pandas DataFrame.')

    final = pd.concat([data_ls_df, issr_ls_df], axis=1)

    final.to_csv(path_or_buf="final_csv.csv", sep=',', header=True
                ,index=False, mode='w', encoding="UTF-8")

    logging.info("Conversion Completed Successfully.")


# step 5) Store the csv from step 4) in an AWS S3 bucket

# We need to use The AWS SDK for Python (Boto3), I cant acces the AWS as i don't have a credit card.
# But i can complete this requirement if you guys can provide a S3 bucket.

# #!!!!!! AWS Code !!!!!

# import boto
# import boto.s3.connection
# access_key = "temp_access_key"
# secret_key = "temp_secret_key"

# conn = boto.connect_s3(
#             aws_access_key_id = 'temp_access_key',
#             aws_secret_access_key = 'temp_secret_key')

# #creating all buckets

# bucket1 = conn.create_bucket('xml_to_csv')

# #Loading csv file in s3

# from io import StringIO
# import boto3

# hc = pd.read_csv("final.csv")

# s3 = boto3.client("s3", aws_access_key_id = "temp_access_key",
#                  aws_secret_access_key = 'temp_secret_key')

# csv_buf = StringIO()
# hc.to_csv(csv_buf, header=True, index=False)
# csv_buf.seek(0)
# s3.put_object(Bucket = "xml_to_csv", Body = csv_buf.getvalue(), key="test.csv")
