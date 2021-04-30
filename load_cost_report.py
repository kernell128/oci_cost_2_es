import oci
import gzip
import pandas as pd
import json
import os
import shutil
import configparser
from elasticsearch import Elasticsearch
import elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from os import path
from collections import deque
# Config setup
cost_report_config = configparser.ConfigParser()
cost_report_config.read('cost_report_config.cnf')
#
es_server_addr = cost_report_config['DEFAULT']['es_server_addr']
es_server_port = cost_report_config['DEFAULT']['es_server_port']
es_http_compress = cost_report_config['DEFAULT']['es_http_compress']
es_cost_report_index = cost_report_config['DEFAULT']['es_cost_report_index']
es_cost_report_doc_type = cost_report_config['DEFAULT']['es_cost_report_doc_type']
es_cost_report_index_replicas = cost_report_config['DEFAULT']['es_cost_report_doc_type']
# OCI VARS and Config
config_path = cost_report_config['DEFAULT']['oci_config_path']
config = oci.config.from_file(config_path, 'DEFAULT')
usage_report_namespace = 'bling'
usage_report_bucket = config['tenancy']
# Cost_Report Configurations
work_directory = cost_report_config['DEFAULT']['work_directory']
file_prefix = cost_report_config['DEFAULT']['file_prefix']
#
es = Elasticsearch([{
    'host': es_server_addr,
    'port': es_server_port,
    'http_compress': es_http_compress
}])

object_storage = oci.object_storage.ObjectStorageClient(config)


def create_index():
    request_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0,
            "index.mapping.ignore_malformed": "true"
        }
    }
    es.indices.create(index=es_cost_report_index, body=request_body)


def check_process(usage_report):
    usage_query = json.dumps({'query': {'match_phrase': {'FileId': usage_report}}})
    try:
        res = es.search(index=es_cost_report_index, body=usage_query)
        if res['hits']['max_score'] is not None:
            len(res)
            return True
        else:
            return False
    except elasticsearch.exceptions.NotFoundError:
        print('New Environment, creating index')
        create_index()
        return False


def list_existing_usage_reports():
    report_list = oci.pagination.list_call_get_all_results(object_storage.list_objects, usage_report_namespace,
                                                           usage_report_bucket, prefix=file_prefix,
                                                           fields='name,size,timeCreated')
    return report_list.data.objects


def download_usage_report(usage_report):
    destination_path = work_directory
    usage_report_name = file_prefix + usage_report + '.gz'
    print(usage_report_name)
    try:
        print("Download: ", usage_report_name)
        object_details = object_storage.get_object(usage_report_namespace, usage_report_bucket,
                                                   usage_report_name)
    except oci.exceptions.ServiceError as svc_err:
        print("Object storage download error: " + svc_err)
        return
    filename = usage_report_name.rsplit('/', 1)[-1]
    with open(destination_path + '/' + filename, 'wb') as f:
        for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
            f.write(chunk)
    with gzip.open(destination_path + '/' + filename, 'rb') as f_in:
        with open(destination_path + '/' + filename.replace('.gz', ''), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def classify_report_list():
    reports_in_bucket = str(list_existing_usage_reports())
    reports_in_bucket = json.loads(reports_in_bucket)
    for usage_report in reports_in_bucket:
        filename = (usage_report['name'].rsplit('/', 1)[-1]).replace('.gz', '')
        if check_process(filename):
            usage_report_processed.append(filename)
        else:
            usage_report_to_process.append(filename)
    return usage_report_processed, usage_report_to_process


def import_usage_report_es(new_usage_report_to_process):
    local_report_file_path = work_directory + '/' + new_usage_report_to_process.replace('_cost.csv', '.csv')
    if path.exists(local_report_file_path):
        df = pd.read_csv(local_report_file_path, low_memory=False)
        df['cost/myCost'] = df['cost/myCost'].astype('float64')
        df['cost/unitPrice'] = df['cost/unitPrice'].astype('float64')
        df['cost/unitPriceOverage'] = df['cost/unitPriceOverage'].astype('float64')
        df['usage/billedQuantityOverage'] = df['usage/billedQuantityOverage'].astype('float64')
        df['usage/billedQuantity'] = df['usage/billedQuantity'].astype('float64')
        df['_id'] = df['lineItem/referenceNo']
        df['FileId'] = new_usage_report_to_process
        print('Importing: ', new_usage_report_to_process, 'Number of Docs: ', len(df.index))
        tmp = df.to_json(orient="records")
        documents = json.loads(tmp)
        #r = bulk(es, documents, index=es_cost_report_index, doc_type=es_cost_report_doc_type, request_timeout=120)
        deque(parallel_bulk(es, documents, index=es_cost_report_index, doc_type=es_cost_report_doc_type), maxlen=0)
        #print(r[0])
    else:
        print('File not Found err', local_report_file_path)


if __name__ == '__main__':
    usage_report_processed = []
    usage_report_to_process = []
    usage_report_processed, usage_report_to_process = classify_report_list()
    print('ALREADY PROCESSED: ', usage_report_processed)
    print('TO      PROCESS: ',usage_report_to_process)
    for new_usage_report in usage_report_to_process:
        print(new_usage_report)
        try:
            download_usage_report(new_usage_report)
        except oci.exceptions.ClientError as err:
            print('NotFound', new_usage_report)
            continue
        try:
            import_usage_report_es(new_usage_report.replace('.csv.gz', '_cost.csv'))
        except elasticsearch.ElasticsearchException as es_err:
            print(es_err)
            exit(24)
        try:
            os.remove(work_directory + '/' + new_usage_report)
            os.remove(work_directory + '/' + new_usage_report.replace('.csv', '.csv.gz'))
        except os.error as err:
            print('Failed to remove: ' + err)
        # on err find /home/opc/oci/scripts/reports -mmin +2 -type f -exec rm -fv {} \;
