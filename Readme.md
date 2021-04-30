# Load Cost Report to Elastic Search

![version][load_cost_es]

This is an sample written in python to load OCI cost report into Elastic Search.
Is required configure the permission to download cost report file chekc the procedure here: [OCI Documentation](https://docs.oracle.com/en-us/iaas/Content/Billing/Tasks/accessingusagereports.htm)
This script will download the cost report file from your OCI tenancy and create the elasticsearch index and load all entries of the cost report file. Will perform this process and also will ignore files already processed. You can add this script in a josb schedule plataform of your choice like cron, at r any other project.

Script use an configuration file __cost_report_config.cnf__ that handle all required configuration that will be needed:

## Configuration Section - Default or Global
Attribute | Description
--------- | -----------
es_server_addr | Elasticsearch server ip address
es_server_port | Elasticsearch server port
es_http_compress | Enable HTTP compression default value True
es_cost_report_index | Name of document index that will be created on Elasticsearch. Default cost_report
es_cost_report_doc_type | Type of document that will be created on Elasticsearch. Default cost.
es_cost_report_index_replicas | Number of replicas of document on Elasticsearch deployment Default value 2. Only set to 1 in development environments.


## Configuration Section - OCI cost report
Attribute | Description
--------- | -----------
usage_report_namespace | Default value bling. **Do not change**
usage_report_bucket |  Default value config['tenancy'] **Do not change**
work_directory  | Default value 'reports' **Do not change**
file_prefix | Default value 'reports/cost-csv/' **Do not change**
oci_config_path | Location of config path oci_config_path.  <br> For Linux use regular notation: /home/me/.oci/config <br> For Windows use scape: c:\\users\\me\\.oci\\config








<!-- Markdown link & img dfn's -->
[load_cost_es]: https://img.shields.io/badge/load_cost_es-1.0-brightgreen

