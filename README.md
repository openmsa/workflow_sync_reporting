# workflow_sync_reporting

## Overview

This project is used to monitor MSA and generate reports. There are currently three workflows to generate reports:

1.Database synchronize across datacenters

2.ES snapshot

3.Database synchronize within database

Installing this repo on a live MSA
----------------------------------

Login to a live MSA as root and perform the following:

	cd /opt/fmc_repository
	git clone https://github.com/openmsa/workflow_sync_reporting.git
	chown -R ncuser. workflow_sync_reporting/
	cd Process/
	ln -s ../workflow_sync_reporting/MSA_Sync_Report MSA_Sync_Report


Browse to the MSA GUI, open "Manage Repository".

The new entry "MSA_Sync_Report" should be available and browsable
under `Automation > Workflows`.



python modules to install:

- reportlab
- smtplib
- tabulate
- elasticsearch

to install a python module run the docker CMD below

> docker exec --user ncuser <API_CONTAINER_ID> python3 -m pip install <python_module> --target /opt/fmc_repository/Process/PythonReference/

to obtain pgmetrics execution permission

> docker exec <API_CONTAINER_ID> chmod u+x /opt/fmc_repository/workflow_sync_reporting/MSA_Sync_Report/pgmetrics

Create a new instance of the workflow by executing "Create Instance" create process. This process does not need any input and just creates an instance.

Execute the different update processes for generating different kinds of reports. Below are details for the said processes:

## Generate DB Sync Across DCs Report
Input fields:

start_date: start time of es

end_date: end time of es

sender_email: sender's email address

sender_password: sender' email password

receiver_email: one or more receivers email address

subject: subject of email

message: message of email

Output:

CSV and PDF download links for reports

## Generate ES Snapshot Report
Input fields:

kibana_ip : Input MSA/kibana IP . Leave blank if kibana is on the monitoring MSA(i.e. localhost)

output: After the process completes successfully, a csv file and a pdf file will be generated with Elastic Search snapshot report along the following variables on the workflow instance details screen:

es_snapshot_csv_download_link

es_snapshot_pdf_download_link

User can click on the links shown for the files along the above variables and download the report in the desired format.

## Generate DB Sync Whin DB Report
Input fields:

Output:

Report download Link
