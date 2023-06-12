# workflow_sync_reporting

Installing this repo on a live MSA
----------------------------------

Login to a live MSA as root and perform the following:

	cd /opt/fmc_repository
	git clone https://github.com/openmsa/workflow_sync_reporting.git
	chown -R ncuser. workflow_sync_reporting/
	cd Process/
	ln -s ../workflow_sync_reporting/DB_Sync_Report DB_Sync_Report


Browse to the MSA GUI, open "Manage Repository".

The new entry "DB_Sync_Report" should be available and browsable
under `Automation > Workflows`.





python modules to install:

- reportlab
- smtplib
- tabulate
- elasticsearch

to install a python module run the docker CMD below

> docker exec --user ncuser <API_CONTAINER_ID> python3 -m pip install <python_module> --target /opt/fmc_repository/Process/PythonReference/

