from msa_sdk.variables import Variables
from msa_sdk.msa_api import MSA_API
from reportlab.lib.pagesizes import letter, landscape, A2
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Image, PageBreak, KeepTogether
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from tabulate import tabulate
from os.path import basename

import smtplib
import time
import copy
import csv
import os
import pandas as pd

es_host = 'http://msa-es:9200'

index = 'ubi-sync-*'
rows_per_page = 45
doc_path = '/opt/fmc_repository/Process/MSA_Sync_Report'
file_name = time.strftime("%Y-%m-%d") + '-es-snapshot-report'

if not os.path.exists(doc_path):
	os.makedirs(doc_path)

def generate_headers_values(data):
	headers, values = [],[]
	if data:
		header_formart = [list(obj.keys()) for obj in data][0]
		headers = [headers_dic.get(h) for h in header_formart]
		values = [list(obj.values()) for obj in data]
	return headers, values

def create_pdf_story(data_type, data, story, style):
	headers, values = generate_headers_values(data)
	if values:
		df = pd.DataFrame(values[1:], columns=values[0])
		data = [df.columns.tolist()] + df.values.tolist()
		col_widths = [0] * len(data[0])
		data_adjust_width = copy.deepcopy(data)
		data_adjust_width.insert(0, headers)

		for row in data_adjust_width:
			for i, cell in enumerate(row):
				col_widths[i] = max(col_widths[i], len(str(cell)) * 10)

		data_pages = [data[i:i + rows_per_page] for i in range(0, len(data), rows_per_page)]

		for i, data_page in enumerate(data_pages):
			if i == 0:
				title = 'ElasticSearch snapshots report'
				title_style = getSampleStyleSheet()["Title"]
				title_style.fontSize = 28
				title_paragraph = Paragraph(title, title_style)

				image_path = "/opt/fmc_repository/workflow_sync_reporting/MSA_Sync_Report/ubi_logo.png"
				image_top = Image(image_path, width=180, height=40)
				image_top.hAlign = 'RIGHT'
				image_top.vAlign = 'TOP'

			data_page.insert(0, headers)
			table = Table(data_page)
			table._argW = col_widths
			table.setStyle(style)

			image_botton = Image(image_path, width=90, height=20)
			image_botton.hAlign = 'LEFT'
			image_botton.vAlign = 'BOTTON'

			text = "Report generated at " + time.ctime()
			text_style = getSampleStyleSheet()["h5"]
			text_style.alignment = 1
			text_paragraph = Paragraph(text, text_style)

			page_number = str(i + 1)
			page_style = getSampleStyleSheet()["h5"]
			page_style.alignment = 2
			page_paragraph = Paragraph(page_number, page_style)

			if i == 0:
				story.append(KeepTogether(
					[image_top, title_paragraph, Spacer(0, 10), table, Spacer(0, 10), text_paragraph, image_botton,
					page_paragraph, PageBreak()]))
			else:
				story.append(KeepTogether(
					[Spacer(0, 30), table, Spacer(0, 30), text_paragraph, image_botton, page_paragraph, PageBreak()]))

def write_to_csv(data, kibana_ip):
	headers, values = generate_headers_values(data)
	csv_file_path = doc_path + '/' + kibana_ip + '-' + file_name + '.csv'
	with open(csv_file_path, 'w', newline='') as t:
		writer = csv.writer(t)
		writer.writerow(headers)
		writer.writerows(values)
	return csv_file_path

def write_to_pdf(data, kibana_ip):
	pdf_file_path = doc_path + '/' + kibana_ip + '-' + file_name + '.pdf'

	doc = SimpleDocTemplate(pdf_file_path, pagesize=landscape(A2))
	styles = getSampleStyleSheet()
	style = TableStyle([
		('BACKGROUND', (0, 0), (-1, 0), '0x4c5b7b'),
		('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
		('ALIGN', (0, 0), (-1, -1), 'CENTER'),
		('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
		('FONTSIZE', (0, 0), (-1, 0), 12),
		('BOTTOMPADDING', (0, 0), (-1, 0), 12),
		('BACKGROUND', (0, 1), (-1, -1), '0xd7f7e4'),
		('GRID', (0, 0), (-1, -1), 1, colors.black)
	])

	story = []
	create_pdf_story('File', data, story, style)
	doc.build(story)
	return pdf_file_path

headers_dic = {
    'snapshotName': 'Snapshot',
    'repository': 'Repository',
    'indices': 'Indices',
    'shards': 'Shards',
    'failedShards': 'Failed shards',
    'dateCreated': 'Date created',
    'duration': 'Duration',
    'state': 'State',
    'policyName': 'Policy Name'
}



dev_var = Variables()
dev_var.add('es_snapshot_csv_download_link', var_type='String')
dev_var.add('es_snapshot_pdf_download_link', var_type='String')

context = Variables.task_call(dev_var)

data = context['snapshot']
kibana_ip = context["kibana_ip"]
if kibana_ip is None:
	kibana_ip = "localhost"

if data:

	context['es_snapshot_csv_download_link'] = write_to_csv(data, kibana_ip)
	
	context['es_snapshot_pdf_download_link'] = write_to_pdf(data, kibana_ip)

	ret = MSA_API.process_content('ENDED', str(len(data)) + ' records were found and reports was generated', context, True)
	print(ret)
	
else:
	ret = MSA_API.process_content('ENDED', 'no data', context, True)
	print(ret)