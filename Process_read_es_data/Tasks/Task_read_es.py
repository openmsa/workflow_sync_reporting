from msa_sdk.variables import Variables
from msa_sdk.msa_api import MSA_API
from reportlab.lib.pagesizes import letter, landscape, A2
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Image, PageBreak, KeepTogether
from elasticsearch import Elasticsearch
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
doc_path = '/opt/fmc_repository/Process/workflows/esResultFiles'
file_name = time.strftime("%Y-%m-%d %H-%M-%S") + '-db-sync-report'

if not os.path.exists(doc_path):
	os.makedirs(doc_path)

def generate_headers_values(data):
	headers, values = [],[]
	if data:
		header_formart = [list(obj.keys()) for obj in data][0]
		headers = [headers_dic.get(h) for h in header_formart]
		values = [list(obj.values()) for obj in data]
	return headers, values

def process_data(data):
	table_file_data, table_db_data = [], []
	for d in data:
		d['backup_start'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(d['backup_start']))) if d['backup_start'] else ''
		d['backup_end'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(d['backup_end']))) if d['backup_end'] else ''
		d['restore_start'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(d['restore_start']))) if d['restore_start'] else ''
		d['restore_end'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(d['restore_end']))) if d['restore_end'] else ''
		formatted_timestamp = d['_timestamp_'][:-4] + d['_timestamp_'][-1]
		d['_timestamp_'] = time.strftime("%Y-%m-%d %H:%M:%S",time.strptime(formatted_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")) if d['_timestamp_'] else ''
		del d['_timestamp_epoch_']
		if d['type'] == 'FILE_SYNC':
			file_data = copy.deepcopy(d)
			del file_data['restore_end']
			del file_data['restore_start']
			del file_data['type']
			table_file_data.append(file_data)
		elif d['type'] == 'DB_SYNC':
			db_data = copy.deepcopy(d)
			del db_data['type']
			table_db_data.append(db_data)
	return data, table_file_data, table_db_data

def query_conditions(context):
	ranges = {}
	timestamp = {}
	
	if 'start_date' in context and context['start_date']:
		timeArray = time.strptime(context.get('start_date'), '%Y-%m-%d %H:%M:%S')
		start_timestamp = time.mktime(timeArray) * 1000
		timestamp['gte'] = start_timestamp


	if 'end_date' in context and context['end_date']:
		timeArray = time.strptime(context.get('end_date'), '%Y-%m-%d %H:%M:%S')
		end_timestamp = time.mktime(timeArray) * 1000
		timestamp['lte'] = end_timestamp
		
	ranges['_timestamp_epoch_'] = timestamp

	query = {
		'query': {
			'range': ranges
		},
		'size': 1000
	}
	
	return query

def search(query_condition, context):
	
	auth_key = context['auth_key']
	es = Elasticsearch(hosts=es_host, basic_auth=auth_key)
	resp = es.search(index=index, body=query_condition, scroll='1m')

	scroll_id = resp["_scroll_id"]
	total_results = resp["hits"]["total"]["value"]
	data = []

	for hit in resp["hits"]["hits"]:
		data.append(hit["_source"])

	while total_results > 0:
		response = es.scroll(scroll_id=scroll_id, scroll="1m")

		scroll_id = response["_scroll_id"]
		hits = response["hits"]["hits"]
		if len(hits) == 0:
			break

		total_results -= len(hits)
	
		for hit in hits:
			data.append(hit["_source"])
	return data

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
				col_widths[i] = max(col_widths[i], len(str(cell)) * 6)

		data_pages = [data[i:i + rows_per_page] for i in range(0, len(data), rows_per_page)]

		for i, data_page in enumerate(data_pages):
			if i == 0:
				title = data_type + '-sync-report'
				title_style = getSampleStyleSheet()["Title"]
				title_style.fontSize = 28
				title_paragraph = Paragraph(title, title_style)

				image_path = "/opt/fmc_repository/Process/workflows/read/ubi_logo.png"
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

def write_to_csv(data):
	headers, values = generate_headers_values(data)
	csv_file_path = doc_path + '/' + file_name + '.csv'
	with open(csv_file_path, 'w', newline='') as t:
		writer = csv.writer(t)
		writer.writerow(headers)
		writer.writerows(values)
	return csv_file_path

def write_to_pdf(table_file_data, table_db_data):
	pdf_file_path = doc_path + '/' + file_name + '.pdf'

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
	create_pdf_story('Db', table_db_data, story, style)
	create_pdf_story('File', table_file_data, story, style)
	doc.build(story)
	return pdf_file_path

def send_email(context):
	if 'sender_email' in context and context['sender_email']:
		sender_email = context['sender_email']
		sender_password = context['sender_password']
		receiver_email = context['receiver_email']
		if 'subject' in context and context['subject']:
			subject = context['subject']
		else:
			subject = ''
			
		if 'message' in context and context['message']:
			message = context['message']
		else:
			message = ''
		
		msg = MIMEMultipart()
		msg['From'] = sender_email
		msg['To'] = receiver_email
		msg['Subject'] = subject
		
		msg.attach(MIMEText(message, 'plain'))
		
		attachment_paths = [context['pdf_download_link'], context['csv_download_link']]
		for f in attachment_paths:
			with open(f, 'rb') as file:
				part = MIMEApplication(
					file.read(),
					Name=basename(f)
				)
				part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
				msg.attach(part)
		
		smtpObj = smtplib.SMTP()
		smtpObj._host = 'smtp.gmail.com'
		smtpObj.connect('smtp.gmail.com', 587)
		smtpObj.starttls()
		smtpObj.login(sender_email, sender_password)
		smtpObj.sendmail(sender_email, receiver_email.split(','), msg.as_string())
	
headers_dic = {
    'restore_end': 'restore end time',
    'backup_end': 'backup end time',
    'type': 'sync type',
    'dst_ip': 'dc replica ip',
    'errormsg': 'error msg',
    'src_ip': 'dc primary ip',
    'target_size': 'replica size',
    'restore_start': 'restore start time',
    'backup_start': 'backup start time',
    '_timestamp_': 'record time',
    'target_file': 'file name',
    'host': 'host',
    'id': 'dump id',
    'dc_status': 'dc status'
}



dev_var = Variables()
dev_var.add('auth_key', var_type='String')
dev_var.add('start_date', var_type='String')
dev_var.add('end_date', var_type='String')
dev_var.add('sender_email', var_type='String')
dev_var.add('sender_password', var_type='String')
dev_var.add('receiver_email', var_type='String')
dev_var.add('subject', var_type='String')
dev_var.add('message', var_type='String')
dev_var.add('send_email', var_type='Boolean')

context = Variables.task_call(dev_var)

query_condition = query_conditions(context)

data = search(query_condition, context)

if data:
	data, table_file_data, table_db_data = process_data(data)

	context['csv_download_link'] = write_to_csv(data)
	
	context['pdf_download_link'] = write_to_pdf(table_file_data, table_db_data)

	send_email(context)
	
	ret = MSA_API.process_content('ENDED', str(len(data)) + ' records were found and reports was generated', context, True)
	print(ret)
	
else:
	ret = MSA_API.process_content('ENDED', 'no data', context, True)
	print(ret)
