from msa_sdk.variables import Variables
from msa_sdk.msa_api import MSA_API
from reportlab.lib.pagesizes import letter, landscape, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Flowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
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
rows_per_page = 8
doc_path = '/opt/fmc_repository/Process/MSA_Sync_Report'
file_name = time.strftime("%Y-%m-%d") + '-es-snapshot-report'

ubi_logo_path = '/opt/fmc_repository/workflow_sync_reporting/MSA_Sync_Report/logo/ubiqube_logo.png'
tm_logo_path = '/opt/fmc_repository/workflow_sync_reporting/MSA_Sync_Report/logo/tm_logo.jpg'
backgroud_path = "/opt/fmc_repository/workflow_sync_reporting/MSA_Sync_Report/logo/backgroud.jpg"

if not os.path.exists(doc_path):
	os.makedirs(doc_path)
	
def get_ubi_logo():
	image_top = Image(ubi_logo_path)
	return image_top

ubi_logo = get_ubi_logo()

def get_tm_logo():
	image_bottom = Image(tm_logo_path)
	return image_bottom

tm_logo = get_tm_logo()

table_style = TableStyle([
	('TEXTCOLOR', (0, 0), (-1, -1), '#2e3a52'),
	('ALIGN', (0, 0), (-1, -1), 'CENTRE'),
	('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
	('FONTNAME', (0, 0), (-1, 0), 'Helvetica'),
	('FONTSIZE', (0, 0), (-1, 0), 10),

	('LINEBEFORE', (1, 1), (-1, -1), 1, colors.grey), 
	('LINEABOVE', (0, 2), (-1, -1), 1, colors.grey),

])

table_wrap_text_style = ParagraphStyle(
	'table text',
	parent=getSampleStyleSheet()['Normal'],
	fontName='Helvetica',
	fontSize=9,
	alignment=0,
	textColor='#2e3a52',
)

class PositionedTitle(Flowable):
	def __init__(self, table, backgroud_image, x=0, y=0, width=500, height=200):
		Flowable.__init__(self)
		self.table = table
		self.x = x
		self.y = y
		self.width = width
		self.height = height
		self.back = backgroud_image

	def draw(self):
		self.canv.saveState()
		self.canv.translate(self.x, self.y)
		self.table.wrapOn(self.canv, self.width, self.height)
		self.back.drawWidth = 595
		self.back.drawOn(self.canv, -25, -60)
		self.table.drawOn(self.canv, 0, 0)
		self.canv.restoreState()

class TableWithBackgroundImage(Flowable):
	def __init__(self, table, img_path):
		Flowable.__init__(self)
		self.table = table
		self.img_path = img_path

	def wrap(self, availWidth, availHeight):
		self.width, self.height = self.table.wrap(availWidth, availHeight)
		return self.width, self.height

	def drawOn(self, canvas, x, y, _sW=0):
		img = Image(self.img_path, self.width, self.table._rowHeights[0])
		img.drawOn(canvas, x, y + self.height - self.table._rowHeights[0])
		self.table.drawOn(canvas, x, y)

def add_header_footer(canvas, doc):
	canvas.saveState()

	ubi = ubi_logo
	ubi.drawOn(canvas, 30, doc.pagesize[1] - 30)

	tm = tm_logo
	tm.drawOn(canvas, doc.pagesize[0] - 70, doc.pagesize[1] - 30)

	text = "%s" % canvas.getPageNumber()
	canvas.setFont("Times-Roman", 10)
	canvas.setFillColor(colors.grey)

	canvas.drawString(30, 20, 'Confidential')
	canvas.drawString(doc.pagesize[0] - 40, 20, text)

	canvas.restoreState()


def get_title(doc):
	title1 = 'MSA Infrastructure'
	title1_style = getSampleStyleSheet()["Title"]
	title1_style.fontSize = 24
	title1_style.textColor = '#2e3a52'
	title1_style.alignment = 0
	title1_style.fontName = 'Helvetica'
	title1_paragraph = Paragraph(title1, title1_style)

	title2 = 'ElasticSearch snapshots report'
	title2_style = getSampleStyleSheet()["Title"]
	title2_style.fontSize = 32
	title2_style.textColor = '#2e3a52'
	title2_style.alignment = 0
	title2_style.fontName = 'Helvetica'
	title2_paragraph = Paragraph(title2, title2_style)

	data = [[title1_paragraph], [title2_paragraph]]
	table = Table(data)

	background = Image(backgroud_path, doc.width)

	return PositionedTitle(table, background, x=0, y=150)

def generate_headers_values(data):
	headers, values = [],[]
	if data:
		header_formart = [list(obj.keys()) for obj in data][0]
		headers = [headers_dic.get(h) for h in header_formart]
		values = [list(obj.values()) for obj in data]
	return headers, values

def create_pdf_story(data_type, data, story, doc):
	headers, values = generate_headers_values(data)
	if values:
		df = pd.DataFrame(values[1:], columns=values[0])
		data = [df.columns.tolist()] + df.values.tolist()
		col_widths = [0] * len(data[0])
		data_adjust_width = copy.deepcopy(data)
		data_adjust_width.insert(0, headers)
		
		total_width = 540
		num_cols = len(headers)
		col_width = total_width / num_cols

		data_pages = [data[i:i + rows_per_page] for i in range(0, len(data), rows_per_page)]

		for i, data_page in enumerate(data_pages):


			data_page.insert(0, headers)
			table = Table(data_page, colWidths=[col_width] * num_cols, hAlign='LEFT')
			table.setStyle(table_style)
			
			for row in table._cellvalues:
				for i, cell in enumerate(row):
					row[i] = Paragraph(str(cell), table_wrap_text_style)

			table_with_bg = TableWithBackgroundImage(table, backgroud_path)

			story.append(table_with_bg)
			story.append(PageBreak())

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

	doc = SimpleDocTemplate(pdf_file_path, leftMargin=20, pagesize=A4)
	styles = getSampleStyleSheet()

	story = []
	
	title = get_title(doc)
	story.append(title)
	
	create_pdf_story('File', data, story, doc)
	doc.build(story, onFirstPage=add_header_footer, onLaterPages=add_header_footer)
	
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