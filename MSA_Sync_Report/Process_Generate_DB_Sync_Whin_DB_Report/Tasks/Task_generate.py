import subprocess
import os
import time
import json
import copy

from msa_sdk.variables import Variables
from msa_sdk.msa_api import MSA_API
from reportlab.lib.pagesizes import A4
from reportlab.platypus import Paragraph, Spacer, Flowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Image, PageBreak, KeepTogether

pdf_path = '/opt/fmc_repository/Process/MSA_Sync_Report/esResultFiles'
if not os.path.exists(pdf_path):
	os.makedirs(pdf_path)
ubi_logo_path = '/opt/fmc_repository/Process/MSA_Sync_Report/logo/ubiqube_logo.png'
tm_logo_path = '/opt/fmc_repository/Process/MSA_Sync_Report/logo/tm_logo.jpg'
backgroud_path = "/opt/fmc_repository/Process/MSA_Sync_Report/logo/backgroud.jpg"
file_name = time.strftime("%Y-%m-%d %H-%M-%S") + '-MSA-infrastructure-status-report.pdf'

table_style = TableStyle([
	('TEXTCOLOR', (0, 0), (-1, -1), '#2e3a52'),
	('ALIGN', (0, 0), (-1, -1), 'CENTRE'),
	('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
	('FONTNAME', (0, 0), (-1, 0), 'Helvetica'),
	('FONTSIZE', (0, 0), (-1, 0), 10),
	('LINEBEFORE', (1, 1), (-1, -1), 1, colors.grey), 
	('LINEABOVE', (0, 2), (-1, -1), 1, colors.grey),

])

text_box_style = TableStyle([
	('FONTSIZE', (0, 0), (-1, -1), 12),
	('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
	('LINEAFTER', (0, 0), (0, -1), 1, colors.grey),
	('BOX', (0, 0), (-1, -1), 1, colors.grey),
	('TEXTCOLOR', (0, 0), (-1, -1), '#2e3a52'),
])

table_title_style = ParagraphStyle(
	'table title',
	parent=getSampleStyleSheet()['Normal'],
	fontName='Helvetica',
	fontSize=14,
	alignment=0,
	textColor='#2e3a52',
)

table_wrap_text_style = ParagraphStyle(
	'table text',
	parent=getSampleStyleSheet()['Normal'],
	fontName='Helvetica',
	fontSize=9,
	alignment=0,
	textColor='#2e3a52',
)

def get_ubi_logo():
	image_top = Image(ubi_logo_path)
	return image_top

ubi_logo = get_ubi_logo()

def get_tm_logo():
	image_bottom = Image(tm_logo_path)
	return image_bottom

tm_logo = get_tm_logo()

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
		
def get_title():
	title1 = 'MSA Infrastructure'
	title1_style = getSampleStyleSheet()["Title"]
	title1_style.fontSize = 24
	title1_style.textColor = '#2e3a52'
	title1_style.alignment = 0
	title1_style.fontName = 'Helvetica'
	title1_paragraph = Paragraph(title1, title1_style)

	title2 = 'Status Report'
	title2_style = getSampleStyleSheet()["Title"]
	title2_style.fontSize = 40
	title2_style.textColor = '#2e3a52'
	title2_style.alignment = 0
	title2_style.fontName = 'Helvetica'
	title2_paragraph = Paragraph(title2, title2_style)

	data = [[title1_paragraph], [title2_paragraph]]
	table = Table(data)

	background = Image(backgroud_path, doc.width)

	return PositionedTitle(table, background, x=0, y=150)

def get_page_text(page_number):
	page_style = getSampleStyleSheet()["h5"]
	page_style.alignment = 2
	page_paragraph = Paragraph(str(page_number), page_style)
	return page_paragraph

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

def generate_headers_values(data):
	headers, values = [], []
	if data:
		headers = [list(obj.keys()) for obj in data][0]
		values = [list(obj.values()) for obj in data]
	return headers, values

def get_roles_dic(roles):
	result = {}
	for r in roles:
		oid = r.get('oid')
		name = r.get('name')
		result[oid] = name
	return result

def get_tablespaces_dic(tablespaces):
	result = {}
	for t in tablespaces:
		oid = t.get('oid')
		name = t.get('name')
		result[oid] = name
	return result

def add_db_cluster_info(data, story):
	d = [
		['Data Collected At:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))],
		['Cluster Name:', data.get('name')],
		['Server Version:', data.get('settings').get('server_version').get('setting')],
		['Server Started:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(data.get('start_time')))) if data.get('start_time') else ''],
		['Checkpoint LSN:', data.get('checkpoint_lsn')],
		['Last Checkpoint:',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(data.get('checkpoint_time')))) if data.get('checkpoint_time') else ''],
		['Active Backends:', len(data.get('backends'))],
		['Wal Count:', data.get('wal_count')],
		['Wal Ready Count:', data.get('wal_ready_count')],
		['Wal Replay Paused?', "yes" if data.get('is_wal_replay_paused') else "no"],
		['Recovery Mode? ', "yes" if data.get('is_in_recovery') else "no"],
	]

	title_text = '<font size=14><b>PostgreSQL Cluster Overview:</b></font>'

	title = Paragraph(title_text, table_title_style)

	story.append(title)
	story.append(Spacer(0, 10))

	table = Table(d , hAlign='LEFT')
	table.setStyle(text_box_style)
	story.append(table)
	story.append(Spacer(0, 20))

def add_bg_writer_info(data, story):
	bg_writer = data.get('bg_writer')

	sched = bg_writer.get('checkpoints_timed') if bg_writer.get('checkpoints_timed') else 0
	req = bg_writer.get('checkpoints_req') if bg_writer.get('checkpoints_req') else 0
	total_checkpoints = sched + req

	d1 = [
		['Total Checkpoints', str(sched) + ' sched + ' + str(req) + ' req = ' + str(total_checkpoints)],
		['Buffers Allocated', str(bg_writer.get('buffers_alloc')) + ' KiB'],
		['Buffers Written', bg_writer.get('buffers_checkpoint')],
		['Buffers Clean', bg_writer.get('buffers_clean')],
		['Buffers Backend', bg_writer.get('buffers_backend')],
		['Counts Since',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(bg_writer.get('stats_reset')))) if bg_writer.get('stats_reset') else '']
	]

	title_text = '<font size=14><b>BG Writer:</b></font>'
	title = Paragraph(title_text, table_title_style)
	title.alignment = 0
	story.append(title)
	story.append(Spacer(0, 10))

	table1 = Table(d1, hAlign='LEFT')
	table1.setStyle(text_box_style)
	story.append(table1)
	story.append(Spacer(0, 20))

def add_outgoing_replication(data, story):
	outgoing_replication_data = data.get('replication_outgoing')
	title_text = '<font size=14><b>Outgoing Replication:</b></font>'
	title = Paragraph(title_text, table_title_style)
	story.append(title)
	story.append(Spacer(0, 10))
	headers_dic = {
		'role_name': 'User',
		'pid': 'Pid',
		'application_name': 'Application',
		'client_addr': 'Client Address',
		'state': 'State',
		'backend_start': 'Started At',
		'sent_lsn': 'Sent LSN',
		'write_lsn': 'Written Until',
		'flush_lsn': 'Flushed Until',
		'replay_lsn': 'Replayed Until',
		'write_lag': 'Write Lag',
		'flush_lag': 'Flush Lag',
	}
	if outgoing_replication_data:
		new_replication = []
		for d in outgoing_replication_data:
			d['backend_start'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(d['backend_start']))) if d['backend_start'] else ''
			d['write_lag'] = ' (' + d['write_lag'] + ' write lag)' if d['write_lag'] else '(no write lag)'
			d['flush_lag'] = ' (' + d['flush_lag'] + ' flush lag)' if d['flush_lag'] else '(no flush lag)'
			d['replay_lag'] = ' (' + d['replay_lag'] + ' replay lag)' if d['replay_lag'] else '(no replay lag)'
			
			del d['backend_xmin']
			del d['reply_time']
			del d['pid']
			del d['write_lsn']
			del d['flush_lsn']
			del d['replay_lsn']
			new_replication.append(d)
		
		headers, values = generate_headers_values(new_replication)
		headers = [headers_dic.get(h) for h in headers]

		values.insert(0, headers)

		total_width = 540
		num_cols = len(headers)
		col_width = total_width / num_cols

		data_pages = [values[i:i + 34] for i in range(0, len(values), 34)]

		for i, data_page in enumerate(data_pages):
			if i != 0:
				data_page.insert(0, headers)

			table = Table(data_page, colWidths=[col_width] * num_cols, hAlign='LEFT')
			table.setStyle(table_style)
			for row in table._cellvalues:
				for i, cell in enumerate(row):
					row[i] = Paragraph(str(cell), table_wrap_text_style)

			table_with_bg = TableWithBackgroundImage(table, backgroud_path)
			story.append(table_with_bg)

		story.append(PageBreak())

def add_roles(data, story):
	roles = data.get('roles')

	title_text = '<font size=14><b>Roles:</b></font>'
	title = Paragraph(title_text, table_title_style)
	story.append(title)
	story.append(Spacer(0, 10))

	headers_dic = {
		'name': 'Name',
		'rolsuper': 'Super',
		'rolinherit': 'Inherit',
		'rolcreaterole': 'Creat Rol',
		'rolcreatedb': 'Creat DB',
		'rolcanlogin': 'Login',
		'rolreplication': 'Repl',
		'rolbypassrls': 'Bypass RLS',
		'rolvaliduntil': 'Expires',
		'memberof': 'Member Of'
	}
	new_roles = []
	for r in roles:
		d = copy.deepcopy(r)
		del d['oid']
		del d['rolconnlimit']
		if d['rolsuper']:
			d['rolsuper'] = 'yes'
		else:
			d['rolsuper'] = ''

		if d['rolinherit']:
			d['rolinherit'] = 'yes'
		else:
			d['rolinherit'] = ''

		if d['rolcreaterole']:
			d['rolcreaterole'] = 'yes'
		else:
			d['rolcreaterole'] = ''

		if d['rolcreatedb']:
			d['rolcreatedb'] = 'yes'
		else:
			d['rolcreatedb'] = ''

		if d['rolcanlogin']:
			d['rolcanlogin'] = 'yes'
		else:
			d['rolcanlogin'] = ''

		if d['rolreplication']:
			d['rolreplication'] = 'yes'
		else:
			d['rolreplication'] = ''

		if d['rolbypassrls']:
			d['rolbypassrls'] = 'yes'
		else:
			d['rolbypassrls'] = ''

		if not d['rolvaliduntil']:
			d['rolvaliduntil'] = ''

		if d['memberof']:
			d['memberof'] = ",".join(d['memberof'])

		del d['memberof']
		del d['rolvaliduntil']
		new_roles.append(d)

	headers, values = generate_headers_values(new_roles)
	headers = [headers_dic.get(h) for h in headers]

	values.insert(0, headers)

	table2 = Table(values, hAlign='LEFT')
	table2.setStyle(table_style)

	table_with_bg = TableWithBackgroundImage(table2, backgroud_path)
	story.append(table_with_bg)
	story.append(Spacer(0, 20))

def add_databases(data, story):
	databases = data.get('databases')
	role_dic = get_roles_dic(data.get('roles'))
	tablespace_dic = get_tablespaces_dic(data.get('tablespaces'))

	title_text = '<font size=14><b>Databases:</b></font>'
	title = Paragraph(title_text, table_title_style)
	story.append(title)
	story.append(Spacer(0, 10))

	headers_dic = {
		'name': 'Name',
		'datdba': 'Owner',
		'dattablespace': 'Tablespace',
		'numbackends': 'Backends',
		'age_datfrozenxid': 'Xid Age',
		'Rows Changed': 'Rows Changed',
		'size': 'Size'
	}

	new_roles = []
	for d in databases:
		db = copy.deepcopy(d)
		db['datdba'] = role_dic.get(db['datdba'])
		db['dattablespace'] = tablespace_dic.get(db['dattablespace'])
		db['Rows Changed'] = 'ins ' + str(db.get('tup_inserted')) + ', upd ' + str(db.get('tup_updated')) + ', del ' + str(db.get('tup_deleted'))
		db['size'] = str(round(db.get('size') / 1048576)) + ' MiB'

		del db['oid']
		del db['datconnlimit']
		del db['xact_commit']
		del db['xact_rollback']
		del db['blks_read']
		del db['blks_hit']
		del db['tup_returned']
		del db['tup_fetched']
		del db['tup_inserted']
		del db['tup_updated']
		del db['tup_deleted']
		del db['conflicts']
		del db['temp_files']
		del db['temp_bytes']
		del db['deadlocks']
		del db['blk_read_time']
		del db['stats_reset']
		del db['blk_write_time']

		new_roles.append(db)

	headers, values = generate_headers_values(new_roles)
	headers = [headers_dic.get(h) for h in headers]

	values.insert(0, headers)
	table1 = Table(values, hAlign='LEFT')
	table1.setStyle(table_style)

	table_with_bg = TableWithBackgroundImage(table1, backgroud_path)
	story.append(table_with_bg)
	story.append(Spacer(0, 5))

def add_backends(data, story):
	backends = data.get('backends')

	total_backends = len(backends)
	total_connect = data.get('settings').get('max_connections').get('setting')
	title_text = '<font size=14><b>Backends: ' + str(total_backends) + ' ( max ' + total_connect + ') </b></font>'
	title = Paragraph(title_text, table_title_style)

	headers_dic = {
		'pid': 'PID',
		'role_name': 'User',
		'application_name': 'Application Name',
		'client_addr': 'Client Addr',
		'db_name': 'Database',
		'wait': 'Wait',
		'query_start': 'Query Start',
	}
	if backends:
		new_backends = []
		for d in backends:
			d['query_start'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(d['query_start']))) if d[
				'query_start'] else ''
			d['wait'] = d['wait_event']

			del d['backend_start']
			del d['xact_start']
			del d['state_change']
			del d['state']
			del d['backend_xid']
			del d['backend_xmin']
			del d['query']
			del d['wait_event_type']
			del d['wait_event']
			value = d['pid']
			del d['pid']
			new_backends.append({'pid': value, **d})

		headers, values = generate_headers_values(new_backends)
		headers = [headers_dic.get(h) for h in headers]

		values.insert(0, headers)

		data_pages = [values[i:i + 35] for i in range(0, len(values), 35)]

		for i, data_page in enumerate(data_pages):
			if i != 0:
				data_page.insert(0, headers)
			table = Table(data_page, hAlign='LEFT')
			table.setStyle(table_style)
			table_with_bg = TableWithBackgroundImage(table, backgroud_path)
			if i == 0:

				story.append(KeepTogether([title, Spacer(0, 10), table_with_bg, PageBreak()]))
			elif i == len(data_pages) - 1:
				story.append(KeepTogether([table_with_bg, Spacer(0, 20)]))
			else:
				story.append(KeepTogether([table_with_bg, PageBreak()]))

dev_var = Variables()
#dev_var.add('db_username', var_type='String')
#dev_var.add('db_password', var_type='String')
dev_var.add('MSA_infrastructure_status_pdf_download_link', var_type='String')
context = Variables.task_call(dev_var)


context['db_username']="ncgest"
context['db_password']="Sec_52jlkhin_b4hdh"

command = 'PGPASSWORD='+ context['db_password'] +' ./opt/fmc_repository/Process/MSA_Sync_Report/pgmetrics -f json -h db -p 5432 -U '+ context['db_username'] +' POSTGRESQL'
result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
data = result.stdout


if data:
	data = json.loads(data)
	
	doc = SimpleDocTemplate(pdf_path +'/'+ file_name, leftMargin=20, pagesize=A4)

	story = []
	styles = getSampleStyleSheet()
	
	title = get_title()
	story.append(title)
	
	db_cluster_info = add_db_cluster_info(data, story)

	add_bg_writer_info(data, story)

	add_outgoing_replication(data, story)

	add_roles(data, story)
	
	add_databases(data, story)
	
	add_backends(data, story)
	
	doc.build(story, onFirstPage=add_header_footer, onLaterPages=add_header_footer)
	
	context['MSA_infrastructure_status_pdf_download_link'] = pdf_path +'/'+ file_name

	ret = MSA_API.process_content('ENDED', "ok", context, True)

	print(ret)
else:
	ret = MSA_API.process_content('ENDED', 'no data', context, True)
	print(ret)
