from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import sys

class SendAlert:

	def __init__(self, error, recipients, subject, efrom):
		self.error = error
		self.email_list = [elem.strip().split(',') for elem in recipients]
		self.msg = MIMEMultipart()
		self.msg['Subject'] = subject
		self.msg['From'] = efrom

	def send_error_message(self):
		html = """\
		<html>
			<head>"File cannot be processed because it is in the wrong format"</head>
		  		<body>
		    		{0}
		  		</body>
		</html>
		""".format(self.error.to_html())
		part1 = MIMEText(html, 'html')
		self.msg.attach(part1)
		print(self.msg)
		# it's commented when executed in localhost
		# server = smtplib.SMTP('smtp.example.com', 587)
		# server.sendmail(msg['From'], emaillist , msg.as_string())
