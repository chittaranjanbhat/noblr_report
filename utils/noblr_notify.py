import smtplib
import traceback
from datetime import datetime
from pytz import timezone
from email.message import EmailMessage


class Notify:

    def __init__(self, report_type, schedule_type, to, now, start_time, end_time):
        self.report_type = report_type
        self.schedule_type = schedule_type
        self.mail_to = to
        self.start_time = start_time
        self.end_time = end_time
        self.mail_server = 'localhost'
        self.mail_from = ''
        self.mail_from = ''
        self.gmail_password = ""
        self.date_time = now.strftime('%Y-%m-%d @ %H:%M %p')
        self.date_today = now.strftime('%m%d%Y')
        self.month_year = now.strftime("%B %Y")

    def notification(self, excel_file_name, excel_file, execution_time, records):

        if self.report_type == 'cust_loss' and self.schedule_type == 'monthly':
            self.cust_monthly(excel_file_name, excel_file)
        elif self.report_type == 'cust_loss' and self.schedule_type == 'weekly':
            self.cust_weekly(excel_file_name, excel_file)
        elif self.report_type == 'payment' and self.schedule_type == 'daily':
            self.payment_daily(excel_file_name, excel_file)
        else:
            print('Invalid report type or invalid report schedule..!')

    def cust_weekly(self, excel_file_name, excel_file):
        msg = EmailMessage()
        msg['Subject'] = f'Noblr Weekly Customer loss PIT Report - {self.date_today}'
        msg['From'] = self.mail_from
        msg['To'] = self.mail_to
        msg.add_alternative(f"""\
            <!DOCTYPE html>
            <html>
                <body>
                   <p>
                    Hello team,
                    <br><br>Here is the attached weekly customer loss PIT report for <b>{self.month_year}</b>, ran on <b>{self.date_time} CST</b>.
                    <br> for the time period of {self.start_time} and {self.end_time} </br>
                    <br>If you have any questions please contact Noblr Reporting team.</h4>
                    </p>
                    <p>
                    <br>Thank you,
                    <br><em>Noblr Reporting Team</em>
                    </p>
                </body>
            </html>
            """, subtype='html')

        try:
            with open(excel_file, 'rb') as f:
                file_data = f.read()
            msg.add_attachment(file_data, maintype="application", subtype="xlsx", filename=excel_file_name)

            with smtplib.SMTP(self.mail_server) as smtp:
                smtp.send_message(msg)
            # with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            #     smtp.login(self.mail_from, self.gmail_password)
            #     smtp.send_message(msg)
            print('Email sent!')
        except Exception as e:
            error_msg = traceback.format_exc()
            print(error_msg)
            print('Something went wrong...')

    def cust_monthly(self, excel_file_name, excel_file):
        msg = EmailMessage()
        msg['Subject'] = f'Noblr Monthly Customer loss PIT Report - {self.date_today}'
        msg['From'] = self.mail_from
        msg['To'] = self.mail_to
        msg.add_alternative(f"""\
            <!DOCTYPE html>
            <html>
                <body>
                   <p>
                    Hello team,
                    <br><br>Here is the attached monthly Customer loss PIT report for <b>{self.month_year}</b>, ran on <b>{self.date_time} CST</b>.
                    <br> for the time period of {self.start_time} and {self.end_time} </br>
                    <br>If you have any questions please contact Noblr Reporting team.</h4>
                    </p>
                    <p>
                    <br>Thank you,
                    <br><em>Noblr Reporting Team</em>
                    </p>
                </body>
            </html>
            """, subtype='html')

        try:
            with open(excel_file, 'rb') as f:
                file_data = f.read()
            msg.add_attachment(file_data, maintype="application", subtype="xlsx", filename=excel_file_name)

            with smtplib.SMTP(self.mail_server) as smtp:
                smtp.send_message(msg)
            # with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            #     smtp.login(self.mail_from, self.gmail_password)
            #     smtp.send_message(msg)
            print('Email sent!')
        except Exception as e:
            error_msg = traceback.format_exc()
            print(error_msg)
            print('Something went wrong...')

    def payment_daily(self, excel_file_name, excel_file):
        msg = EmailMessage()
        msg['Subject'] = f'{self.date_today} : Noblr Daily Payment Report'
        msg['From'] = self.mail_from
        msg['To'] = self.mail_to
        msg.add_alternative(f"""\
            <!DOCTYPE html>
            <html>
                <body>
                   <p>
                    Hello team,
                    <br><br>Here is the <b>daily Noblr Payment Report</b> as on <b>{self.date_time} CST</b>.
                    <br>For any questions, please contact Noblr Reporting team.
                    <br> for the time period of {self.start_time} and {self.end_time} </br>
                    </p>
                    <p>
                    <br>Thank you,
                    <br><em>Noblr Reporting Team</em>
                    </p>
                </body>
            </html>
            """, subtype='html')

        try:
            with open(excel_file, 'rb') as f:
                file_data = f.read()
            msg.add_attachment(file_data, maintype="application", subtype="xlsx", filename=excel_file_name)

            # with smtplib.SMTP(self.mail_server) as smtp:
            #     smtp.send_message(msg)
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(self.mail_from, self.gmail_password)
                smtp.send_message(msg)
            print('Email sent!')
        except Exception as e:
            error_msg = traceback.format_exc()
            print(error_msg)
            print('Something went wrong...')
