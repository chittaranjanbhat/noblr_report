import smtplib
import traceback
from datetime import datetime
from email.message import EmailMessage


def notification(excel_file, to, execution_time, records):

    # Month abbreviation, day and year
    date_time = datetime.today().strftime('%Y-%m-%d @ %H:%M %p')

    gmail_user = 'chittaranjanbhat1992@gmail.com'
    gmail_password = ""

    msg = EmailMessage()
    msg['Subject'] = f'Noblr Month End Payment Report: {date_time}'
    msg['From'] = gmail_user
    msg['To'] = to
    msg.add_alternative(f"""\
    <!DOCTYPE html>
    <html>
        <body>
            <h4 style="color:SlateGray;">Hello team,
            Here is the month end payment report attached, which ran on {date_time} and took {execution_time} minutes to complete. In today’s run it created the report with {records} records. If you have any questions please contact Noblr Reporting team</h4>
        </body>
    </html>
    """, subtype='html')

    try:
        with open(excel_file, 'rb') as f:
            file_data = f.read()
        msg.add_attachment(file_data, maintype="application", subtype="xlsx", filename='Report.xlsx')

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(gmail_user, gmail_password)
            smtp.send_message(msg)
        print('Email sent!')
    except Exception as e:
        error_msg = traceback.format_exc()
        print(error_msg)
        print('Something went wrong...')
