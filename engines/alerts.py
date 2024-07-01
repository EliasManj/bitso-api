import smtplib
from email.message import EmailMessage
import os
from dotenv import load_dotenv

class Alerts():

    def __init__(self, user, pwd):
        self.user = user
        self.pwd = pwd

    def email_alert(self, to, subject, body):
        msg = EmailMessage()
        msg.set_content(body)
        msg['subject'] = subject
        msg['to'] = to
        msg['from'] = self.user
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(self.user, self.pwd)
        server.send_message(msg)
        server.quit()

if __name__ == "__main__":
    load_dotenv()
    user = os.getenv('EMAIL_USR')
    pwd = os.getenv('EMAIL_PWD')
    to = os.getenv('EMAIL_TO')
    alerts = Alerts(user, pwd)
    alerts.email_alert(to, "Hello wold", "Hello world! Bye")
    print(user)
    print(pwd)
    print(to)