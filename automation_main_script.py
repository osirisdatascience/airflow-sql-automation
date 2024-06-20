import os
import pymssql
import datetime
import pendulum
import smtplib,ssl
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

with DAG(
        dag_id="DAG_AUTOMATIC_NOTIFICATION_PYTHON",
        default_args={"retries": 2},
        description="Sending information automaticaly about unpaid bills due on the current day by email",
        schedule_interval="58 7 * * *",
        start_date=pendulum.datetime(year=2023, month=9, day=27, tz="America/Sao_Paulo"),
        catchup=False,
        max_active_runs=1,
        tags=["Python", "SQL", "Automation", "Osiris de Castro"],
) as dag:

    DW_SQL_CONN = {'DBIP':'0.0.0.0',
            'DBLOGIN' : 'login',
            'DBPASSWORD' : 'password'}

    def send_email(email_from, email_to, subject, text, html):

        try:

            msg = MIMEMultipart('multipart')
            msg['Subject'] = subject
            msg['From'] = email_from
            msg['To'] = email_to

            text = text
            html = html

            part1 = MIMEText(text, 'plain')
            part2 = MIMEText(html, 'html')

            msg.attach(part1)
            msg.attach(part2)

            context = ssl.create_default_context()
            with smtplib.SMTP("email-smtp.sa-east-1.amazonaws.com", 25) as server:
                server.starttls(context=context)
                server.ehlo()
                server.login('email_login', 'email_password')
                response = server.sendmail(email_from, email_to, msg.as_string())
                server.quit()
                if not response:
                    print("E-mail sent succesfully!")
                    return {"msg": "E-mail sent succesfully"}
                else:
                    return response
                
        except Exception as e:
            return e


    def query_sql_df(sql,db,connx=DW_SQL_CONN):
        try:
            conn = pymssql.connect(server=connx['DBIP'], user=connx['DBLOGIN'], password=connx['DBPASSWORD'] , database=db)
            df = pd.read_sql_query(sql, conn)
            return df
            conn.commit()
        except Exception as e: 
            print(e)
            conn.rollback()
            return  None
        finally:
            conn.close()  


    def notification_expired_installments():

        try:

            date = datetime.datetime.today()
            file_date, email_date, query_date = date.strftime("%d-%m-%Y"), date.strftime("%d/%m/%Y"), date.strftime("%Y-%m-%d")

            expired_installments_query = f'''
                                SELECT  Costumer_Name AS 'Account',
                                        a.Installment_ID AS 'Installment ID', 
                                        a.Value AS 'Value'
                                FROM [dbLoansSystem].[dbo].[tbLoan] a
                                INNER JOIN [dbLoansSystem].[dbo].[tbCostumers] c ON a.CostumerID = c.ID
                                WHERE 
                                    a.payment_date IS NULL 
                                    AND a.installment_expire_date = '{query_date}'
                                    AND a.loan_type = 'D' 
                                    AND CONCAT(b.Collection_Code, b.ChargeStatus) IN ('90Ok','90Ok')
            '''

            df = query_sql_df(expired_installments_query, db='dbLoansSystem')

            if not df.empty:

                xlsx_path = f"/opt/airflow/config/collection/Expired_unpaid_bill_{file_date}.xlsx"

                df.to_excel(xlsx_path, index=False)

                Emails = ['chose_email@whatever.com', 'and_send_it@company.com']

                for email in Emails:
                    send_email(
                        email_from = 'osirisdatantech@gmail.com',
                        email_to=email,
                        subject=f'Installments Expiring today, lets charge them! :) {email_date}',
                        text=f'In the atachments bellow, you have the installments and the responsible costumers {email_date}',
                        html=f'',
                        p_password = 'password',
                        p_filename=xlsx_path
                    )

                print('Emails sent succesfully')

                os.remove(xlsx_path)

        except Exception as e:
            print(e)


    start = EmptyOperator(task_id="start")
    send_emails = PythonOperator(task_id="main", python_callable=notification_expired_installments)
    end = EmptyOperator(task_id="end")

    start >> send_emails >> end