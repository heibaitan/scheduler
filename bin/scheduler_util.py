# coding=utf-8

import xml.etree.ElementTree as ET
import cx_Oracle
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import datetime

#解析xml，并将结果集写入到map
scheduler_path = os.path.dirname(os.path.split(os.path.realpath(__file__))[0])
conf_xml_path = os.path.join(scheduler_path,'conf','config.xml')
def xml2map(str_xml_tag,str_xml_attr,var_map):
    tree = ET.parse(conf_xml_path)
    root = tree.getroot()
    for child in root:
        if child.tag == str_xml_tag and child.attrib['name'] == str_xml_attr:
            for children in child:
                var_map[children.tag] = children.text
    return var_map


#连接oracle数据库，执行query并返回结果集
def oracle_select(str_conn,str_sql):
    v_host = str_conn['host']
    v_port = str_conn['port']
    v_database = str_conn['database']
    v_user = str_conn['user']
    v_password = str_conn['password']
    try:
        db = cx_Oracle.connect(v_user + '/' + v_password + '@' + v_host + ':' + v_port + '/' + v_database)
        cursor = db.cursor()
        cursor.execute(str_sql)
        data = cursor.fetchall()
        result = data
        db.close()
        return result
    except Exception as e:
        print('数据库查询失败')
		
#连接oracle数据库，执行insert
def oracle_insert(str_conn,str_sql,list_param):
    v_host = str_conn['host']
    v_port = str_conn['port']
    v_database = str_conn['database']
    v_user = str_conn['user']
    v_password = str_conn['password']
    try:
        db = cx_Oracle.connect(v_user + '/' + v_password + '@' + v_host + ':' + v_port + '/' + v_database)
        cursor = db.cursor()
        cursor.execute(str_sql,list_param)
        db.commit()
    except Exception as e:
        print('%s' % str(e))
        print('数据库写入失败')

#连接oracle数据库，执行存储过程并返回变量
def oracle_callproc(str_conn,str_procname,str_proc_varlist):
    v_host = str_conn['host']
    v_port = str_conn['port']
    v_database = str_conn['database']
    v_user = str_conn['user']
    v_password = str_conn['password']
    try:
        db = cx_Oracle.connect(v_user + '/' + v_password + '@' + v_host + ':' + v_port + '/' + v_database)
        cursor = db.cursor()
        str_proc_out = cursor.var(cx_Oracle.STRING)
        str_proc_varlist.append(str_proc_out)
        callproc = cursor.callproc(str_procname,str_proc_varlist)
        result = str_proc_out.getvalue()
        db.close()
        return result
    except Exception as e:
        print('数据库调用过程失败')

#发送邮件
def send_mail(map_sender,map_receiver,str_mail_msg):
    smtp_server = map_sender['server']
    smtp_port = map_sender['port']
    smtp_user = map_sender['user']
    smtp_password = map_sender['password']
    mail_receiver = map_receiver['user_list']

    mail = MIMEText(str_mail_msg,'html','utf-8')
    mail['Subject'] = 'etl异常预警'
    mail['From'] = smtp_user  #发件人
    mail['To'] = mail_receiver  #收件人
    try:
        smtp = smtplib.SMTP() 
        smtp.connect(smtp_server,smtp_port)
        print('邮箱服务器连接成功')
        smtp.login(smtp_user, smtp_password)
        print('邮箱服务器登录成功')
        smtp.sendmail(smtp_user,mail_receiver.split(','),mail.as_string())
        print("邮件发送成功")
        smtp.quit()
    except smtplib.SMTPException as e:
        print("邮件发送失败",e)
		
'''
map_conn = {}
map_conn = xml2map('conn','conn_dw',map_conn)
#将待执行的任务写入队列
v_sql_tasklist = "SELECT 1 FROM DUAL"
query_result = oracle_select(map_conn,v_sql_tasklist)
print (query_result)
'''
