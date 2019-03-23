# coding=utf-8

import xml.etree.ElementTree as ET
import cx_Oracle
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import scheduler_util

#查询etl执行情况
v_sql_error = """
SELECT  T1.ETL_FREQUENCY
        ,T1.ETL_TIME
        ,WAITFOR_EXEC_NUM
        ,NVL(EXEC_SUCCESS_NUM,0) EXEC_SUCCESS_NUM
        ,NVL(EXEC_WAIT_NUM,0) EXEC_WAIT_NUM
        ,NVL(EXEC_ERROR_NUM,0) EXEC_ERROR_NUM
        ,CASE WHEN NVL(EXEC_SUCCESS_NUM,0) + NVL(EXEC_WAIT_NUM,0) + NVL(EXEC_ERROR_NUM,0) = 0 THEN '调度未执行'
              WHEN NVL(EXEC_ERROR_NUM,0) > 0 THEN EXEC_ERROR_NUM || ' 个执行报错'
              WHEN NVL(EXEC_WAIT_NUM,0) > 0 THEN EXEC_WAIT_NUM || ' 个执行等待'
              WHEN WAITFOR_EXEC_NUM = EXEC_SUCCESS_NUM THEN '执行完成'
              ELSE '未知异常'
              END EXEC_DESC
FROM  (SELECT ETL_FREQUENCY
             ,CASE WHEN ETL_FREQUENCY = 'H' THEN TO_CHAR(TRUNC(SYSDATE,'HH')-1/24,'YYYYMMDDHH24')
                   WHEN ETL_FREQUENCY = 'D' THEN TO_CHAR(SYSDATE-1,'YYYYMMDD')
                   WHEN ETL_FREQUENCY = 'M' THEN TO_CHAR(TRUNC(SYSDATE,'MM')-1,'YYYYMM')
                   WHEN ETL_FREQUENCY = 'Y' THEN TO_CHAR(TRUNC(SYSDATE,'YY')-1,'YYYY')
                   ELSE NULL
                   END ETL_TIME
             ,COUNT(*) WAITFOR_EXEC_NUM
      FROM ETL_TASK 
      WHERE VALID_FLAG = 1 
      GROUP BY ETL_FREQUENCY
              ,CASE WHEN ETL_FREQUENCY = 'H' THEN TO_CHAR(TRUNC(SYSDATE,'HH')-1/24,'YYYYMMDDHH24')
                    WHEN ETL_FREQUENCY = 'D' THEN TO_CHAR(SYSDATE-1,'YYYYMMDD')
                    WHEN ETL_FREQUENCY = 'M' THEN TO_CHAR(TRUNC(SYSDATE,'MM')-1,'YYYYMM')
                    WHEN ETL_FREQUENCY = 'Y' THEN TO_CHAR(TRUNC(SYSDATE,'YY')-1,'YYYY')
                    ELSE NULL
                    END
      ) T1
LEFT JOIN (SELECT ETL_FREQUENCY
                 ,ETL_TIME
                 ,SUM(CASE WHEN ETL_STATUS = 1 THEN 1 ELSE 0 END) EXEC_SUCCESS_NUM
                 ,SUM(CASE WHEN ETL_STATUS = 0 THEN 1 ELSE 0 END) EXEC_WAIT_NUM
                 ,SUM(CASE WHEN ETL_STATUS = 4 THEN 1 ELSE 0 END) EXEC_ERROR_NUM
          FROM ETL_LOG_ALL 
          WHERE (ETL_FREQUENCY = 'H' AND ETL_TIME = TO_CHAR(TRUNC(SYSDATE,'HH')-1/24,'YYYYMMDDHH24'))
                OR (ETL_FREQUENCY = 'D' AND ETL_TIME = TO_CHAR(SYSDATE-1,'YYYYMMDD'))
                OR (ETL_FREQUENCY = 'M' AND ETL_TIME = TO_CHAR(TRUNC(SYSDATE,'MM')-1,'YYYYMM'))
                OR (ETL_FREQUENCY = 'Y' AND ETL_TIME = TO_CHAR(TRUNC(SYSDATE,'YY')-1,'YYYY'))
          GROUP BY ETL_FREQUENCY,ETL_TIME
          ) T2
  ON T1.ETL_FREQUENCY = T2.ETL_FREQUENCY AND T1.ETL_TIME = T2.ETL_TIME"""

v_sql_error1 = 'SELECT COUNT(*) FROM (' + v_sql_error + ' ) xx WHERE EXEC_DESC != \'执行完成\' '


#解析xml
map_conn = {}
map_mail_from = {}
map_mail_to = {}
map_conn = scheduler_util.xml2map('conn','conn_dw',map_conn)
map_mail_from = scheduler_util.xml2map('mail','from',map_mail_from)
map_mail_to = scheduler_util.xml2map('mail','to',map_mail_to)

#查看执行是否有异常，没有异常则结束，有异常则发送邮件
query_result1 = scheduler_util.oracle_select(map_conn,v_sql_error1)
error_num = query_result1[0][0]
#error_num = 0 表示对应周期内所有跑批都正常完成
if error_num == 0:
    pass
else:
    #将etl执行情况写入到map
    query_result2 = scheduler_util.oracle_select(map_conn,v_sql_error)
    #print(query_result2)
    #生成html
    html_msg1 = """
    <table align="center">
        <tr>
            <th>频率</th>
            <th>周期</th>
            <th>所有任务数量</th>
            <th>成功执行数量</th>
            <th>被阻塞数量</th>
            <th>报错数量</th>
            <th>描述</th>
        </tr>
    """
    html_msg2 = ""
    for error_msg in query_result2:
        #print(error_msg)
        etl_frequency = error_msg[0]
        etl_time = error_msg[1]
        waitfor_exec_num = error_msg[2]
        exec_success_num = error_msg[3]
        exec_wait_num = error_msg[4]
        exec_error_num = error_msg[5]
        exec_desc = error_msg[6]
        html_msg2 += """
        <tr>
            <td align="center">{}</td>
            <td align="center">{}</td>
            <td align="center">{}</td>
            <td align="center">{}</td>
            <td align="center">{}</td>
            <td align="center">{}</td>
            <td align="center">{}</td>
        </tr>\r""".format(etl_frequency,etl_time,waitfor_exec_num,exec_success_num,exec_wait_num,exec_error_num,exec_desc)
    html_msg3 = """    </table>"""
    html_msg = html_msg1 + html_msg2 + html_msg3
    #print (html_msg)

scheduler_util.send_mail(map_mail_from,map_mail_to,html_msg)