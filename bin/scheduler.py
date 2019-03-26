# coding=utf-8

import scheduler_util
import logging
import datetime
import os
import sys
import queue
import threading
import subprocess

queue_task = queue.Queue(0)

today = datetime.datetime.today()
scheduler_date = today.strftime('%Y%m%d')

scheduler_path = os.path.dirname(os.path.split(os.path.realpath(__file__))[0])
scheduler_log_path = os.path.join(scheduler_path,'logs','scheduler_' + scheduler_date + '.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=scheduler_log_path,
                    filemode='a')

#外部传入两个参数：参数分别是 etl频率 并发数
etl_frequency = sys.argv[1]
etl_parallel = int(sys.argv[2])
'''
etl_frequency = 'D'
etl_parallel = 4
'''
if etl_frequency == 'D':
    etl_time = int((today + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
elif etl_frequency == 'M':
    etl_time = int((today.replace(day=1) - datetime.timedelta(days=1)).strftime('%Y%m'))
elif etl_frequency == 'Y':
    etl_time = today.year - 1
elif etl_frequency == 'H':
    etl_time = int((today + datetime.timedelta(minutes=-60)).strftime('%Y%m%d%H'))


#获取数仓所在数据库链接信息
map_conn = {}
map_conn = scheduler_util.xml2map('conn','conn_dw',map_conn)
#将待执行的任务写入队列
v_sql_tasklist = "SELECT TASK_TYPE || '_' || TASK_NAME FROM ETL_TASK WHERE VALID_FLAG = 1 AND ETL_FREQUENCY = '" + etl_frequency + "'"
query_result = scheduler_util.oracle_select(map_conn,v_sql_tasklist)
for row in query_result:
    queue_task.put(row[0])

map_error = {}

str_sql_insert = "INSERT INTO ETL_LOG_PDI(TASK_ID,ETL_FREQUENCY,ETL_STATUS,ETL_TIME,BEGIN_TIME,END_TIME,LOG) \
                    VALUES (:TASK_ID,:ETL_FREQUENCY,:ETL_STATUS,:ETL_TIME,:BEGIN_TIME,:END_TIME,:LOG)"

def task_run(thread_name):
    while not queue_task.empty():
        str_tast = queue_task.get()
        task_type = str_tast.split('_')[0]
        task_id = str_tast[str_tast.index('_')+1:]
        logging.info("%s %s %s start execute", etl_time, task_id, threading.currentThread().getName() )
        begin_time = datetime.datetime.today()
        str_begin_time = begin_time.strftime('%Y-%m-%d %H:%M:%S')

        if task_type == 'PDI':
            ''' for windows
            run_shell = 'D:/kettle8.2/kitchen.bat -rep scheduler -dir /BIN -job SCHEDULER \
                -level Basic "-param:ETL_FREQUENCY=' + etl_frequency + '" "-param:ETL_TIME=' + str(etl_time) + '" "-param:TASK_ID=' + task_id +'"'  
            '''
            run_shell = '/usr/local/kettle7.1/kitchen.sh -rep REPOS_PDI -dir /BIN -job SCHEDULER \
                -level Basic -param:ETL_FREQUENCY=' + etl_frequency + ' -param:ETL_TIME=' + str(etl_time) + ' -param:TASK_ID=' + task_id
            run_status=subprocess.getstatusoutput(run_shell)
            end_time = datetime.datetime.today()
            etl_status,run_log = run_status
            try:
                run_log_short = run_log[run_log.index('SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]')+69:]
            except Exception as e:
                run_log_short =run_log
            if etl_status == 0:
                etl_status = '1'
            else:
                etl_status = '2'
            #print(etl_status)
            #将PDI日志写入到日志表
            list_param = [task_id,etl_frequency,etl_status,etl_time,begin_time,end_time,run_log_short]
            query_result = scheduler_util.oracle_insert(map_conn,str_sql_insert,list_param)

        if task_type == 'PLSQL':
            list_parm = [etl_time+1]
            etl_status = scheduler_util.oracle_callproc(map_conn,task_id,list_parm)
            if etl_status == '2':
                etl_status = '1'
            elif etl_status == '1':
                etl_status = '0'
            else:
                etl_status == '2'
            #print(etl_status)

        end_time = datetime.datetime.today()
        if etl_status == '1':
            logging.info("%s %s %s %s executed successfully [start at %s, costs %s minutes]", etl_time, task_type, task_id, threading.currentThread().getName(), str_begin_time, round((end_time - begin_time).seconds/60,2) )
        elif etl_status == '0':
            queue_task.put(str_tast)
            logging.info("%s %s %s executed blocked", etl_time, task_id, threading.currentThread().getName() )
        else:
            if task_id not in map_error.keys():
                map_error[task_id] = 1
                queue_task.put(str_tast)
                logging.warning("%s %s %s %s executed failed  time-1 [start at %s, costs %s minutes]", etl_time, task_type, task_id, threading.currentThread().getName(), str_begin_time, round((end_time - begin_time).seconds/60,2) )
            elif map_error[task_id] < 2:
                map_error[task_id] += 1
                queue_task.put(str_tast)
                logging.warning("%s %s %s executed failed  time-%s [start at %s, costs %s minutes]", etl_time, task_type, task_id, threading.currentThread().getName(), map_error[task_id], str_begin_time, round((end_time - begin_time).seconds/60,2) )
            else:
                map_error[task_id] += 1
                logging.error("%s %s %s executed failed  time-%s [start at %s, costs %s minutes]", etl_time, task_type, task_id, threading.currentThread().getName(), map_error[task_id], str_begin_time, round((end_time - begin_time).seconds/60,2) )

list_thread = []
for i in range(etl_parallel):
    t =threading.Thread(target=task_run,args=(i,))
    list_thread.append(t)

for i in range(len(list_thread)):
    list_thread[i].start()

for i in range(len(list_thread)):
    list_thread[i].join()

logging.info("%s all task executed done", etl_time)