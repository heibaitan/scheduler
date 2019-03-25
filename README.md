**scheduler** is a parallel scheduling tool by python3  
it can only support pdi and plsql now，Maybe it will support more tools in the future


### table description

- ETL\_LOG\_PDI:PDI日志
```sql(LOG_ID,TASK_ID,ETL_FREQUENCY,ETL_STATUS,ETL_TIME,BEGIN_TIME,END_TIME,LOG)
```
- ETL\_LOG\_DB:过程日志
```sql(LOG_ID,TASK_ID,ETL_FREQUENCY,ETL_STATUS,ETL_TIME,BEGIN_TIME,END_TIME,LOG_SQLERRM,LOG_ERROR_STACK,LOG_BACKTRACE)
```
- ETL\_LOG\_ALL:日志汇总，通过这个表查看当期是否执行成功，只保留当期最后一条日志
```sql(TASK_ID,TASK_TYPE,ETL_FREQUENCY,ETL_STATUS,ETL_TIME,BEGIN_TIME,END_TIME)
```
- ETL_TASK:任务列表
```sql
(TASK_ID,TASK_NAME,TASK_TYPE,ETL_FREQUENCY,PARENT_TASK_LIST,VALID_FLAG)
```

### column description
- 执行状态(ETL_STATUS)：成功1，等待0，报错2;  
- 执行频率(ETL_FREQUENCY)：年Y、月M、周、天D、七天7D、时H;  
- 任务类型(TASK_TYPE)：PDI抽取任务PDI、PLSQL任务PLSQL;  
- 有效标志(VALID_FLAG)：无效0、有效1;  


### note
@author         kywenlee@gmail.com  
@begin at       2019-03-23  
@description    使用python3开发，用于传统数仓(PDI加上oracle)的并发调度工具  
@note           2019-03-23  构建整体架构