CREATE OR REPLACE PACKAGE BODY pkg_debug IS
    /*
    create table ETL_LOG
    (
      id             NUMBER,
      module         VARCHAR2(4000),
      start_desc     VARCHAR2(4000),
      time_start     TIMESTAMP(6),
      time_end       TIMESTAMP(6),
      err_code       NUMBER,
      err_msg        VARCHAR2(4000),
      err_desc       VARCHAR2(4000),
      err_stack      VARCHAR2(4000),
      err_backtrace  VARCHAR2(4000),
      err_call_stack VARCHAR2(4000),
      intrvl         as (TO_CHAR(EXTRACT(DAY FROM NVL(("TIME_END"-"TIME_START")DAY(9) TO SECOND(6),INTERVAL'+000000000 00:00:00.000000000' DAY(9) TO SECOND(9))))||'天'||TO_CHAR(EXTRACT(HOUR FROM NVL(("TIME_END"-"TIME_START")DAY(9) TO SECOND(6),INTERVAL'+000000000 00:00:00.000000000' DAY(9) TO SECOND(9))))||'小时'||TO_CHAR(EXTRACT(MINUTE FROM NVL(("TIME_END"-"TIME_START")DAY(9) TO SECOND(6),INTERVAL'+000000000 00:00:00.000000000' DAY(9) TO SECOND(9))))||'分'||TO_CHAR(ROUND(EXTRACT(SECOND FROM NVL(("TIME_END"-"TIME_START")DAY(9) TO SECOND(6),INTERVAL'+000000000 00:00:00.000000000' DAY(9) TO SECOND(9)))))||'秒')
    )
    */
    --私有变量，保存当前的默认存储DEBUG和错误信息的日志表名
    g_v_log_table  VARCHAR2(30) := 'ETL_LOG';
    g_b_show_debug BOOLEAN := TRUE;

    --设置是否显示调试信息
    PROCEDURE set_show_debug(in_b_show_debug BOOLEAN) IS
    BEGIN
        g_b_show_debug := in_b_show_debug;
    END set_show_debug;

    --获取日志表名
    FUNCTION get_log_table RETURN VARCHAR2 IS
    BEGIN
        RETURN g_v_log_table;
    END get_log_table;

    --记录起始信息
    FUNCTION log_start(in_v_module IN VARCHAR2, in_v_start_desc IN VARCHAR2 DEFAULT NULL)
        RETURN NUMBER IS
        l_return_val NUMBER;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT nvl(MAX(id),0) FROM ' ||
                          pkg_debug.get_log_table
            INTO l_return_val;
        EXECUTE IMMEDIATE 'INSERT INTO ' || pkg_debug.get_log_table ||
                          ' (id,module,time_start,start_desc) values (:1,:2,systimestamp,:3)'
            USING l_return_val + 1, in_v_module, in_v_start_desc;
        COMMIT;
        RETURN l_return_val + 1;
    END log_start;

    --记录结束信息
    PROCEDURE log_end(in_n_id IN NUMBER, in_n_err_code IN NUMBER DEFAULT SQLCODE, in_v_err_msg IN VARCHAR2 DEFAULT SQLERRM, in_v_err_desc IN VARCHAR2 DEFAULT NULL, in_v_err_stack IN VARCHAR2 DEFAULT dbms_utility.format_error_stack, in_v_error_backtrace IN VARCHAR2 DEFAULT dbms_utility.format_error_backtrace, in_v_error_callstack IN VARCHAR2 DEFAULT dbms_utility.format_call_stack) IS
    BEGIN
        EXECUTE IMMEDIATE 'UPDATE ' || pkg_debug.get_log_table ||
                          ' SET TIME_END=systimestamp,ERR_CODE=:1,ERR_MSG=:2,ERR_DESC=:3,err_stack=:4,err_backtrace=:5,err_call_stack=:6 WHERE ID=:7'
            USING substr(in_n_err_code, 0, 4000), substr(in_v_err_msg, 0, 4000), substr(in_v_err_desc, 0, 4000), substr(in_v_err_stack, 0, 4000), substr(in_v_error_backtrace, 0, 4000), substr(in_v_error_callstack, 0, 4000), in_n_id;
        COMMIT;
    END log_end;
    
    --清空表
    PROCEDURE TRUNC_TAB(i_v_tableName IN VARCHAR2) IS
      v_tableName varchar2(100) := upper(TRIM(i_v_tableName));
    BEGIN
      EXECUTE IMMEDIATE 'TRUNCATE TABLE '|| v_tableName; 
    END;
    
    --删除分区
    --仅适用于按月分区，且分区列为时间类型
    PROCEDURE TRUNC_TARTITION(i_v_tableName IN VARCHAR2
                             ,i_v_date_begin IN VARCHAR2
                             ,i_v_date_end IN VARCHAR2 default '29990101') IS
      v_tableName varchar2(100) := upper(TRIM(i_v_tableName));
      v_partition_key varchar2(100) := '';
      v_date_begin varchar2(8) := to_char(add_months(to_date(i_v_date_begin,'yyyymmdd'),1),'yyyymmdd');
      v_date_end varchar2(8) := to_char(add_months(to_date(i_v_date_end,'yyyymmdd'),1),'yyyymmdd');
    BEGIN
      FOR I IN (SELECT partition_name,high_value FROM user_tab_partitions WHERE table_name = v_tableName) 
        LOOP
          EXECUTE IMMEDIATE 'SELECT TO_CHAR(' || I.HIGH_VALUE || ',''YYYYMMDD'') FROM DUAL' INTO v_partition_key;
          DBMS_OUTPUT.put_line(v_partition_key);
          IF v_partition_key >= v_date_begin and v_partition_key < v_date_end THEN
            BEGIN
            DBMS_OUTPUT.put_line('ALTER TABLE DROP PARTITION ' || I.PARTITION_NAME);
            EXECUTE IMMEDIATE 'ALTER TABLE DROP PARTITION ' || I.PARTITION_NAME ;
            EXCEPTION
                    WHEN OTHERS THEN
                      NULL;
            END;
          END IF;
        END LOOP;
    END TRUNC_TARTITION;
    
    
    --输出调试信息
    PROCEDURE print(in_v_msg IN VARCHAR2) IS
    BEGIN
        IF g_b_show_debug = TRUE
        THEN
            FOR output_cnt IN 1 .. ceil(length(in_v_msg) /
                                        gc_v_output_len_per_line)
            LOOP
                dbms_output.put_line(substr(in_v_msg
                                           ,(output_cnt - 1) *
                                            gc_v_output_len_per_line + 1
                                           ,gc_v_output_len_per_line));
            END LOOP;
        ELSE
            NULL;
        END IF;
    END print;

    PROCEDURE list_long_ops IS
    BEGIN
        EXECUTE IMMEDIATE 'SELECT SID,serial#,username,opname,target,sofar,totalwork,ROUND(sofar/totalwork,2)*100||'' % '' progress,units,start_time,last_update_time,time_remaining,elapsed_seconds,CONTEXT,message,sql_address,sql_hash_value,qcsid,target_desc  FROM v$session_longops WHERE sofar<>totalwork';
    END list_long_ops;
    
    --每月一号执行
    FUNCTION EXE_WINDOWS RETURN BOOLEAN IS
    BEGIN
      IF EXTRACT(DAY FROM SYSDATE) = 1 THEN
        RETURN TRUE;
      ELSE 
        RETURN FALSE;
      END IF;
    END;

END pkg_debug;
