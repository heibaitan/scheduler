CREATE OR REPLACE PACKAGE pkg_debug IS
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
    --获取日志表名
    FUNCTION get_log_table RETURN VARCHAR2;

    --设置是否显示调试信息
    PROCEDURE set_show_debug(in_b_show_debug BOOLEAN);

    --记录起始信息
    FUNCTION log_start(in_v_module IN VARCHAR2, in_v_start_desc IN VARCHAR2 DEFAULT NULL)
        RETURN NUMBER;

    --记录结束信息
    PROCEDURE log_end(in_n_id IN NUMBER, in_n_err_code IN NUMBER DEFAULT SQLCODE, in_v_err_msg IN VARCHAR2 DEFAULT SQLERRM, in_v_err_desc IN VARCHAR2 DEFAULT NULL, in_v_err_stack IN VARCHAR2 DEFAULT dbms_utility.format_error_stack, in_v_error_backtrace IN VARCHAR2 DEFAULT dbms_utility.format_error_backtrace, in_v_error_callstack IN VARCHAR2 DEFAULT dbms_utility.format_call_stack);

    --清空表
    PROCEDURE TRUNC_TAB(i_v_tableName IN VARCHAR2);

    --删除分区
    --仅适用于按月分区，且分区列为时间类型
    PROCEDURE TRUNC_TARTITION(i_v_tableName IN VARCHAR2
                             ,i_v_date_begin IN VARCHAR2
                             ,i_v_date_end IN VARCHAR2 default '29990101');
                             
    --输出调试信息
    PROCEDURE print(in_v_msg IN VARCHAR2);
    PROCEDURE list_long_ops;
    
    --每月一号执行
    FUNCTION EXE_WINDOWS RETURN BOOLEAN;
    
    gc_v_output_len_per_line CONSTANT PLS_INTEGER := 255;
END pkg_debug;
