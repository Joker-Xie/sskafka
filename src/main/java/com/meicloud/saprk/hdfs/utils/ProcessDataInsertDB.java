package com.meicloud.saprk.hdfs.utils;


import com.meicloud.spark.entity.CaseVo;
import com.meicloud.spark.utils.MySQLPoolManager;
import com.meicloud.spark.utils.StringUtils;
import scala.collection.mutable.HashMap;

import java.sql.*;
import java.util.TimerTask;

public class ProcessDataInsertDB extends TimerTask {
    private CaseVo.ProcessStateVo processStateVo;
    private HashMap<String, CaseVo.OutputSourceConfigVo> outputSourceConfigVoHashMap;
    private CaseVo.JobConfigVo jobConfigVo;

    public ProcessDataInsertDB(CaseVo.ProcessStateVo processStateVo, CaseVo.JobConfigVo jobConfigVo, HashMap<String, CaseVo.OutputSourceConfigVo> outputSourceConfigVoHashMap) {
        this.processStateVo = processStateVo;
        this.outputSourceConfigVoHashMap = outputSourceConfigVoHashMap;
        this.jobConfigVo = jobConfigVo;
    }

    @Override
    public void run() {
        long errorNum = processStateVo.errorNum().sum();
        long finishNum = processStateVo.finishNum().sum();
        long totalNum = errorNum + finishNum;
        CaseVo.OutputJdbcConfigVo outputJdbcConfigVo = (CaseVo.OutputJdbcConfigVo)outputSourceConfigVoHashMap.get("jdbc").get();
        String job_name = jobConfigVo.jobName() + "_" + StringUtils.getTimeNum(jobConfigVo.updateTime());
        Connection conn = MySQLPoolManager.getMysqlManager(outputJdbcConfigVo).getConnection();
        try {
            conn.setAutoCommit(false);
            PreparedStatement preparedStatement = conn.prepareStatement("REPLACE INTO TEST.T_SS_JOB_RUNING_STATUS VALUES(?,?,?,?) ");
            preparedStatement.setString(1,job_name);
            preparedStatement.setLong(2,totalNum);
            preparedStatement.setLong(3,finishNum);
            preparedStatement.setLong(4,errorNum);
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
            conn.commit();
            conn.close();
//            System.out.println("数据已更新!"+"errorNum: "+errorNum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
