package com.alibaba.datax.plugin.reader.rdbmswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

public class RdbmsWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.RDBMS;

    static {
        //加载插件下面配置的驱动类
        //  DBUtil.loadDriverClass("writer", "rdbms");
    }

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            // warn：not like mysql, only support insert mode, don't use
            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                throw DataXException
                        .build(
                                DBUtilErrorCode.CONF_ERROR,
                                String.format(
                                        "写入模式(writeMode)配置有误. 因为不支持配置参数项 writeMode: %s, 仅使用insert sql 插入数据. 请检查您的配置并作出修改.",
                                        writeMode));
            }

            this.commonRdbmsWriterMaster = new SubCommonRdbmsWriter.Job(
                    DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return commonRdbmsWriterMaster.split(originalConfig, mandatoryNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterSlave = new SubCommonRdbmsWriter.Task(
                    DATABASE_TYPE);
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterSlave.startWrite(recordReceiver,
                    writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }

    }

    public static void main(String[] args) throws Exception {
        Class.forName("com.ibm.db2.jcc.DB2Driver");
        Connection connection = DriverManager.getConnection("jdbc:db2://10.88.36.79:50000/testdb:currentSchema=T1;", "DB2INST1", "123456");

        PreparedStatement preparedStatement = connection.prepareStatement("insert into order values (?,?,?,?,?,?,?,?)");

        for (int d = 0; d < 370; d++) {
            for (int a = 0; a < 10000; a++) {
                preparedStatement.setLong(1, 70);
                preparedStatement.setString(2, "70");
                preparedStatement.setLong(3, 70);
                preparedStatement.setLong(4, 70);
                preparedStatement.setShort(5, (short) 70);
                preparedStatement.setBigDecimal(6, new BigDecimal(70));
                preparedStatement.setShort(7, (short) 70);
                preparedStatement.setBigDecimal(8, new BigDecimal(70));

                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }

    }

}