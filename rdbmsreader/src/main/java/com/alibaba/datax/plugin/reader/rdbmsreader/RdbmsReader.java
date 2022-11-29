package com.alibaba.datax.plugin.reader.rdbmsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

import java.util.List;

public class RdbmsReader extends Reader {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.RDBMS;

    static {
        DBUtil.loadDriverClass("reader", "rdbms");
    }

    public static class Job extends Reader.Job {
        private Configuration pluginJobReaderWriterParamConfCopy;

        private CommonRdbmsReader.Job commonRdbmsReaderMaster;

        @Override
        public void init() {
            pluginJobReaderWriterParamConfCopy = getPluginJobReaderWriterParamConf();

            int fetchSize = pluginJobReaderWriterParamConfCopy.getInt(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE, Constant.DEFAULT_FETCH_SIZE);

            if (fetchSize < 1) {
                throw DataXException.build(DBUtilErrorCode.REQUIRED_VALUE,
                        String.format("您配置的fetchSize:[%d]不对,不能小于 1.", fetchSize));
            }

            pluginJobReaderWriterParamConfCopy.set(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE, fetchSize);

            commonRdbmsReaderMaster = new CommonRdbmsReader.Job(DATABASE_TYPE);
            commonRdbmsReaderMaster.init(pluginJobReaderWriterParamConfCopy);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return commonRdbmsReaderMaster.split(pluginJobReaderWriterParamConfCopy, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderMaster.post(pluginJobReaderWriterParamConfCopy);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderMaster.destroy(pluginJobReaderWriterParamConfCopy);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration pluginJobReaderParamConf;
        private CommonRdbmsReader.Task commonRdbmsReaderSlave;

        @Override
        public void init() {
            pluginJobReaderParamConf = super.getPluginJobReaderWriterParamConf();
            commonRdbmsReaderSlave = new CommonRdbmsReader.Task(DATABASE_TYPE);
            commonRdbmsReaderSlave.init(pluginJobReaderParamConf);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = pluginJobReaderParamConf.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);
            commonRdbmsReaderSlave.startRead(pluginJobReaderParamConf, recordSender, super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderSlave.post(this.pluginJobReaderParamConf);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderSlave.destroy(this.pluginJobReaderParamConf);
        }
    }
}
