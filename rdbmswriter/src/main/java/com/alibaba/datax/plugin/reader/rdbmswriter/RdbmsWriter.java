package com.alibaba.datax.plugin.reader.rdbmswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.ibm.db2.jcc.t4.e;
import org.apache.commons.collections4.CollectionUtils;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RdbmsWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.RDBMS;

    static {
        //加载插件下面配置的驱动类
        //  DBUtil.loadDriverClass("writer", "rdbms");
    }

    public static class Job extends Writer.Job {
        private Configuration originalConfig;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init() {
            originalConfig = getPluginJobReaderWriterParamConf();

            // warn：not like mysql, only support insert mode, don't use
            String writeMode = originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                throw DataXException.build(DBUtilErrorCode.CONF_ERROR,
                        String.format("写入模式(writeMode)配置有误. 因为不支持配置参数项 writeMode: %s, 仅能使用insert", writeMode));
            }

            commonRdbmsWriterMaster = new SubCommonRdbmsWriter.Job(DATABASE_TYPE);
            commonRdbmsWriterMaster.init(originalConfig);
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
            this.writerSliceConfig = super.getPluginJobReaderWriterParamConf();
            this.commonRdbmsWriterSlave = new SubCommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver) {
            commonRdbmsWriterSlave.startWrite(recordReceiver, writerSliceConfig, super.getTaskPluginCollector());
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

        //System.out.println(new String("浣滀笟閮ㄤ綅".getBytes("gbk"), "utf-8"));

        System.out.println(getEncoding("浣滀笟閮ㄤ綅"));

      /*  Class.forName("com.mysql.cj.jdbc.Driver"); // com.ibm.db2.jcc.DB2Driver

        // DriverManager.getConnection("jdbc:db2://10.88.36.79:50000/testdb:currentSchema=T1;", "DB2INST1", "123456");

        for (int d = 0; d < 70; d++) {
            int d0 = d;
            new Thread(() -> {
                try {
                    Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test", "root", "xuelang@1");

                    PreparedStatement preparedStatement = connection.prepareStatement("insert into order_table (ORDER_NO,STORE_ID,USER_ID,ORDER_STATE,GOOD_MONEY,DELIVER_TYPE,DELIVER_MONEY)values (?,?,?,?,?,?,?)");

                    for (int a = 0; a < 10000; a++) {
                        preparedStatement.setString(1, "a");
                        preparedStatement.setLong(2, new Random().nextInt(10000));
                        preparedStatement.setLong(3, new Random().nextInt(10000));
                        preparedStatement.setShort(4, (short) new Random().nextInt(10000));
                        preparedStatement.setBigDecimal(5, new BigDecimal(new Random().nextInt(10000)));
                        preparedStatement.setShort(6, (short) 70);
                        preparedStatement.setBigDecimal(7, new BigDecimal(new Random().nextInt(10000)));

                        preparedStatement.addBatch();
                    }
                    preparedStatement.executeBatch();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(d0);
            }).start();
        }*/

    }

    public static String getEncoding(String str) {
        List<String> matchEncodingList = new ArrayList<>();

        for (String encoding : Arrays.asList("ISO-8859-1", "GBK")) {

            Charset cs;
            try {
                cs = Charset.forName(encoding);
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            if (cs.canEncode() && cs.newEncoder().canEncode(str)) {
                matchEncodingList.add(encoding);
            }
/*
            try {
                if (str.equals(new String(str.getBytes(encoding), encoding))) {
                    matchEncodingList.add(encoding);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }*/
        }

        if (CollectionUtils.isNotEmpty(matchEncodingList)) {
            return matchEncodingList.get(0);
        }

        return "UTF-8";

    }


    public static String getRandomString2(int length) {
        Random random = new Random(System.currentTimeMillis());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(3);
            long result;
            switch (number) {
                case 0:
                    result = Math.round(Math.random() * 25 + 65);
                    sb.append((char) result);
                    break;
                case 1:
                    result = Math.round(Math.random() * 25 + 97);
                    sb.append((char) result);
                    break;
                case 2:
                    sb.append(new Random().nextInt(10));
                    break;
            }


        }
        return sb.toString();
    }


}