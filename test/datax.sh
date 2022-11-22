nohup java -server \
-Xms1g -Xmx1g \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=/root/datax/log \
-Dloglevel=info \
-Dfile.encoding=UTF-8 \
-Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener \
-Djava.security.egd=file:///dev/urandom \
-Ddatax.home=/root/datax \
-Dlogback.configurationFile=/root/datax/conf/logback.xml \
-classpath /root/datax/lib/*:. \
-Dlog.file.name=datax_bin_db2_pg_json \
com.alibaba.datax.core.Engine -mode local -jobid 70 -job /root/datax/bin/db2_pg.json > db2_pg.log 2>&1 &