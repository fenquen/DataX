/home/a/program/azul/bin/java -server \
-Xms1g -Xmx1g \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=/home/a/github/DataX/target/datax/datax/log \
-Dloglevel=info \
-Dfile.encoding=UTF-8 \
-Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener \
-Djava.security.egd=file:///dev/urandom \
-Ddatax.home=/home/a/github/DataX/target/datax/datax \
-Dlogback.configurationFile=/home/a/github/DataX/target/datax/datax/conf/logback.xml \
-classpath /home/a/github/DataX/target/datax/datax/lib/*:. \
-Dlog.file.name=$2 \
com.alibaba.datax.core.Engine  -job $1 -jobid $2 -mode $3