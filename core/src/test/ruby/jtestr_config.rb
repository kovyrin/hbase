output_level :VERBOSE
log_level :DEBUG
output STDOUT


java.lang.Thread.current_thread.context_class_loader = JRuby.runtime.getJRubyClassLoader

%w[
/Users/scoundrel/.m2/repository/ant/ant/1.6.5/ant-1.6.5.jar
/Users/scoundrel/.m2/repository/asm/asm/3.0/asm-3.0.jar
/Users/scoundrel/.m2/repository/com/thoughtworks/paranamer/paranamer/1.5/paranamer-1.5.jar
/Users/scoundrel/.m2/repository/com/thoughtworks/paranamer/paranamer-ant/1.5/paranamer-ant-1.5.jar
/Users/scoundrel/.m2/repository/com/thoughtworks/paranamer/paranamer-generator/1.5/paranamer-generator-1.5.jar
/Users/scoundrel/.m2/repository/com/thoughtworks/qdox/qdox/1.9.1/qdox-1.9.1.jar
/Users/scoundrel/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar
/Users/scoundrel/.m2/repository/commons-codec/commons-codec/1.3/commons-codec-1.3.jar
/Users/scoundrel/.m2/repository/commons-el/commons-el/1.0/commons-el-1.0.jar
/Users/scoundrel/.m2/repository/commons-httpclient/commons-httpclient/3.0.1/commons-httpclient-3.0.1.jar
/Users/scoundrel/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar
/Users/scoundrel/.m2/repository/commons-logging/commons-logging/1.0.3/commons-logging-1.0.3.jar
/Users/scoundrel/.m2/repository/commons-net/commons-net/1.4.1/commons-net-1.4.1.jar
/Users/scoundrel/.m2/repository/hsqldb/hsqldb/1.8.0.10/hsqldb-1.8.0.10.jar
/Users/scoundrel/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar
/Users/scoundrel/.m2/repository/javax/mail/mail/1.4/mail-1.4.jar
/Users/scoundrel/.m2/repository/junit/junit/4.5/junit-4.5.jar
/Users/scoundrel/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar
/Users/scoundrel/.m2/repository/net/java/dev/jets3t/jets3t/0.7.1/jets3t-0.7.1.jar
/Users/scoundrel/.m2/repository/net/sf/kosmosfs/kfs/0.3/kfs-0.3.jar
/Users/scoundrel/.m2/repository/org/apache/commons/commons-math/2.0/commons-math-2.0.jar
/Users/scoundrel/.m2/repository/org/apache/ftpserver/ftplet-api/1.0.0/ftplet-api-1.0.0.jar
/Users/scoundrel/.m2/repository/org/apache/ftpserver/ftpserver-core/1.0.0/ftpserver-core-1.0.0.jar
/Users/scoundrel/.m2/repository/org/apache/ftpserver/ftpserver-deprecated/1.0.0-M2/ftpserver-deprecated-1.0.0-M2.jar
/Users/scoundrel/.m2/repository/org/apache/hadoop/avro/1.2.0/avro-1.2.0.jar
/Users/scoundrel/.m2/repository/org/apache/hadoop/hadoop-core/0.21.0-SNAPSHOT/hadoop-core-0.21.0-SNAPSHOT.jar
/Users/scoundrel/.m2/repository/org/apache/hadoop/hadoop-core-test/0.21.0-SNAPSHOT/hadoop-core-test-0.21.0-SNAPSHOT.jar
/Users/scoundrel/.m2/repository/org/apache/hadoop/hadoop-hdfs/0.21.0-SNAPSHOT/hadoop-hdfs-0.21.0-SNAPSHOT.jar
/Users/scoundrel/.m2/repository/org/apache/hadoop/hadoop-hdfs-test/0.21.0-SNAPSHOT/hadoop-hdfs-test-0.21.0-SNAPSHOT.jar
/Users/scoundrel/.m2/repository/org/apache/hadoop/hadoop-mapred/0.21.0-SNAPSHOT/hadoop-mapred-0.21.0-SNAPSHOT.jar
/Users/scoundrel/.m2/repository/org/apache/hadoop/hadoop-mapred-test/0.21.0-SNAPSHOT/hadoop-mapred-test-0.21.0-SNAPSHOT.jar
/Users/scoundrel/.m2/repository/org/apache/hadoop/zookeeper/zookeeper/3.2.2/zookeeper-3.2.2.jar
/Users/scoundrel/.m2/repository/org/apache/mina/mina-core/2.0.0-M5/mina-core-2.0.0-M5.jar
/Users/scoundrel/.m2/repository/org/apache/thrift/thrift/0.2.0/thrift-0.2.0.jar
/Users/scoundrel/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.0.1/jackson-core-asl-1.0.1.jar
/Users/scoundrel/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.0.1/jackson-mapper-asl-1.0.1.jar
/Users/scoundrel/.m2/repository/org/eclipse/jdt/core/3.1.1/core-3.1.1.jar
/Users/scoundrel/.m2/repository/org/jruby/jruby-complete/1.4.0/jruby-complete-1.4.0.jar
/Users/scoundrel/.m2/repository/org/mortbay/jetty/jetty/6.1.14/jetty-6.1.14.jar
/Users/scoundrel/.m2/repository/org/mortbay/jetty/jetty-util/6.1.14/jetty-util-6.1.14.jar
/Users/scoundrel/.m2/repository/org/mortbay/jetty/jsp-2.1/6.1.14/jsp-2.1-6.1.14.jar
/Users/scoundrel/.m2/repository/org/mortbay/jetty/jsp-api-2.1/6.1.14/jsp-api-2.1-6.1.14.jar
/Users/scoundrel/.m2/repository/org/mortbay/jetty/servlet-api-2.5/6.1.14/servlet-api-2.5-6.1.14.jar
/Users/scoundrel/.m2/repository/org/slf4j/slf4j-api/1.5.8/slf4j-api-1.5.8.jar
/Users/scoundrel/.m2/repository/org/slf4j/slf4j-log4j12/1.5.8/slf4j-log4j12-1.5.8.jar
/Users/scoundrel/.m2/repository/org/slf4j/slf4j-simple/1.5.8/slf4j-simple-1.5.8.jar
/Users/scoundrel/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar
/Users/scoundrel/.m2/repository/tomcat/jasper-compiler/5.5.12/jasper-compiler-5.5.12.jar
/Users/scoundrel/.m2/repository/tomcat/jasper-runtime/5.5.12/jasper-runtime-5.5.12.jar
/Users/scoundrel/.m2/repository/xmlenc/xmlenc/0.52/xmlenc-0.52.jar
].each do |f|
  $CLASSPATH << f
end

$CLASSPATH << 'src/test/resources/'
