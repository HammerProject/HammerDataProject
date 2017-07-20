export HADOOP_PREFIX=/home/hadoop/software/hadoop-2.7.1
export HADOOP_CONF_DIR=/home/hadoop/software/hadoop-2.7.1/etc/hadoop
export HADOOP_YARN_HOME=/home/hadoop/software/hadoop-2.7.1 

$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs stop namenode
$HADOOP_PREFIX/sbin/hadoop-daemons.sh --config $HADOOP_CONF_DIR --script hdfs stop datanode
$HADOOP_PREFIX/sbin/stop-dfs.sh
$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR stop resourcemanager
$HADOOP_YARN_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR stop nodemanager
$HADOOP_PREFIX/sbin/stop-yarn.sh
$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR stop proxyserver
$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR stop historyserver
