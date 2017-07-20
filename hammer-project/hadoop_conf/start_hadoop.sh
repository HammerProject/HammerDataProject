

# sudo -b mongod -f /etc/mongod.conf --smallfiles

export HADOOP_PREFIX=/home/hadoop/software/hadoop-2.7.1
export HADOOP_CONF_DIR=/home/hadoop/software/hadoop-2.7.1/etc/hadoop
export HADOOP_YARN_HOME=/home/hadoop/software/hadoop-2.7.1
export YARN_CONF_DIR=/home/hadoop/software/hadoop-2.7.1/etc/hadoop

$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
$HADOOP_PREFIX/sbin/hadoop-daemons.sh --config $HADOOP_CONF_DIR --script hdfs start datanode
$HADOOP_PREFIX/sbin/start-dfs.sh
$HADOOP_YARN_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR start nodemanager
$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start proxyserver
$HADOOP_PREFIX/sbin/start-yarn.sh
$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver


