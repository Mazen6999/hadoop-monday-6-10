# hadoop-monday-6-10

#!/bin/bash

echo "Starting hadoop Service " 
hdfs --daemon start namenode

# Start DataNode
hdfs --daemon start datanode

# Start Secondary NameNode
hdfs --daemon start secondarynamenode

# Start ResourceManager
yarn --daemon start resourcemanager

# Start NodeManager
yarn --daemon start nodemanager

# 3. Verify services are running
jps
