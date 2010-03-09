puts "-----------------------------------------------------------"
puts "Starting test cluster"

include Java

# Set logging level to avoid verboseness
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(org.apache.log4j.Level::ERROR)
org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level::ERROR)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(org.apache.log4j.Level::ERROR)

# Load testing class
import org.apache.hadoop.hbase.HBaseTestingUtility

# Create the cluster
$TEST_CLUSTER = HBaseTestingUtility.new
$TEST_CLUSTER.startMiniCluster

puts "-----------------------------------------------------------"
