include Java
require "lib/ruby/test/test_helper"

# Set logging level to avoid verboseness
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(org.apache.log4j.Level::ERROR)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(org.apache.log4j.Level::ERROR)

# Load the test files from the command line.
ARGV.each { |f| load(f) unless f =~ /^-/  }
