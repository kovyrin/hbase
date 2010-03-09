# HBase ruby classes.
# Has wrapper classes for org.apache.hadoop.hbase.client.HBaseAdmin
# and for org.apache.hadoop.hbase.client.HTable.  Classes take
# Formatters on construction and outputs any results using
# Formatter methods.  These classes are only really for use by
# the hirb.rb HBase Shell script; they don't make much sense elsewhere.
# For example, the exists method on Admin class prints to the formatter
# whether the table exists and returns nil regardless.
include Java

include_class('java.lang.Integer') {|package,name| "J#{name}" }
include_class('java.lang.Long') {|package,name| "J#{name}" }
include_class('java.lang.Boolean') {|package,name| "J#{name}" }

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.io.hfile.Compression
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.ZooKeeperMain

module HBaseConstants
  COLUMN = "COLUMN"
  COLUMNS = "COLUMNS"
  TIMESTAMP = "TIMESTAMP"
  NAME = HConstants::NAME
  VERSIONS = HConstants::VERSIONS
  IN_MEMORY = HConstants::IN_MEMORY
  STOPROW = "STOPROW"
  STARTROW = "STARTROW"
  ENDROW = STOPROW
  LIMIT = "LIMIT"
  METHOD = "METHOD"
  MAXLENGTH = "MAXLENGTH"
  CACHE_BLOCKS = "CACHE_BLOCKS"
  REPLICATION_SCOPE = "REPLICATION_SCOPE"

  # Load constants from hbase java API
  def self.promote_constants(constants)
    # The constants to import are all in uppercase
    constants.each do |c|
      next if c =~ /DEFAULT_.*/ || c != c.upcase
      next if eval("defined?(#{c})")
      eval("#{c} = '#{c}'")
    end
  end

  promote_constants(org.apache.hadoop.hbase.HColumnDescriptor.constants)
  promote_constants(org.apache.hadoop.hbase.HTableDescriptor.constants)
end

# Include classes definition
require 'hbase/hbase'
require 'hbase/admin'
require 'hbase/table'
