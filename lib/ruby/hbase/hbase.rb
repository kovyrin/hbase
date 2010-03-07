module Hbase
  class Hbase
    attr_accessor :configuration, :formatter

    def initialize(formatter)
      # Save formatter and create configuration
      self.formatter = formatter
      self.configuration = org.apache.hadoop.hbase.HBaseConfiguration.new()

      # Turn off retries in hbase and ipc.  Human doesn't want to wait on N retries.
      configuration.setInt("hbase.client.retries.number", 7)
      configuration.setInt("ipc.client.connect.max.retries", 3)
    end

    def admin
      @admin ||= HBase::Admin.new(configuration, formatter)
    end

    # Create new one each time
    def table(table)
      HBase::Table.new(configuration, table, formatter)
    end
  end
end
