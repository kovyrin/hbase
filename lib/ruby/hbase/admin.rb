# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class Admin
    include HBaseConstants

    def initialize(configuration, formatter)
      @admin = HBaseAdmin.new(configuration)
      connection = @admin.getConnection()
      @zk_wrapper = connection.getZooKeeperWrapper()
      zk = @zk_wrapper.getZooKeeper()
      @zk_main = ZooKeeperMain.new(zk)
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of tables in hbase
    def list
      @admin.listTables.map { |t| t.getNameAsString }
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region flush
    def flush(table_or_region_name)
      @admin.flush(table_or_region_name)
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region compaction
    def compact(table_or_region_name)
      @admin.compact(table_or_region_name)
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region major compaction
    def major_compact(table_or_region_name)
      @admin.majorCompact(table_or_region_name)
    end

    #----------------------------------------------------------------------------------------------
    # Requests a table or region split
    def split(table_or_region_name)
      @admin.split(table_or_region_name)
    end

    #----------------------------------------------------------------------------------------------
    # Enables a table
    def enable(table_name)
      return if enabled?(table_name)
      @admin.enableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Disables a table
    def disable(table_name)
      return unless enabled?(table_name)
      @admin.disableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Drops a table
    def drop(table_name)
      raise ArgumentError, "Table #{table_name} does not exist.'" unless exists?(table_name)
      raise ArgumentError, "Table #{table_name} is enabled. Disable it first.'" if enabled?(table_name)

      @admin.deleteTable(table_name)
      flush(HConstants::META_TABLE_NAME)
      major_compact(HConstants::META_TABLE_NAME)
    end

    #----------------------------------------------------------------------------------------------
    # Shuts hbase down
    def shutdown
      @admin.shutdown
    end

    #----------------------------------------------------------------------------------------------
    # Returns ZooKeeper status dump
    def zk_dump
      @zk_wrapper.dump
    end

    #----------------------------------------------------------------------------------------------
    # Creates a table
    def create(table_name, *args)
      # Fail if table name is not a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Flatten params array
      args = args.flatten.compact

      # Fail if no column families defined
      raise(ArgumentError, "Table must have at least one column family") if args.empty?

      # Start defining the table
      htd = HTableDescriptor.new(table_name)

      # All args are columns, add them to the table definition
      # TODO: add table options support
      args.each do |arg|
        unless arg.kind_of?(String) || arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
        end

        # Add column to the table
        htd.addFamily(hcd(arg))
      end

      # Perform the create table call
      @admin.createTable(htd)
    end

    #----------------------------------------------------------------------------------------------
    # Closes a region
    def close_region(region_name, server = nil)
      @admin.closeRegion(region_name, server ? [server].to_java : nil)
    end

    #----------------------------------------------------------------------------------------------
    # Enables a region
    def enable_region(region_name)
      online(region_name, false)
    end

    #----------------------------------------------------------------------------------------------
    # Disables a region
    def disable_region(region_name)
      online(region_name, true)
    end

    #----------------------------------------------------------------------------------------------
    # Returns table's structure description
    def describe(table_name)
      tables = @admin.listTables.to_a
      tables << HTableDescriptor::META_TABLEDESC
      tables << HTableDescriptor::ROOT_TABLEDESC

      tables.each do |t|
        # Found the table
        return t.to_s if t.getNameAsString == table_name
      end

      raise(ArgumentError, "Failed to find table named #{table_name}")
    end

    def truncate(table_name)
      now = Time.now
      @formatter.header
      h_table = HTable.new(table_name)
      table_description = h_table.getTableDescriptor()
      puts 'Truncating ' + table_name + '; it may take a while'
      puts 'Disabling table...'
      disable(table_name)
      puts 'Dropping table...'
      drop(table_name)
      puts 'Creating table...'
      @admin.createTable(table_description)
      @formatter.footer(now)
    end

    def alter(table_name, args)
      now = Time.now

      raise(TypeError, "Table name must be of type String") unless table_name.instance_of?(String)
      htd = @admin.getTableDescriptor(table_name.to_java_bytes)
      method = args.delete(METHOD)

      if method == "delete"
        @admin.deleteColumn(table_name, args[NAME])
      elsif method == "table_att"
        htd.setMaxFileSize(JLong.valueOf(args[MAX_FILESIZE])) if args[MAX_FILESIZE]
        htd.setReadOnly(JBoolean.valueOf(args[READONLY])) if args[READONLY]
        htd.setMemStoreFlushSize(JLong.valueOf(args[MEMSTORE_FLUSHSIZE])) if args[MEMSTORE_FLUSHSIZE]
        htd.setDeferredLogFlush(JBoolean.valueOf(args[DEFERRED_LOG_FLUSH])) if args[DEFERRED_LOG_FLUSH]
        @admin.modifyTable(table_name.to_java_bytes, htd)
      else
        descriptor = hcd(args)
        if htd.hasFamily(descriptor.getNameAsString.to_java_bytes)
          @admin.modifyColumn(table_name, descriptor.getNameAsString, descriptor);
        else
          @admin.addColumn(table_name, descriptor);
        end
      end

      @formatter.header
      @formatter.footer(now)
    end

    def status(format)
      status = @admin.getClusterStatus()
      if format == "detailed"
        puts("version %s" % [ status.getHBaseVersion() ])
        # Put regions in transition first because usually empty
        puts("%d regionsInTransition" % status.getRegionsInTransition().size())
        for k, v in status.getRegionsInTransition()
          puts("    %s" % [v])
        end
        puts("%d live servers" % [ status.getServers() ])
        for server in status.getServerInfo()
          puts("    %s:%d %d" % \
            [ server.getServerAddress().getHostname(),  \
              server.getServerAddress().getPort(), server.getStartCode() ])
          puts("        %s" % [ server.getLoad().toString() ])
          for region in server.getLoad().getRegionsLoad()
            puts("        %s" % [ region.getNameAsString() ])
            puts("            %s" % [ region.toString() ])
          end
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
      elsif format == "simple"
        load = 0
        regions = 0
        puts("%d live servers" % [ status.getServers() ])
        for server in status.getServerInfo()
          puts("    %s:%d %d" % \
            [ server.getServerAddress().getHostname(),  \
              server.getServerAddress().getPort(), server.getStartCode() ])
          puts("        %s" % [ server.getLoad().toString() ])
          load += server.getLoad().getNumberOfRequests()
          regions += server.getLoad().getNumberOfRegions()
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
        puts("Aggregate load: %d, regions: %d" % [ load , regions ] )
      else
        puts "#{status.getServers} servers, #{status.getDeadServers} dead, #{'%.4f' % status.getAverageLoad} average load"
      end
    end

    def zk(args)
      line = args.join(' ')
      line = 'help' if line.empty?
      @zk_main.executeLine(line)
    end

    #----------------------------------------------------------------------------------------------
    #
    # Helper methods
    #

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Is table enabled
    def enabled?(table_name)
      @admin.isTableEnabled(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Return a new HColumnDescriptor made of passed args
    def hcd(arg)
      # String arg, single parameter constructor
      return HColumnDescriptor.new(arg) if arg.kind_of?(String)

      # TODO: This is brittle code.
      # Here is current HCD constructor:
      # public HColumnDescriptor(final byte [] familyName, final int maxVersions,
      # final String compression, final boolean inMemory,
      # final boolean blockCacheEnabled, final int blocksize,
      # final int timeToLive, final boolean bloomFilter, final int scope) {
      raise(ArgumentError, "Column family #{arg} must have a name") unless name = arg[NAME]

      # TODO: What encoding are Strings in jruby?
      return HColumnDescriptor.new(name.to_java_bytes,
        # JRuby uses longs for ints. Need to convert.  Also constants are String
        arg[VERSIONS]? JInteger.new(arg[VERSIONS]): HColumnDescriptor::DEFAULT_VERSIONS,
        arg[HColumnDescriptor::COMPRESSION]? arg[HColumnDescriptor::COMPRESSION]: HColumnDescriptor::DEFAULT_COMPRESSION,
        arg[IN_MEMORY]? JBoolean.valueOf(arg[IN_MEMORY]): HColumnDescriptor::DEFAULT_IN_MEMORY,
        arg[HColumnDescriptor::BLOCKCACHE]? JBoolean.valueOf(arg[HColumnDescriptor::BLOCKCACHE]): HColumnDescriptor::DEFAULT_BLOCKCACHE,
        arg[HColumnDescriptor::BLOCKSIZE]? JInteger.valueOf(arg[HColumnDescriptor::BLOCKSIZE]): HColumnDescriptor::DEFAULT_BLOCKSIZE,
        arg[HColumnDescriptor::TTL]? JInteger.new(arg[HColumnDescriptor::TTL]): HColumnDescriptor::DEFAULT_TTL,
        arg[HColumnDescriptor::BLOOMFILTER]? JBoolean.valueOf(arg[HColumnDescriptor::BLOOMFILTER]): HColumnDescriptor::DEFAULT_BLOOMFILTER,
        arg[HColumnDescriptor::REPLICATION_SCOPE]? JInteger.new(arg[REPLICATION_SCOPE]): HColumnDescriptor::DEFAULT_REPLICATION_SCOPE)
    end

    #----------------------------------------------------------------------------------------------
    # Enables/disables a region by name
    def online(region_name, on_off)
      # Open meta table
      meta = HTable.new(HConstants::META_TABLE_NAME)

      # Read region info
      # FIXME: fail gracefully if can't find the region
      region_bytes = Bytes.toBytes(region_name)
      g = Get.new(region_bytes)
      g.addColumn(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER)
      hri_bytes = meta.get(g).value

      # Change region status
      hri = Writables.getWritable(hri_bytes, HRegionInfo.new)
      hri.setOffline(on_off)

      # Write it back
      put = Put.new(region_bytes)
      put.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri))
      meta.put(put)
    end
  end
end
