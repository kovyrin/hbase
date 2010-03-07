# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class Admin
    def initialize(configuration, formatter)
      @admin = HBaseAdmin.new(configuration)
      connection = @admin.getConnection()
      @zk_wrapper = connection.getZooKeeperWrapper()
      zk = @zk_wrapper.getZooKeeper()
      @zk_main = ZooKeeperMain.new(zk)
      @formatter = formatter
    end

    def list
      now = Time.now
      @formatter.header
      @admin.listTables.each { |t| @formatter.row([t.getNameAsString]) }
      @formatter.footer(now)
    end

    def describe(table_name)
      now = Time.now
      @formatter.header(["DESCRIPTION", "ENABLED"], [64])
      found = false

      tables = @admin.listTables().to_a
      tables << HTableDescriptor::META_TABLEDESC
      tables << HTableDescriptor::ROOT_TABLEDESC

      tables.each do |t|
        if t.getNameAsString == table_name
          @formatter.row([t.to_s, "%s" % [@admin.isTableEnabled(table_name)]], true, [64])
          found = true
          break
        end
      end
      raise(ArgumentError, "Failed to find table named #{table_name}") unless found

      @formatter.footer(now)
    end

    def exists(table_name)
      now = Time.now
      @formatter.header
      e = @admin.tableExists(table_name)
      @formatter.row([e.to_s])
      @formatter.footer(now)
    end

    def flush(table_or_region_name)
      now = Time.now
      @formatter.header
      @admin.flush(table_or_region_name)
      @formatter.footer(now)
    end

    def compact(table_or_region_name)
      now = Time.now
      @formatter.header
      @admin.compact(table_or_region_name)
      @formatter.footer(now)
    end

    def major_compact(table_or_region_name)
      now = Time.now
      @formatter.header
      @admin.majorCompact(table_or_region_name)
      @formatter.footer(now)
    end

    def split(table_or_region_name)
      now = Time.now
      @formatter.header
      @admin.split(table_or_region_name)
      @formatter.footer(now)
    end

    def enable(table_name)
      # TODO: Need an isEnabled method
      now = Time.now
      @admin.enableTable(table_name)
      @formatter.header
      @formatter.footer(now)
    end

    def disable(table_name)
      # TODO: Need an isDisabled method
      now = Time.now
      @admin.disableTable(table_name)
      @formatter.header
      @formatter.footer(now)
    end

    def enable_region(region_name)
      online(region_name, false)
    end

    def disable_region(region_name)
      online(region_name, true)
    end

    def online(region_name, on_off)
      now = Time.now

      # Open meta table
      meta = HTable.new(HConstants::META_TABLE_NAME)

      # Read region info
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

      @formatter.header
      @formatter.footer(now)
    end

    def drop(table_name)
      now = Time.now

      if @admin.isTableEnabled(table_name)
        raise IOError.new("Table " + table_name + " is enabled. Disable it first")
      end

      @formatter.header
      @admin.deleteTable(table_name)
      flush(HConstants::META_TABLE_NAME);
      major_compact(HConstants::META_TABLE_NAME);

      @formatter.footer(now)
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

    # Pass table_name and an array of Hashes
    def create(table_name, args)
      now = Time.now
      # Pass table name and an array of Hashes.  Later, test the last
      # array to see if its table options rather than column family spec.
      raise(TypeError, "Table name must be of type String") unless table_name.instance_of?(String)

      # For now presume all the rest of the args are column family
      # hash specifications.
      # TODO: Add table options handling.
      htd = HTableDescriptor.new(table_name)
      args.each do |arg|
        if arg.instance_of? String
          htd.addFamily(HColumnDescriptor.new(arg))
        else
          raise(TypeError, "#{arg.class} of #{arg.inspect} is not of Hash type") unless arg.instance_of?(Hash)
          htd.addFamily(hcd(arg))
        end
      end
      @admin.createTable(htd)

      @formatter.header
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

    def close_region(region_name, server)
      now = Time.now
      s = nil
      s = [server].to_java if server
      @admin.closeRegion(region_name, s)
      @formatter.header
      @formatter.footer(now)
    end

    def shutdown()
      @admin.shutdown()
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
        puts("%d servers, %d dead, %.4f average load" % \
          [ status.getServers(), status.getDeadServers(), \
            status.getAverageLoad()])
      end
    end
    def hcd(arg)
      # Return a new HColumnDescriptor made of passed args
      # TODO: This is brittle code.
      # Here is current HCD constructor:
      # public HColumnDescriptor(final byte [] familyName, final int maxVersions,
      # final String compression, final boolean inMemory,
      # final boolean blockCacheEnabled, final int blocksize,
      # final int timeToLive, final boolean bloomFilter, final int scope) {
      name = arg[NAME]
      raise ArgumentError.new("Column family " + arg + " must have a name") \
        unless name
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

    def zk(args)
      line = args.join(' ')
      line = 'help' if line.empty?
      @zk_main.executeLine(line)
    end

    def zk_dump
      puts @zk_wrapper.dump
    end
  end
end
