# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class Admin
    def initialize(configuration, formatter)
      @admin = HBaseAdmin.new(configuration)
      connection = @admin.getConnection()
      @zkWrapper = connection.getZooKeeperWrapper()
      zk = @zkWrapper.getZooKeeper()
      @zkMain = ZooKeeperMain.new(zk)
      @formatter = formatter
    end

    def list
      now = Time.now
      @formatter.header()
      for t in @admin.listTables()
        @formatter.row([t.getNameAsString()])
      end
      @formatter.footer(now)
    end

    def describe(tableName)
      now = Time.now
      @formatter.header(["DESCRIPTION", "ENABLED"], [64])
      found = false
      tables = @admin.listTables().to_a
      tables.push(HTableDescriptor::META_TABLEDESC, HTableDescriptor::ROOT_TABLEDESC)
      for t in tables
        if t.getNameAsString() == tableName
          @formatter.row([t.to_s, "%s" % [@admin.isTableEnabled(tableName)]], true, [64])
          found = true
        end
      end
      if not found
        raise ArgumentError.new("Failed to find table named " + tableName)
      end
      @formatter.footer(now)
    end

    def exists(tableName)
      now = Time.now
      @formatter.header()
      e = @admin.tableExists(tableName)
      @formatter.row([e.to_s])
      @formatter.footer(now)
    end

    def flush(tableNameOrRegionName)
      now = Time.now
      @formatter.header()
      @admin.flush(tableNameOrRegionName)
      @formatter.footer(now)
    end

    def compact(tableNameOrRegionName)
      now = Time.now
      @formatter.header()
      @admin.compact(tableNameOrRegionName)
      @formatter.footer(now)
    end

    def major_compact(tableNameOrRegionName)
      now = Time.now
      @formatter.header()
      @admin.majorCompact(tableNameOrRegionName)
      @formatter.footer(now)
    end

    def split(tableNameOrRegionName)
      now = Time.now
      @formatter.header()
      @admin.split(tableNameOrRegionName)
      @formatter.footer(now)
    end

    def enable(tableName)
      # TODO: Need an isEnabled method
      now = Time.now
      @admin.enableTable(tableName)
      @formatter.header()
      @formatter.footer(now)
    end

    def disable(tableName)
      # TODO: Need an isDisabled method
      now = Time.now
      @admin.disableTable(tableName)
      @formatter.header()
      @formatter.footer(now)
    end

    def enable_region(regionName)
      online(regionName, false)
    end

    def disable_region(regionName)
      online(regionName, true)
    end

    def online(regionName, onOrOff)
      now = Time.now
      meta = HTable.new(HConstants::META_TABLE_NAME)
      bytes = Bytes.toBytes(regionName)
      g = Get.new(bytes)
      g.addColumn(HConstants::CATALOG_FAMILY,
        HConstants::REGIONINFO_QUALIFIER)
      hriBytes = meta.get(g).value()
      hri = Writables.getWritable(hriBytes, HRegionInfo.new());
      hri.setOffline(onOrOff)
      put = Put.new(bytes)
      put.add(HConstants::CATALOG_FAMILY,
        HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri))
      meta.put(put);
      @formatter.header()
      @formatter.footer(now)
    end

    def drop(tableName)
      now = Time.now
      @formatter.header()
      if @admin.isTableEnabled(tableName)
        raise IOError.new("Table " + tableName + " is enabled. Disable it first")
      else
        @admin.deleteTable(tableName)
        flush(HConstants::META_TABLE_NAME);
        major_compact(HConstants::META_TABLE_NAME);
      end
      @formatter.footer(now)
    end

    def truncate(tableName)
      now = Time.now
      @formatter.header()
      hTable = HTable.new(tableName)
      tableDescription = hTable.getTableDescriptor()
      puts 'Truncating ' + tableName + '; it may take a while'
      puts 'Disabling table...'
      disable(tableName)
      puts 'Dropping table...'
      drop(tableName)
      puts 'Creating table...'
      @admin.createTable(tableDescription)
      @formatter.footer(now)
    end

    # Pass tablename and an array of Hashes
    def create(tableName, args)
      now = Time.now
      # Pass table name and an array of Hashes.  Later, test the last
      # array to see if its table options rather than column family spec.
      raise TypeError.new("Table name must be of type String") \
        unless tableName.instance_of? String
      # For now presume all the rest of the args are column family
      # hash specifications. TODO: Add table options handling.
      htd = HTableDescriptor.new(tableName)
      for arg in args
        if arg.instance_of? String
          htd.addFamily(HColumnDescriptor.new(arg))
        else
          raise TypeError.new(arg.class.to_s + " of " + arg.to_s + " is not of Hash type") \
            unless arg.instance_of? Hash
          htd.addFamily(hcd(arg))
        end
      end
      @admin.createTable(htd)
      @formatter.header()
      @formatter.footer(now)
    end

    def alter(tableName, args)
      now = Time.now
      raise TypeError.new("Table name must be of type String") \
        unless tableName.instance_of? String
      htd = @admin.getTableDescriptor(tableName.to_java_bytes)
      method = args.delete(METHOD)
      if method == "delete"
        @admin.deleteColumn(tableName, args[NAME])
      elsif method == "table_att"
        if args[MAX_FILESIZE]
          htd.setMaxFileSize(JLong.valueOf(args[MAX_FILESIZE]))
        end
        if args[READONLY]
          htd.setReadOnly(JBoolean.valueOf(args[READONLY]))
        end
        if args[MEMSTORE_FLUSHSIZE]
          htd.setMemStoreFlushSize(JLong.valueOf(args[MEMSTORE_FLUSHSIZE]))
        end
        if args[DEFERRED_LOG_FLUSH]
          htd.setDeferredLogFlush(JBoolean.valueOf(args[DEFERRED_LOG_FLUSH]))
        end
        @admin.modifyTable(tableName.to_java_bytes, htd)
      else
        descriptor = hcd(args)
        if (htd.hasFamily(descriptor.getNameAsString().to_java_bytes))
          @admin.modifyColumn(tableName, descriptor.getNameAsString(),
                              descriptor);
        else
          @admin.addColumn(tableName, descriptor);
        end
      end
      @formatter.header()
      @formatter.footer(now)
    end

    def close_region(regionName, server)
      now = Time.now
      s = nil
      s = [server].to_java if server
      @admin.closeRegion(regionName, s)
      @formatter.header()
      @formatter.footer(now)
    end

    def shutdown()
      @admin.shutdown()
    end

    def status(format)
      status = @admin.getClusterStatus()
      if format != nil and format == "detailed"
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
      elsif format != nil and format == "simple"
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
      @zkMain.executeLine(line)
    end

    def zk_dump
      puts @zkWrapper.dump
    end
  end
end
