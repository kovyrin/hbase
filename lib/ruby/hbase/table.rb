# Wrapper for org.apache.hadoop.hbase.client.HTable

module Hbase
  class Table
    include HBaseConstants

    def initialize(configuration, table_name, formatter)
      @table = HTable.new(configuration, table_name)
      @formatter = formatter
    end

    # Delete a cell
    def delete(row, column, timestamp = HConstants::LATEST_TIMESTAMP)
      now = Time.now
      d = Delete.new(row.to_java_bytes, timestamp, nil)
      split = KeyValue.parseColumn(column.to_java_bytes)
      d.deleteColumn(split[0], (split.length > 1) ? split[1] : nil, timestamp)
      @table.delete(d)
      @formatter.header
      @formatter.footer(now)
    end

    def deleteall(row, column = nil, timestamp = HConstants::LATEST_TIMESTAMP)
      now = Time.now
      d = Delete.new(row.to_java_bytes, timestamp, nil)
      if column
        split = KeyValue.parseColumn(column.to_java_bytes)
        d.deleteColumns(split[0], (split.length > 1) ? split[1] : nil, timestamp)
      end
      @table.delete(d)
      @formatter.header
      @formatter.footer(now)
    end

    def scan(args = {})
      now = Time.now
      limit = -1
      maxlength = -1
      if args && args.length > 0
        limit = args["LIMIT"] || -1
        maxlength = args["MAXLENGTH"] || -1
        filter = args["FILTER"]
        startrow = args["STARTROW"] || ""
        stoprow = args["STOPROW"]
        timestamp = args["TIMESTAMP"]
        columns = args["COLUMNS"] || get_all_columns
        cache = args["CACHE_BLOCKS"] || true
        versions = args["VERSIONS"] || 1

        if columns.class == String
          columns = [columns]
        elsif columns.class != Array
          raise ArgumentError.new("COLUMNS must be specified as a String or an Array")
        end

        if stoprow
          scan = Scan.new(startrow.to_java_bytes, stoprow.to_java_bytes)
        else
          scan = Scan.new(startrow.to_java_bytes)
        end

        columns.each { |c| scan.addColumns(c) }
        scan.setFilter(filter) if filter
        scan.setTimeStamp(timestamp) if timestamp
        scan.setCacheBlocks(cache)
        scan.setMaxVersions(versions) if versions > 1
      else
        scan = Scan.new
      end
      s = @table.getScanner(scan)
      count = 0
      @formatter.header(["ROW", "COLUMN+CELL"])
      i = s.iterator
      while i.hasNext
        r = i.next
        row = Bytes::toStringBinary(r.getRow)
        break if limit != -1 && count >= limit

        r.list.each do |kv|
          family = String.from_java_bytes(kv.getFamily)
          qualifier = Bytes::toStringBinary(kv.getQualifier)
          column = "#{family}:#{qualifier}"
          cell = to_string(column, kv, maxlength)
          @formatter.row([row, "column=%s, %s" % [column, cell]])
        end
        count += 1
      end
      @formatter.footer(now, count)
    end

    def put(row, column, value, timestamp = nil)
      now = Time.now
      p = Put.new(row.to_java_bytes)
      split = KeyValue.parseColumn(column.to_java_bytes)
      if split.length > 1
        if timestamp
          p.add(split[0], split[1], timestamp, value.to_java_bytes)
        else
          p.add(split[0], split[1], value.to_java_bytes)
        end
      else
        if timestamp
          p.add(split[0], nil, timestamp, value.to_java_bytes)
        else
          p.add(split[0], nil, value.to_java_bytes)
        end
      end
      @table.put(p)
      @formatter.header
      @formatter.footer(now)
    end

    def incr(row, column, value = nil)
      now = Time.now
      split = KeyValue.parseColumn(column.to_java_bytes)
      family = split[0]
      qualifier = nil
      qualifier = split[1] if split.length > 1
      value ||= 1
      @table.incrementColumnValue(row.to_java_bytes, family, qualifier, value)
      @formatter.header
      @formatter.footer(now)
    end

    # Get from table
    def get(row, args = {})
      now = Time.now
      if args == nil or args.length == 0 or (args.length == 1 and args[MAXLENGTH] != nil)
        get = Get.new(row.to_java_bytes)
      else
        # Its a hash.
        columns = args[COLUMN] || args[COLUMNS]
        unless columns
          # May have passed TIMESTAMP and row only; wants all columns from ts.
          unless ts = args[TIMESTAMP]
            raise ArgumentError, "Failed parse of #{args.inspect}, #{args.class}"
          end
          get = Get.new(row.to_java_bytes, ts)
        else
          get = Get.new(row.to_java_bytes)
          # Columns are non-nil
          if columns.class == String
            # Single column
            split = KeyValue.parseColumn(columns.to_java_bytes)
            if (split.length > 1)
              get.addColumn(split[0], split[1])
            else
              get.addFamily(split[0])
            end
          elsif columns.class == Array
            for column in columns
              split = KeyValue.parseColumn(column.to_java_bytes)
              if (split.length > 1)
                get.addColumn(split[0], split[1])
              else
                get.addFamily(split[0])
              end
            end
          else
            raise ArgumentError, "Failed parse column argument type #{args.inspect}, #{args.class}"
          end
          get.setMaxVersions(args[VERSIONS] ? args[VERSIONS] : 1)
          get.setTimeStamp(args[TIMESTAMP]) if args[TIMESTAMP]
        end
      end

      # Call hbase for the results
      result = @table.get(get)

      # Print out results.  Result can be Cell or RowResult.
      maxlength = args[MAXLENGTH] || -1
      @formatter.header(["COLUMN", "CELL"])

      # Print result rows
      unless result.isEmpty
        result.list.each do |kv|
          family = String.from_java_bytes(kv.getFamily)
          qualifier = Bytes::toStringBinary(kv.getQualifier)
          column = "#{family}:#{qualifier}"
          @formatter.row([column, to_string(column, kv, maxlength)])
        end
      end

      @formatter.footer(now)
    end

    def count(interval = 1000)
      now = Time.now
      scan = Scan.new()
      scan.setCacheBlocks(false)
      # We can safely set scanner caching with the first key only filter
      scan.setCaching(10)
      scan.setFilter(FirstKeyOnlyFilter.new())
      s = @table.getScanner(scan)
      count = 0
      i = s.iterator
      @formatter.header
      while i.hasNext
        r = i.next
        count += 1
        if count % interval == 0
          @formatter.row(["Current count: #{count}, row: #{String.from_java_bytes r.getRow}"])
        end
      end
      @formatter.footer(now, count)
    end

    #----------------------------------------------------------------------------------------
    # Helper methods

    def get_all_columns
      @table.getTableDescriptor.getFamilies.map do |family|
        "#{family.getNameAsString}:"
      end
    end

    # Checks if current table is one of the 'meta' tables
    def is_meta_table?
      tn = @table.getTableName
      Bytes.equals(tn, HConstants::META_TABLE_NAME) || Bytes.equals(tn, HConstants::ROOT_TABLE_NAME)
    end

    # Make a String of the passed kv
    # Intercept cells whose format we know such as the info:regioninfo in .META.
    def to_string(column, kv, maxlength)
      if isMetaTable()
        if column == 'info:regioninfo'
          hri = Writables.getHRegionInfoOrNull(kv.getValue)
          return "timestamp=%d, value=%s" % [kv.getTimestamp, hri.toString]
        end
        if column == 'info:serverstartcode'
          return "timestamp=%d, value=%s" % [kv.getTimestamp, Bytes.toLong(kv.getValue)]
        end
      end

      val = "timestamp=#{kv.getTimestamp}, value=#{Bytes::toStringBinary(kv.getValue)}"
      (maxlength != -1) ? val[0, maxlength] : val
    end

  end
end
