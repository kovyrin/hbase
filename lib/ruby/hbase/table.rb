module Hbase
  # Wrapper for org.apache.hadoop.hbase.client.HTable
  class Table
    def initialize(configuration, tableName, formatter)
      @table = HTable.new(configuration, tableName)
      @formatter = formatter
    end

    # Delete a cell
    def delete(row, column, timestamp = HConstants::LATEST_TIMESTAMP)
      now = Time.now
      d = Delete.new(row.to_java_bytes, timestamp, nil)
      split = KeyValue.parseColumn(column.to_java_bytes)
      d.deleteColumn(split[0], split.length > 1 ? split[1] : nil, timestamp)
      @table.delete(d)
      @formatter.header()
      @formatter.footer(now)
    end

    def deleteall(row, column = nil, timestamp = HConstants::LATEST_TIMESTAMP)
      now = Time.now
      d = Delete.new(row.to_java_bytes, timestamp, nil)
      if column != nil
        split = KeyValue.parseColumn(column.to_java_bytes)
        d.deleteColumns(split[0], split.length > 1 ? split[1] : nil, timestamp)
      end
      @table.delete(d)
      @formatter.header()
      @formatter.footer(now)
    end

    def getAllColumns
       htd = @table.getTableDescriptor()
       result = []
       for f in htd.getFamilies()
         n = f.getNameAsString()
         n << ':'
         result << n
       end
       result
    end

    def scan(args = {})
      now = Time.now
      limit = -1
      maxlength = -1
      if args != nil and args.length > 0
        limit = args["LIMIT"] || -1
        maxlength = args["MAXLENGTH"] || -1
        filter = args["FILTER"] || nil
        startrow = args["STARTROW"] || ""
        stoprow = args["STOPROW"] || nil
        timestamp = args["TIMESTAMP"] || nil
        columns = args["COLUMNS"] || getAllColumns()
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
        for c in columns
          scan.addColumns(c)
        end
        if filter != nil
          scan.setFilter(filter)
        end
        if timestamp != nil
          scan.setTimeStamp(timestamp)
        end
        scan.setCacheBlocks(cache)
        scan.setMaxVersions(versions) if versions > 1
      else
        scan = Scan.new()
      end
      s = @table.getScanner(scan)
      count = 0
      @formatter.header(["ROW", "COLUMN+CELL"])
      i = s.iterator()
      while i.hasNext()
        r = i.next()
        row = Bytes::toStringBinary(r.getRow())
        if limit != -1 and count >= limit
          break
        end
        for kv in r.list
          family = String.from_java_bytes kv.getFamily()
          qualifier = Bytes::toStringBinary(kv.getQualifier())
          column = family + ':' + qualifier
          cell = toString(column, kv, maxlength)
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
      @formatter.header()
      @formatter.footer(now)
    end

    def incr(row, column, value = nil)
      now = Time.now
      split = KeyValue.parseColumn(column.to_java_bytes)
      family = split[0]
      qualifier = nil
      if split.length > 1
        qualifier = split[1]
      end
      if value == nil
        value = 1
      end
      @table.incrementColumnValue(row.to_java_bytes, family, qualifier, value)
      @formatter.header()
      @formatter.footer(now)
    end

    def isMetaTable()
      tn = @table.getTableName()
      return Bytes.equals(tn, HConstants::META_TABLE_NAME) ||
        Bytes.equals(tn, HConstants::ROOT_TABLE_NAME)
    end

    # Make a String of the passed kv
    # Intercept cells whose format we know such as the info:regioninfo in .META.
    def toString(column, kv, maxlength)
      if isMetaTable()
        if column == 'info:regioninfo'
          hri = Writables.getHRegionInfoOrNull(kv.getValue())
          return "timestamp=%d, value=%s" % [kv.getTimestamp(), hri.toString()]
        elsif column == 'info:serverstartcode'
          return "timestamp=%d, value=%s" % [kv.getTimestamp(), \
            Bytes.toLong(kv.getValue())]
        end
      end
      val = "timestamp=" + kv.getTimestamp().to_s + ", value=" + Bytes::toStringBinary(kv.getValue())
      maxlength != -1 ? val[0, maxlength] : val
    end

    # Get from table
    def get(row, args = {})
      now = Time.now
      result = nil
      if args == nil or args.length == 0 or (args.length == 1 and args[MAXLENGTH] != nil)
        get = Get.new(row.to_java_bytes)
      else
        # Its a hash.
        columns = args[COLUMN]
        if columns == nil
          # Maybe they used the COLUMNS key
          columns = args[COLUMNS]
        end
        if columns == nil
          # May have passed TIMESTAMP and row only; wants all columns from ts.
          ts = args[TIMESTAMP]
          if not ts
            raise ArgumentError.new("Failed parse of " + args + ", " + args.class)
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
              split = KeyValue.parseColumn(columns.to_java_bytes)
              if (split.length > 1)
                get.addColumn(split[0], split[1])
              else
                get.addFamily(split[0])
              end
            end
          else
            raise ArgumentError.new("Failed parse column argument type " +
              args + ", " + args.class)
          end
          get.setMaxVersions(args[VERSIONS] ? args[VERSIONS] : 1)
          if args[TIMESTAMP]
            get.setTimeStamp(args[TIMESTAMP])
          end
        end
      end
      result = @table.get(get)
      # Print out results.  Result can be Cell or RowResult.
      maxlength = args[MAXLENGTH] || -1
      @formatter.header(["COLUMN", "CELL"])
      if !result.isEmpty()
        for kv in result.list()
          family = String.from_java_bytes kv.getFamily()
          qualifier = Bytes::toStringBinary(kv.getQualifier())
          column = family + ':' + qualifier
          @formatter.row([column, toString(column, kv, maxlength)])
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
      i = s.iterator()
      @formatter.header()
      while i.hasNext()
        r = i.next()
        count += 1
        if count % interval == 0
          @formatter.row(["Current count: " + count.to_s + ", row: " + \
            (String.from_java_bytes r.getRow())])
        end
      end
      @formatter.footer(now, count)
    end
  end  
end
