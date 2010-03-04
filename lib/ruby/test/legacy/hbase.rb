  # Testing. To run this test, there needs to be an hbase cluster up and
  # running.  Then do: ${HBASE_HOME}/bin/hbase org.jruby.Main bin/HBase.rb
  if $0 == __FILE__
    # Add this directory to LOAD_PATH; presumption is that Formatter module
    # sits beside this one.  Then load it up.
    $LOAD_PATH.unshift File.dirname($PROGRAM_NAME)
    require 'Formatter'
    # Make a console formatter
    formatter = Formatter::Console.new(STDOUT)
    # Now add in java and hbase classes
    configuration = HBaseConfiguration.new()
    admin = Admin.new(configuration, formatter)
    # Drop old table.  If it does not exist, get an exception.  Catch and
    # continue
    TESTTABLE = "HBase_rb_testtable"
    begin
      admin.disable(TESTTABLE)
      admin.drop(TESTTABLE)
    rescue org.apache.hadoop.hbase.TableNotFoundException
      # Just suppress not found exception
    end
    admin.create(TESTTABLE, [{NAME => 'x', VERSIONS => 5}])
    # Presume it exists.  If it doesn't, next items will fail.
    table = Table.new(configuration, TESTTABLE, formatter)
    for i in 1..10
      table.put('x%d' % i, 'x:%d' % i, 'x%d' % i)
    end
    table.get('x1', {COLUMNS => 'x:1'})
    if formatter.rowCount() != 1
      raise IOError.new("Failed first put")
    end
    table.scan({COLUMNS => ['x:']})
    if formatter.rowCount() != 10
      raise IOError.new("Failed scan of expected 10 rows")
    end
    # Verify that limit works.
    table.scan({COLUMNS => ['x:'], LIMIT => 4})
    if formatter.rowCount() != 3
      raise IOError.new("Failed scan of expected 3 rows")
    end
    # Should only be two rows if we start at 8 (Row x10 sorts beside x1).
    table.scan({COLUMNS => ['x:'], STARTROW => 'x8', LIMIT => 3})
    if formatter.rowCount() != 2
      raise IOError.new("Failed scan of expected 2 rows")
    end
    # Scan between two rows
    table.scan({COLUMNS => ['x:'], STARTROW => 'x5', ENDROW => 'x8'})
    if formatter.rowCount() != 3
      raise IOError.new("Failed endrow test")
    end
    # Verify that incr works
    table.incr('incr1', 'c:1');
    table.scan({COLUMNS => ['c:1']})
    if formatter.rowCount() != 1
      raise IOError.new("Failed incr test")
    end
    # Verify that delete works
    table.delete('x1', 'x:1');
    table.scan({COLUMNS => ['x:1']})
    scan1 = formatter.rowCount()
    table.scan({COLUMNS => ['x:']})
    scan2 = formatter.rowCount()
    if scan1 != 0 or scan2 != 9
      raise IOError.new("Failed delete test")
    end
    # Verify that deletall works
    table.put('x2', 'x:1', 'x:1')
    table.deleteall('x2')
    table.scan({COLUMNS => ['x:2']})
    scan1 = formatter.rowCount()
    table.scan({COLUMNS => ['x:']})
    scan2 = formatter.rowCount()
    if scan1 != 0 or scan2 != 8
      raise IOError.new("Failed deleteall test")
    end
    admin.disable(TESTTABLE)
    admin.drop(TESTTABLE)
  end
