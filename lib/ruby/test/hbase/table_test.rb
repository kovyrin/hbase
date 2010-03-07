require 'test/unit'
require 'hbase'

module Hbase
  class TableTest < Test::Unit::TestCase
    def setup
      @formatter = Shell::Formatter::Console.new(:format_width => 110)
      @hbase = ::Hbase::Hbase.new(@formatter)
    end

    def table(table)
      @hbase.table(table)
    end

    define_test "Hbase::Table constructor should fail for non-existent tables" do
      assert_raise(NativeException) do
        table('non-existent-table-name')
      end
    end

    define_test "Hbase::Table constructor should not fail for existent tables" do
      assert_nothing_raised do
        table('.META.')
      end
    end
  end
end
