require 'test/unit'
require 'hbase'

module Hbase
  module TableTestHelpers
    def setup_hbase
      @formatter = Shell::Formatter::Console.new(:format_width => 110)
      @hbase = ::Hbase::Hbase.new
    end

    def table(table)
      @hbase.table(table, @formatter)
    end

    def admin
      @hbase.admin(@formatter)
    end

    def create_test_table(name)
      admin.create name, [{'NAME' => 'x', 'VERSIONS' => 5}] unless admin.exists?(name)
    end

    def destroy_test_table(name)
      return if admin.exists?(name)
      begin
        admin.disable(name)
        admin.drop(name)
      rescue org.apache.hadoop.hbase.TableNotFoundException
        # Just suppress not found exception
      end
    end
  end

  # Constructor tests
  class TableConstructorTest < Test::Unit::TestCase
    include TableTestHelpers
    def setup
      setup_hbase
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

  # Helper methods tests
  class TableHelpersTest < Test::Unit::TestCase
    include TableTestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_test_table_#{Time.now.to_i}"
      create_test_table(@test_name)
      @test_table = table(@test_name)
    end

    def teardown
      destroy_test_table(@test_name)
    end

    define_test "is_meta_table? method should return true for the meta table" do
      assert(table('.META.').is_meta_table?)
    end

    define_test "is_meta_table? method should return true for the root table" do
      assert(table('-ROOT-').is_meta_table?)
    end

    define_test "is_meta_table? method should return false for a normal table" do
      assert(!@test_table.is_meta_table?)
    end
  end
end
