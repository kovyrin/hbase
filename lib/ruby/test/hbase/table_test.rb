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
      @test_name = "hbase_table_test_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)
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

    #-------------------------------------------------------------------------------

    define_test "get_all_columns should return columns list" do
      cols = table('.META.').get_all_columns
      assert_kind_of(Array, cols)
      assert(cols.length > 0)
    end

    #-------------------------------------------------------------------------------

    define_test "parse_column_name should not return a qualifier for name-only column specifiers" do
      col, qual = table('.META.').parse_column_name('foo')
      assert_not_nil(col)
      assert_nil(qual)
    end

    define_test "parse_column_name should not return a qualifier for family-only column specifiers" do
      col, qual = table('.META.').parse_column_name('foo:')
      assert_not_nil(col)
      assert_nil(qual)
    end

    define_test "parse_column_name should return a qualifier for family:qualifier column specifiers" do
      col, qual = table('.META.').parse_column_name('foo:bar')
      assert_not_nil(col)
      assert_not_nil(qual)
    end
  end

  # Helper methods tests
  class TableSimpleMethodsTest < Test::Unit::TestCase
    include TableTestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_table_test_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)
    end

    define_test "put should work without timestamp" do
      @test_table.put("123", "x:a", "1")
    end

    define_test "put should work with timestamp" do
      @test_table.put("123", "x:a", "2", Time.now.to_i)
    end

    define_test "put should work with integer keys" do
      @test_table.put(123, "x:a", "3")
    end

    define_test "put should work with integer values" do
      @test_table.put("123", "x:a", 4)
    end

    #-------------------------------------------------------------------------------

    define_test "delete should work without timestamp" do
      @test_table.delete("123", "x:a")
    end

    define_test "delete should work with timestamp" do
      @test_table.delete("123", "x:a", Time.now.to_i)
    end

    define_test "delete should work with integer keys" do
      @test_table.delete(123, "x:a")
    end

    #-------------------------------------------------------------------------------

    define_test "deleteall should work w/o columns and timestamps" do
      @test_table.deleteall("123")
    end

    define_test "deleteall should work with integer keys" do
      @test_table.deleteall(123)
    end
  end
end
