require 'test/unit'
require 'hbase'
require 'test/hbase/test_helper'

include HBaseConstants

module Hbase
  class AdminHelpersTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)
    end

    define_test "exists? should return true when a table exists" do
      assert(admin.exists?('.META.'))
    end

    define_test "exists? should return false when a table exists" do
      assert(!admin.exists?('.NOT.EXISTS.'))
    end

    define_test "enabled? should return true for enabled tables" do
      admin.enable(@test_name)
      assert(admin.enabled?(@test_name))
    end

    define_test "enabled? should return false for disabled tables" do
      admin.disable(@test_name)
      assert(!admin.enabled?(@test_name))
    end
  end

    # Simple administration methods tests
  class AdminSimpleMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)

      # Create table test table name
      @create_test_name = 'hbase_create_table_test_table'
    end

    define_test "list should return a list of tables" do
      assert(admin.list.member?(@test_name))
    end

    define_test "list should not return meta tables" do
      assert(!admin.list.member?('.META.'))
      assert(!admin.list.member?('-ROOT-'))
    end

    #-------------------------------------------------------------------------------

    define_test "flush should work" do
      admin.flush('.META.')
    end

    #-------------------------------------------------------------------------------

    define_test "compact should work" do
      admin.compact('.META.')
    end

    #-------------------------------------------------------------------------------

    define_test "major_compact should work" do
      admin.major_compact('.META.')
    end

    #-------------------------------------------------------------------------------

    define_test "split should work" do
      admin.split('.META.')
    end

    #-------------------------------------------------------------------------------

    define_test "drop should fail on non-existent tables" do
      assert_raise(ArgumentError) do
        admin.drop('.NOT.EXISTS.')
      end
    end

    define_test "drop should fail on enabled tables" do
      assert_raise(ArgumentError) do
        admin.drop(@test_name)
      end
    end

    define_test "drop should drop tables" do
      admin.disable(@test_name)
      admin.drop(@test_name)
      assert(!admin.exists?(@test_name))
    end

    #-------------------------------------------------------------------------------

    define_test "zk_dump should work" do
      assert_not_nil(admin.zk_dump)
    end

    #-------------------------------------------------------------------------------

    define_test "create should fail with non-string table names" do
      assert_raise(ArgumentError) do
        admin.create(123, 'xxx')
      end
    end

    define_test "create should fail with non-string/non-hash column args" do
      assert_raise(ArgumentError) do
        admin.create(@create_test_name, 123)
      end
    end

    define_test "create should fail without columns" do
      if admin.exists?(@create_test_name)
        admin.disable(@create_test_name)
        admin.drop(@create_test_name)
      end
      assert_raise(ArgumentError) do
        admin.create(@create_test_name)
      end
    end

    define_test "should work with string column args" do
      if admin.exists?(@create_test_name)
        admin.disable(@create_test_name)
        admin.drop(@create_test_name)
      end
      admin.create(@create_test_name, 'a', 'b')
      assert_equal(['a:', 'b:'], table(@create_test_name).get_all_columns.sort)
     end

    define_test "should work with hash column args" do
      if admin.exists?(@create_test_name)
        admin.disable(@create_test_name)
        admin.drop(@create_test_name)
      end
      admin.create(@create_test_name, { NAME => 'a'}, { NAME => 'b'})
      assert_equal(['a:', 'b:'], table(@create_test_name).get_all_columns.sort)
    end
  end
end
