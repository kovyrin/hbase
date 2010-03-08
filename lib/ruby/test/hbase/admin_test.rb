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


  end
end
