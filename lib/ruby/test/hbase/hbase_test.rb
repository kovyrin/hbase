require 'test/test_helper'
require 'hbase'
require 'shell/formatter'

module Hbase
  class HbaseTest < Test::Unit::TestCase
    def setup
      @formatter = Shell::Formatter::Console.new(:format_width => 110)
    end

    define_test "should pass" do
      assert(true)
    end
  end
end
