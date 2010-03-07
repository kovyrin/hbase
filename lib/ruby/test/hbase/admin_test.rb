require 'test/unit'
require 'hbase'
require 'shell/formatter'

module Hbase
  class AdminTest < Test::Unit::TestCase
    def setup
      @formatter = ::Shell::Formatter::Console.new(:format_width => 110)
    end

    def test_pass
      assert(true)
    end
  end
end
