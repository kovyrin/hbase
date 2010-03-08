module Hbase
  module TestHelpers
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
      admin.enable(name) unless admin.enabled?(name)
    end
  end
end
