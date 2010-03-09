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
      # Create the table if needed
      unless admin.exists?(name)
        admin.create name, [{'NAME' => 'x', 'VERSIONS' => 5}, 'y']
        return
      end

      # Enable the table if needed
      unless admin.enabled?(name)
        admin.enable(name)
      end
    end

    def drop_test_table(name)
      return unless admin.exists?(name)
      admin.disable(name) if admin.enabled?(name)
      admin.drop(name)
    end
  end
end
