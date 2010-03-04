module Shell
  module Commands
    class Get < Command
      def help
        return <<-EOF
          Get row or cell contents; pass table name, row, and optionally
          a dictionary of column(s), timestamp and versions.  Examples:

            hbase> get 't1', 'r1'
            hbase> get 't1', 'r1', {COLUMN => 'c1'}
            hbase> get 't1', 'r1', {COLUMN => ['c1', 'c2', 'c3']}
            hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1}
            hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, VERSIONS => 4}
        EOF
      end

      def command(table, row, args = {})
        table(table).get(row, args)
      end
    end
  end
end
