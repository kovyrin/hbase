module Shell
  module Commands
    class Disable < Command
      def help
        return <<-EOF
          Disable the named table: e.g. "hbase> disable 't1'"
        EOF
      end

      def command(table)
        admin.disable(table)
      end
    end
  end
end
