module Shell
  module Commands
    class Disable < Command
      def help
        return <<-EOF
          Disable the named table: e.g. "hbase> disable 't1'"
        EOF
      end

      def command(table)
        check_table(table)
        format_simple_command do
          admin.disable(table)
        end
      end
    end
  end
end
