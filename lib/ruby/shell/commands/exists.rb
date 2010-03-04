module Shell
  module Commands
    class Exists < Command
      def help
        return <<-EOF
          Does the named table exist? e.g. "hbase> exists 't1'"
        EOF
      end

      def command(table)
        admin.exists(table)
      end
    end
  end
end
