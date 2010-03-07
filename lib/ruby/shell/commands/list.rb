module Shell
  module Commands
    class List < Command
      def help
        return <<-EOF
          List all tables in hbase
        EOF
      end

      def command
        admin.list
      end
    end
  end
end
