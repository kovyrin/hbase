module Shell
  module Commands
    class Describe < Command
      def help
        return <<-EOF
          Describe the named table. For example:
            hbase> describe 't1'
        EOF
      end

      def command(table)
        admin.describe(table)
      end
    end
  end
end
