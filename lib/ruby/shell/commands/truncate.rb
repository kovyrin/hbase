module Shell
  module Commands
    class Truncate < Command
      def help
        return <<-EOF
          Disables, drops and recreates the specified table.
        EOF
      end

      def command(table)
        admin.truncate(table)
      end
    end
  end
end
