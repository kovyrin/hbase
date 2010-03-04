module Shell
  module Commands
    class Command
      attr_accessor :shell

      def initialize(shell)
        self.shell = shell
      end

      def command_safe(*args)
        command(*args)
      rescue ArgumentError => e
        puts
        puts "ERROR: #{e}"
        puts
        puts "Here is some help for this command:"
        puts help
        puts
      ensure
        return nil
      end

      def admin
        shell.hbase.admin
      end

      def table
        shell.hbase.table
      end

      def command
        raise "Please override command method in your command class!"
      end
    end
  end
end
