module Shell
  module Commands
    class Command
      attr_accessor :shell

      def initialize(shell)
        self.shell = shell
      end

      def command_safe(debug, *args)
        command(*args)
      rescue => e
        puts
        puts "ERROR: #{e}"
        puts "Backtrace: #{e.backtrace.join("\n           ")}" if debug
        puts
        puts "Here is some help for this command:"
        puts help
        puts
      ensure
        return nil
      end

      def admin
        shell.hbase_admin
      end

      def table(name)
        shell.hbase_table(name)
      end

      #----------------------------------------------------------------------

      def formatter
        shell.formatter
      end

      def format_simple_command
        now = Time.now
        yield
        formatter.header
        formatter.footer(now)
      end

      def check_table(table)
        raise ArgumentError, "Unknown table #{table}!" unless admin.exists?(table)
      end
    end
  end
end
