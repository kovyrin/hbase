module Shell
  module Commands
    class Split < Command
      def help
        return <<-EOF
          Split table or pass a region row to split individual region
        EOF
      end

      def command(table_or_region_name)
        admin.split(table_or_region_name)
      end
    end
  end
end
