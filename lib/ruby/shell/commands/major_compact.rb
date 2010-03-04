module Shell
  module Commands
    class MajorCompact < Command
      def help
        return <<-EOF
          Run major compaction on passed table or pass a region row
          to major compact an individual region
        EOF
      end

      def command(table_or_region_name)
        admin.major_compact(table_or_region_name)
      end
    end
  end
end
