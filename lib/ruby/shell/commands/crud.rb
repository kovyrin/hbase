module Shell
  module Commands
    module CRUD
      def get(table, row, args = {})
        table(table).get(row, args)
      end

      def put(table, row, column, value, timestamp = nil)
        table(table).put(row, column, value, timestamp)
      end

      def incr(table, row, column, value = nil)
        table(table).incr(row, column, value)
      end

      def scan(table, args = {})
        table(table).scan(args)
      end

      def delete(table, row, column,
          timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
        table(table).delete(row, column, timestamp)
      end

      def deleteall(table, row, column = nil,
          timestamp = org.apache.hadoop.hbase.HConstants::LATEST_TIMESTAMP)
        table(table).deleteall(row, column, timestamp)
      end

      def count(table, interval = 1000)
        table(table).count(interval)
      end

      def flush(table_or_region_name)
        admin().flush(table_or_region_name)
      end

      def compact(table_or_region_name)
        admin().compact(table_or_region_name)
      end

      def major_compact(table_or_region_name)
        admin().major_compact(table_or_region_name)
      end

      def split(table_or_region_name)
        admin().split(table_or_region_name)
      end
    end
  end
end
