module Shell
  module Commands
    module DDL
      def table(table)
        # Create new one each time
        HBase::Table.new(@configuration, table, @formatter)
      end

      def create(table, *args)
        admin().create(table, args)
      end

      def drop(table)
        admin().drop(table)
      end

      def alter(table, args)
        admin().alter(table, args)
      end
    end
  end
end
