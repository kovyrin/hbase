module Shell
  module Commands
    module Admin
      def admin()
        @admin = HBase::Admin.new(@configuration, @formatter) unless @admin
        @admin
      end

      def shutdown
        admin().shutdown()
      end

      def list
        admin().list()
      end

      def describe(table)
        admin().describe(table)
      end

      def enable(table)
        admin().enable(table)
      end

      def disable(table)
        admin().disable(table)
      end

      def enable_region(regionName)
        admin().enable_region(regionName)
      end

      def disable_region(regionName)
        admin().disable_region(regionName)
      end

      def exists(table)
        admin().exists(table)
      end

      def truncate(table)
        admin().truncate(table)
      end

      def close_region(regionName, server = nil)
        admin().close_region(regionName, server)
      end

      def status(format = 'summary')
        admin().status(format)
      end

      def zk(*args)
        admin().zk(args)
      end

      def zk_dump
        admin().zk_dump
      end
    end
  end
end
