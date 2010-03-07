require 'test/unit'

module Testing
  module Declarative
    # define_test "should do something" do
    #   ...
    # end
    def define_test(name, &block)
      test_name = "test_#{name.gsub(/\s+/,'_')}".to_sym
      defined = instance_method(test_name) rescue false
      raise "#{test_name} is already defined in #{self}" if defined
      if block_given?
        define_method(test_name, &block)
      else
        define_method(test_name) do
          flunk "No implementation provided for #{name}"
        end
      end
    end
  end
end

# Extend standard unit tests with our helpers
Test::Unit::TestCase.extend(Testing::Declarative)

# Add the $HBASE_HOME/lib/ruby directory to the ruby
# load path so I can load up my HBase ruby modules
$LOAD_PATH.unshift File.dirname(File.dirname(__FILE__))

