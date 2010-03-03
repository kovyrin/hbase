namespace :test do
  task :shell do
    test_files = FileList['lib/ruby/hbase/test/**/*_test.rb']
    sh("./bin/hbase org.jruby.Main bin/ruby_test_runner.rb #{test_files}")
  end
end

task :default => 'test:shell'
