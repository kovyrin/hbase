# File passed to org.jruby.Main by bin/hbase.  Pollutes jirb with hbase imports
# and hbase  commands and then loads jirb.  Outputs a banner that tells user
# where to find help, shell version, and loads up a custom hirb.

# TODO: Add 'debug' support (client-side logs show in shell).  Add it as
# command-line option and as command.
# TODO: Interrupt a table creation or a connection to a bad master.  Currently
# has to time out.  Below we've set down the retries for rpc and hbase but
# still can be annoying (And there seem to be times when we'll retry for
# ever regardless)
# TODO: Add support for listing and manipulating catalog tables, etc.
# TODO: Encoding; need to know how to go from ruby String to UTF-8 bytes

# Run the java magic include and import basic HBase types that will help ease
# hbase hacking.
include Java

# Some goodies for hirb. Should these be left up to the user's discretion?
require 'irb/completion'

# Add the $HBASE_HOME/lib/ruby directory to the ruby
# load path so I can load up my HBase ruby modules
$LOAD_PATH.unshift File.join(File.dirname(File.dirname(__FILE__)), "lib", "ruby")

# Require formatter
require 'shell/formatter'

# See if there are args for this shell. If any, read and then strip from ARGV
# so they don't go through to irb.  Output shell 'usage' if user types '--help'
cmdline_help = <<HERE # HERE document output as shell usage
HBase Shell command-line options:
 format        Formatter for outputting results: console | html. Default: console
 format-width  Width of table outputs. Default: 110 characters.
 -d | --debug  Set DEBUG log levels.
HERE
found = []
format = 'console'
format_width = 110
script2run = nil
log_level = org.apache.log4j.Level::ERROR
for arg in ARGV
  if arg =~ /^--format=(.+)/i
    format = $1
    if format =~ /^html$/i
      raise NoMethodError.new("Not yet implemented")
    elsif format =~ /^console$/i
      # This is default
    else
      raise ArgumentError.new("Unsupported format " + arg)
    end
    found.push(arg)
  elsif arg =~ /^--format-width=(.+)/i
    format_width = $1.to_i
    found.push(arg)
  elsif arg == '-h' || arg == '--help'
    puts cmdline_help
    exit
  elsif arg == '-d' || arg == '--debug'
    log_level = org.apache.log4j.Level::DEBUG
    $fullBackTrace = true
    puts "Setting DEBUG log level..."
  else
    # Presume it a script. Save it off for running later below
    # after we've set up some environment.
    script2run = arg
    found.push(arg)
    # Presume that any other args are meant for the script.
    break
  end
end
for arg in found
  ARGV.delete(arg)
end

# Presume console format.
# Formatter takes an :output_stream parameter, if you don't want STDOUT.
@formatter = Formatter::Console.new(:format_width => format_width)
# TODO, etc.  @formatter = Formatter::XHTML.new(STDOUT)

# Set logging level to avoid verboseness
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level);
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level);

# Require HBase now after setting log levels
require 'hbase'

# Setup the HBase module.  Create a configuration.
# Turn off retries in hbase and ipc.  Human doesn't want to wait on N retries.
@configuration = org.apache.hadoop.hbase.HBaseConfiguration.new()
@configuration.setInt("hbase.client.retries.number", 7)
@configuration.setInt("ipc.client.connect.max.retries", 3)

# Do lazy create of admin because if we are pointed at bad master, it will hang
# shell on startup trying to connect.
@admin = nil

# Promote hbase constants to be constants of this module so can
# be used bare as keys in 'create', 'alter', etc. To see constants
# in IRB, type 'Object.constants'. Don't promote defaults because
# flattens all types to String.  Can be confusing.
def promote_constants(constants)
  # The constants to import are all in uppercase
  constants.each do |c|
    next if c =~ /DEFAULT_.*/ || c != c.upcase
    next if eval("defined?(#{c})")
    eval("#{c} = '#{c}'")
  end
end

promote_constants(org.apache.hadoop.hbase.HColumnDescriptor.constants)
promote_constants(org.apache.hadoop.hbase.HTableDescriptor.constants)
promote_constants(HBase.constants)

# Load all the hbase shell commands
require 'shell'

# If script2run, try running it.  Will go on to run the shell unless
# script calls 'exit' or 'exit 0' or 'exit errcode'.
load(script2run) if script2run

# Output a banner message that tells users where to go for help
print_banner

require "irb"

module IRB
  # Subclass of IRB so can intercept methods
  class HIRB < Irb
    def initialize
      # This is ugly.  Our 'help' method above provokes the following message
      # on irb construction: 'irb: warn: can't alias help from irb_help.'
      # Below, we reset the output so its pointed at /dev/null during irb
      # construction just so this message does not come out after we emit
      # the banner.  Other attempts at playing with the hash of methods
      # down in IRB didn't seem to work. I think the worst thing that can
      # happen is the shell exiting because of failed IRB construction with
      # no error (though we're not blanking STDERR)
      begin
        f = File.open("/dev/null", "w")
        $stdout = f
        super
      ensure
        f.close()
        $stdout = STDOUT
      end
    end

    def output_value
      # Suppress output if last_value is 'nil'
      # Otherwise, when user types help, get ugly 'nil'
      # after all output.
      if @context.last_value != nil
        super
      end
    end
  end

  def IRB.start(ap_path = nil)
    $0 = File::basename(ap_path, ".rb") if ap_path

    IRB.setup(ap_path)
    @CONF[:IRB_NAME] = 'hbase'
    @CONF[:AP_NAME] = 'hbase'
    @CONF[:BACK_TRACE_LIMIT] = 0 unless $fullBackTrace

    if @CONF[:SCRIPT]
      hirb = HIRB.new(nil, @CONF[:SCRIPT])
    else
      hirb = HIRB.new
    end

    @CONF[:IRB_RC].call(hirb.context) if @CONF[:IRB_RC]
    @CONF[:MAIN_CONTEXT] = hirb.context

    catch(:IRB_EXIT) do
      hirb.eval_input
    end
  end
end

IRB.start
