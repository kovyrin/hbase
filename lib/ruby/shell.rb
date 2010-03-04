require 'shell/help'
require 'shell/commands/crud'
require 'shell/commands/admin'
require 'shell/commands/ddl'

# Helper methods

# Help commands
include Shell::Help

# Other commands
include Shell::Commands::CRUD
include Shell::Commands::Admin
include Shell::Commands::DDL
