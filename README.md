allplayers_imports.rb
=====================

Ruby tool that parses data and imports into AllPlayers.com via AllPlayers public API.

Currently works with gdata (Google Spreadsheet API) but can also accept a parsed csv as input.

To install, run <code>gem install allplayers_imports</code>

Extend your allplayers object to include AllPlayersImports

Example:
```
include 'allplayers'
include 'allplayers_imports'

allplayers_session = AllPlayers::Client.new(nil, 'www.allplayers.com')
allplayers_session.add_headers({:Authorization => 'Basic ' + Base64.encode64(user + ':' + pass)})
allplayers_session.extend AllPlayersImports

allplayers_session.import_sheet(spreadsheet, 'Groups or Participant Information')
```
