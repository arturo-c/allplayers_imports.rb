# Provides ImportActions using AllPlayers API.

require 'rubygems'
require 'highline/import'
require 'active_support'
require 'active_support/core_ext/time/conversions.rb'
require 'thread'
require 'logger'
require 'resolv'
require 'date'
require 'faster_csv'

# Stop EOF errors in Highline
HighLine.track_eof = false

class DuplicateUserExists < StandardError
end

# Add some tools to Array to make parsing spreadsheet rows easier.
class Array
  # Little utility to convert array to Hash with defined keys.
  def to_hash(other)
    Hash[ *(0...other.size()).inject([]) { |arr, ix| arr.push(other[ix], self[ix]) } ]
  end
  # Split off first element in each array item, after splitting by pattern, then
  # strip trailing and preceding whitespaces.
  def split_first(pattern)
    arr = []
    self.each do | item |
      arr.push(item.split(pattern)[0].strip)
    end
    arr
  end
  def downcase
    arr = []
    self.each do |item|
      arr.push(item.downcase)
    end
    arr
  end
  def gsub(pattern,replacement)
    arr = []
    self.each do |item|
      arr.push(item.gsub(pattern,replacement))
    end
    arr
  end
end

class Hash
  def key_filter(pattern, replacement = '')
    hsh = {}
    filtered = self.reject { |key,value| key.match(pattern).nil? }
    filtered.each { |key,value| hsh[key.sub(pattern, replacement)] = value }
    hsh
  end
end

class Date
  def to_age
    now = Time.now.utc.to_date
    now.year - self.year - ((now.month > self.month || (now.month == self.month && now.day >= self.day)) ? 0 : 1)
  end
end

# valid_email_address port from Drupal
class String
  def valid_email_address?
    return !self.match(/^[a-zA-Z0-9_\-\.\+\^!#\$%&*+\/\=\?\`\|\{\}~\']+@((?:(?:[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.?)+|(\[([0-9]{1,3}(\.[0-9]{1,3}){3}|[0-9a-fA-F]{1,4}(\:[0-9a-fA-F]{1,4}){7})\]))$/).nil?
  end
  def active_email_domain?
      domain = self.match(/\@(.+)/)[1]
      Resolv::DNS.open do |dns|
          @mx = dns.getresources(domain, Resolv::DNS::Resource::IN::MX)
          @a = dns.getresources(domain, Resolv::DNS::Resource::IN::A)
      end
      @mx.size > 0 || @a.size > 0
  end
end

# Write the correct header for the csv log
class ApciLogDevice < Logger::LogDevice
  private
  def add_log_header(file)
      file.write(
     	"\"Severity\",\"Date\",\"Severity (Full)\",\"Row\",\"Info\"\n"
    )
  end
end

# Build a Logger::Formatter subclass.
class ApciFormatter < Logger::Formatter
  Format = "\"%s\",\"[%s#%d]\",\"%5s\",\"%s\",\"%s\"\n"
  def initialize
    @highline = HighLine.new
    super
  end
  # Provide a call() method that returns the formatted message.
  def call(severity, time, program_name, message)
    message_color =  severity == 'ERROR' ? @highline.color(message, :red, :bold) : message
    message_color =  severity == 'WARN' ? @highline.color(message, :bold) : message_color
    if program_name == program_name.to_i.to_s
      # Abuse program_name as row #
      if program_name.to_i.even?
        say @highline.color('Row ' + program_name + ': ', :cyan, :bold) + message_color
      else
        say @highline.color('Row ' + program_name + ': ', :magenta, :bold) + message_color
      end
    else
      say message_color
    end
    message.gsub!('"', "'")
    Format % [severity[0..0], format_datetime(time), $$, severity, program_name,
        msg2str(message)]
  end
end

# Functions to aid importing any type of spreadsheet to Allplayers.com.
module AllPlayersImports
  @@sheet_mutex = Mutex.new
  @@stats_mutex = Mutex.new
  @@user_mutex = Mutex.new
  @@email_mutexes = {}

  # Static UID to email
  @@uuid_map = {}
  # Statistics about operations performed
  @@stats = {}

  # Cache and honor locks on email to UID req's.
  def email_to_uuid(email, action = nil)
    @@user_mutex.synchronize do
      # If we've cached it, short circuit.
      return @@uuid_map[email] if @@uuid_map.has_key?(email)
      # Haven't cached it, create a targeted Mutex for it.
      # Changed to using monitor, see http://www.velocityreviews.com/forums/t857319-thread-and-mutex-in-ruby-1-8-7-a.html
      @@email_mutexes[email] = Monitor.new unless @@email_mutexes.has_key?(email)
    end

    user = nil
    # Try to get a targeted lock.
    @@email_mutexes[email].synchronize {
      # Got the lock, short circuit if another thread found our UID.
      return @@uuid_map[email] if @@uuid_map.has_key?(email)
      user = self.user_get_email(email)
      @@uuid_map[email] = user['uuid'] if user.include?('uuid')
    }
    # Caller wants the lock while it tries to generate a user.
    return user['uuid'], @@email_mutexes[email] if action == :lock
    user['uuid']
  end

  def verify_children(row, description = 'User', matched_uuid = nil)
    # Fields to match
    import = row.reject {|k,v| k != 'first_name' && k != 'last_name'}
    prefixes = ['parent_1_', 'parent_2_']
    matched_parents = []
    ret = nil
    prefixes.each {|prefix|
      parent_description = prefix.split('_').join(' ').strip.capitalize
      if row.has_key?(prefix + 'uuid')
        children = self.user_children_list(row[prefix + 'uuid']['item'].first['uuid'])
        next if children.nil? || children.length <= 0
        children['item'].each do |child|
          kid = self.user_get(child['uuid'])
          next if kid['firstname'].nil?
          if (matched_uuid.nil? || matched_uuid != child['uuid'])
            system = {}
            system['first_name'] = kid['firstname'].downcase if kid.has_key?('firstname')
            system['last_name'] = kid['lastname'].downcase if kid.has_key?('lastname')
            import['first_name'] = import['first_name'].downcase
            import['last_name'] = import['last_name'].downcase
            if (system != import)
              # Keep looking
              next
            end
            # Found it
            @logger.info(get_row_count.to_s) {parent_description + ' has matching child: ' + description + ' ' + row['first_name'] + ' ' + row['last_name']} if ret.nil?
            if matched_uuid.nil?
              matched_uuid = child['uuid']
            end
            if !child.nil?
              ret = {'mail' => kid['email'], 'uuid' => matched_uuid }
            end
            matched_parents.push(prefix)
            break
          end
        end
      end
    }
    # Add existing child to other parent if needed.
    unless matched_uuid.nil?
      prefixes.each {|prefix|
        parent_description = prefix.split('_').join(' ').strip.capitalize
        if row.has_key?(prefix + 'uuid') && !matched_parents.include?(prefix)
          @logger.info(get_row_count.to_s) {'Adding existing child, ' + description + ' ' + row['first_name'] + ' ' + row['last_name'] + ' has matching child : ' + parent_description}
          self.user_create_child(row[prefix + 'uuid']['item'].first['uuid'], '', '', '', '', {:child_uuid => matched_uuid})
        end
      }
    end

    return ret
  end

  def get_group_names_from_file
    groups_uuids = {}
    if FileTest.exist?("imported_groups.csv")
      FasterCSV.foreach("imported_groups.csv") do |row|
        groups_uuids[row[1]] = row[2]
      end
    end
    groups_uuids
  end

  def get_group_rows_from_file
    groups_uuids = {}
    if FileTest.exist?("imported_groups.csv")
      FasterCSV.foreach("imported_groups.csv") do |row|
        groups_uuids[row[0]] = row[2]
      end
    end
    groups_uuids
  end

  def prepare_row(row_array, column_defs, row_count = nil)
    if row_count
      set_row_count(row_count)
    else
      increment_row_count
    end
    row = row_array.to_hash(column_defs)
    # Convert everything to a string and strip whitespace.
    row.each { |key,value| row.store(key,value.to_s.strip)}
    # Delete empty values.
    row.delete_if { |key,value| value.empty? }
  end

  def get_row_count
    Thread.current['row_count'] = 0 if Thread.current['row_count'].nil?
    Thread.current['row_count']
  end

  def increment_row_count
    set_row_count(get_row_count + 1)
  end

  def set_row_count(count)
    Thread.current['row_count'] = count
  end

  def increment_stat(type)
    @@stats_mutex.synchronize do
      if @@stats.has_key?(type)
        @@stats[type]+=1
      else
        @@stats[type] = 1
      end
    end
  end

  def import_sheet(sheet, name, g = nil, wuri = nil, run_character = nil, skip_emails = nil)
    if skip_emails.nil?
      self.remove_headers({:NOTIFICATION_BYPASS => nil, :API_USER_AGENT => nil})
    else
      self.add_headers({:NOTIFICATION_BYPASS => 1, :API_USER_AGENT => 'AllPlayers-Import-Client'})
    end

    run_char = run_character
    run_char = $run_character unless $run_character.nil?
    rerun_sheet = []
    rerun_row_count = {}
    start_time = Time.now
    @logger.debug('import') {'Started ' + start_time.to_s}

    set_row_count(0)
    increment_row_count
    # Pull the first row and chunk it, it's just extended field descriptions.
    @logger.info(get_row_count.to_s) {"Skipping Descriptions"}
    sheet.shift

    # Pull the second row and use it to define columns.
    increment_row_count
    @logger.info(get_row_count.to_s) {"Parsing column labels"}
    begin
      column_defs = sheet.shift.split_first("\n").gsub(/[^0-9a-z]/i, '_').downcase
    rescue
      @logger.info(get_row_count.to_s) {"Error parsing column labels"}
      return
    end

    if $skip_rows
      @logger.info(get_row_count.to_s) {'Skipping ' + $skip_rows.to_s + ' rows'}
      while get_row_count < $skip_rows do
        sheet.shift
        increment_row_count
      end
      @logger.debug(get_row_count.to_s) {'Skipped ' + $skip_rows.to_s + ' rows'}
    end

    row_count = get_row_count
    # TODO - Detect sheet type / sanity check by searching column_defs
    if (name == 'Participant Information')
      # mixed sheet... FUN!
      @logger.info(get_row_count.to_s) {"Importing Participants, Parents and Group assignments\n"}
      # Multi-thread
      threads = []
      # Set default thread_count to 7, accept global to change it.
      thread_count = $thread_count.nil? ? 7 : $thread_count

      for i in 0..thread_count do
        threads << Thread.new {
           until sheet.nil?
             row = nil
             @@sheet_mutex.synchronize do
               row = sheet.shift
               row_count+=1
             end
             unless row.nil?
               formatted_row = self.prepare_row(row, column_defs, row_count)
               if run_char.nil?
                 self.import_mixed_user(formatted_row)
               else
                 if formatted_row['run_character'].to_s == run_char.to_s
                   self.import_mixed_user(formatted_row)
                 else
                   @logger.info(get_row_count.to_s) {'Skipping row ' + row_count.to_s}
                 end
               end
             else
               break
             end
           end
        }
      end
      threads.each_index {|i|
        threads[i].join
        puts 'Thread ' + i.to_s + ' exited.'
      }
    elsif (name == 'Users')
      #if (2 <= (column_defs & ['First Name', 'Last Name']).length)
      @logger.info(get_row_count.to_s) {"Importing Users\n"}
      sheet.each {|row| self.import_user(self.prepare_row(row, column_defs))}
    elsif (name == 'Groups' || name == 'Group Information' || name == 'Duplicates')
      #elsif (2 <= (column_defs & ['Group Name', 'Category']).length)
      @logger.info(get_row_count.to_s) {"Importing Groups\n"}

      # Multi-thread
      threads = []
      # Set default thread_count to 5, accept global to change it.
      thread_count = $thread_count.nil? ? 5 : $thread_count
      for i in 0..thread_count do
        threads << Thread.new {
           until sheet.nil?
             row = nil
             @@sheet_mutex.synchronize do
               row = sheet.shift
               row_count+=1
             end
             unless row.nil?
               formatted_row = self.prepare_row(row, column_defs, row_count)
               if run_char.nil?
                 self.import_group(formatted_row)
               else
                 if formatted_row['run_character'].to_s == run_char.to_s
                   title = self.import_group(formatted_row)
                   if title == formatted_row['group_name']
                     rerun_sheet.push(row) if title == formatted_row['group_name']
                     rerun_row_count = rerun_row_count.merge(title => get_row_count)
                   end
                 else
                   @logger.info(get_row_count.to_s) {'Skipping row ' + row_count.to_s}
                 end
               end
             else
               break
             end
           end
        }
      end
      threads.each_index {|i|
        threads[i].join
        puts 'Thread ' + i.to_s + ' exited.'
      }
      # Retrying rows that didn't find group above.
      rerun_sheet.each {|row|
        formatted_row = self.prepare_row(row, column_defs)
        set_row_count(rerun_row_count[formatted_row['group_name']])
        self.import_group(formatted_row)
      }
    elsif (name == 'Events')
      #elsif (2 <= (column_defs & ['Title', 'Groups Involved', 'Duration (in minutes)']).length)
      @logger.info(get_row_count.to_s) {"Importing Events\n"}
      sheet.each {|row|
        row_count+=1
        response = self.import_event(self.prepare_row(row, column_defs))
        unless g.nil? || wuri.nil?
          g.put_cell_content(wuri.to_s+'/R'+row_count.to_s+'C6', response['nid'], row_count, 6) if response != 'update'
        end
      }
    elsif (name == 'Users in Groups')
      #elsif (2 <= (column_defs & ['Group Name', 'User email', 'Role (Admin, Coach, Player, etc)']).length)
      @logger.info(get_row_count.to_s) {"Importing Users in Groups\n"}
      sheet.each {|row| self.import_user_group_role(self.prepare_row(row, column_defs))}
    else
      @logger.info(get_row_count.to_s) {"Don't know what to do with sheet " + name + "\n"}
      next # Go to the next sheet.
    end
    # Output stats
    seconds = (Time.now - start_time).to_i
    @logger.debug('import') {' stopped ' + Time.now.to_s}
    stats_array = []
    @@stats.each { |key,value| stats_array.push(key.to_s + ': ' + value.to_s) unless value.nil? or value == 0}
    puts
    puts
    @logger.info('import') {'Imported ' + stats_array.sort.join(', ')}
    @logger.info('import') {' in ' + (seconds / 60).to_s + ' minutes ' + (seconds % 60).to_s + ' seconds.'}
    puts
    # End stats
  end

  def import_mixed_user(row)
    @logger.info(get_row_count.to_s) {'Processing...'}
    # Import Users (Make sure parents come first).
    responses = {}
    ['parent_1_', 'parent_2_',  'participant_'].each {|prefix|
      user = row.key_filter(prefix)
      # Add in Parent email addresses if this is the participant.
      user.merge!(row.reject {|key, value|  !key.include?('email_address')}) if prefix == 'participant_'
      description = prefix.split('_').join(' ').strip.capitalize

      responses[prefix] = import_user(user, description) unless user.empty?
      if !responses[prefix].respond_to?(:has_key?)
        responses[prefix] = {}
      end
    }

    if responses.has_key?('participant_') && !responses['participant_'].nil?
      # Update participant with responses.  We're done with parents.
      row['participant_uuid'] = responses['participant_']['uuid'] if responses['participant_'].has_key?('uuid')
      row['participant_email_address'] = responses['participant_']['mail'] if responses['participant_'].has_key?('mail')

      # Find the max number of groups being imported
      group_list = row.reject {|key, value| key.match('group_').nil?}
      number_of_groups = 0
      key_int_value = 0
      group_list.each {|key, value|
        key_parts = key.split('_')
        key_parts.each {|part|
          key_int_value = part.to_i
          if (key_int_value > number_of_groups)
            number_of_groups = key_int_value
          end
        }
      }

      # Create the list of group names to iterate through
      group_names = []
      for i in 1..number_of_groups
        group_names.push('group_' + i.to_s + '_')
      end

      # Group Assignment + Participant
      group_names.each {|prefix|
        group = row.key_filter(prefix, 'group_')
        user = row.key_filter('participant_')
        responses[prefix] = import_user_group_role(user.merge(group)) unless group.empty?
      }
    end
  end

  def import_user(row, description = 'User')
    more_params = {}

    if row['birthdate'].nil?
      @logger.error(get_row_count.to_s) {'No Birth Date Listed.  Failed to import ' + description + '.'}
      return {}
    else
      begin
        birthdate = Date.parse(row['birthdate'])
      rescue ArgumentError => err
        @logger.error(get_row_count.to_s) {'Invalid Birth Date.  Failed to import ' + description + '.'}
        @logger.error(get_row_count.to_s) {err.message.to_s}
        return {}
      end
    end

    ('1'..'2').each { |i|
      key = 'parent_' + i + '_email_address'
      if row.has_key?(key)
        parent_uuid = nil
        begin
          parent_uuid = self.user_get_email(row[key])
        rescue DuplicateUserExists => dup_e
          @logger.error(get_row_count.to_s) {'Parent ' + i + ' ' + dup_e.message.to_s}
        end
        if parent_uuid.nil?
          @logger.warn(get_row_count.to_s) {"Can't find account for Parent " + i + ": " + row[key]}
        else
          row['parent_' + i + '_uuid'] = parent_uuid
        end
      end
    }

    # If 13 or under, verify parent, request allplayers.net email if needed.
    if birthdate.to_age < 14
      # If 13 or under, no email  & has parent, request allplayers.net email.
      if !(row.has_key?('parent_1_uuid') || row.has_key?('parent_2_uuid'))
        @logger.error(get_row_count.to_s) {'Missing parents for '+ description +' age 13 or less.'}
        return {}
      end
    end

    lock = nil
    # Request allplayers.net email if needed.
    if !row.has_key?('email_address')
      # If 13 or under, no email  & has parent, request allplayers.net email.
      if row.has_key?('parent_1_uuid') || row.has_key?('parent_2_uuid')
        # Request allplayers.net email
        more_params['email_alternative'] = {:value => 1}
        # TODO - Consider how to send welcome email to parent. (Queue allplayers.net emails in Drupal for cron playback)
        # Create a lock for these parents
        @@user_mutex.synchronize do
          parent_uuids = []
          parent_uuids.push(row['parent_1_uuid']['item'].first['uuid']) if row.has_key?('parent_1_uuid')
          parent_uuids.push(row['parent_2_uuid']['item'].first['uuid']) if row.has_key?('parent_2_uuid')
          parents_key = parent_uuids.sort.join('_')
          # Haven't cached it, create a targeted Mutex for it.
          @@email_mutexes[parents_key] = Mutex.new unless @@email_mutexes.has_key?(parents_key)
          lock = @@email_mutexes[parents_key]
        end
      else
        @logger.error(get_row_count.to_s) {'Missing parents for '+ description +' without email address.'}
        return {}
      end
    else
      # Check if user already
      begin
        uuid, lock = email_to_uuid(row['email_address'], :lock)
      rescue DuplicateUserExists => dup_e
        @logger.error(get_row_count.to_s) {description + ' ' + dup_e.message.to_s}
        return {}
      end

      unless uuid.nil?
        @logger.warn(get_row_count.to_s) {description + ' already exists: ' + row['email_address'] + ' at UUID: ' + uuid + '. Participant will still be added to groups.'}
        self.verify_children(row, description, uuid)
        return {'mail' => row['email_address'], 'uuid' => uuid}
      else
        if !row['email_address'].valid_email_address?
          @logger.error(get_row_count.to_s) {description + ' has an invalid email address: ' + row['email_address'] + '. Skipping.'}
          return {}
        end
        if !row['email_address'].active_email_domain?
          @logger.error(get_row_count.to_s) {description + ' has an email address with an invalid or inactive domain: ' + row['email_address'] + '. Skipping.'}
          return {}
        end
      end
    end

    # Check required fields
    missing_fields = ['first_name', 'last_name', 'gender', 'birthdate'].reject {
      |field| row.has_key?(field) && !row[field].nil? && !row[field].empty?
    }
    if !missing_fields.empty?
      @logger.error(get_row_count.to_s) {'Missing required fields for '+ description +': ' + missing_fields.join(', ')}
      return {}
    end

    @logger.info(get_row_count.to_s) {'Importing ' + description +': ' + row['first_name'] + ' ' + row['last_name']}

    response = {}

    # Lock down this email address.
    lock.synchronize {
      # Last minute checks.
      if !row['email_address'].nil? && @@uuid_map.has_key?(row['email_address'])
        @logger.warn(get_row_count.to_s) {description + ' already exists: ' + row['email_address'] + ' at UUID: ' + @@uuid_map[row['email_address']] + '. Participant will still be added to groups.'}
        return {'mail' => row['email_address'], 'uuid' => @@uuid_map[row['email_address']] }
      end

      # Avoid creating duplicate children.
      existing_child = self.verify_children(row, description)
      return existing_child unless existing_child.nil?
      if row.has_key?('email_address') && row.has_key?('parent_1_uuid')
        more_params['email'] = row['email_address']
      end
      if row.has_key?('parent_1_uuid')
        response = self.user_create_child(
          row['parent_1_uuid']['item'].first['uuid'],
          row['first_name'],
          row['last_name'],
          birthdate,
          row['gender'],
          more_params
        )
      else
        response = self.user_create(
          row['email_address'],
          row['first_name'],
          row['last_name'],
          row['gender'],
          birthdate,
          more_params
        )
      end

      if !response.nil?  && response.has_key?('uuid')
        # Cache the new users UID while we have the lock.
        @@user_mutex.synchronize { @@uuid_map[response['email']] = response['uuid'] }
      end
    }

    if !response.nil?  && response.has_key?('uuid')
      increment_stat('Users')
      increment_stat(description + 's') if description != 'User'

      # Don't add parent 1, already added with user_create_child.
      response['parenting_2_response'] = self.user_create_child(row['parent_2_uuid']['item'].first['uuid'], '', '', '', '', {:child_uuid => response['uuid']}) if row.has_key?('parent_2_uuid')
    end

    return response
  rescue RestClient::Exception => e
    @logger.error(get_row_count.to_s) {'Failed to import ' + description + ': ' + e.message}
  end

  def import_group(row)
    @groups_map = get_group_names_from_file unless defined? @groups_map
    @group_rows = get_group_rows_from_file unless defined? @group_rows
    # Checking name duplication, if duplicate add identifier by type division, league, etc..
    if @group_rows.has_key?(get_row_count.to_s)
      row['uuid'] = @group_rows[get_row_count.to_s]
    end
    begin
      if row['delete']
        begin
          # Make sure registration settings are turned off by making group inactive.
          self.group_update(row['uuid'], {'active' => 0})
          self.group_delete(row['uuid'])
        rescue RestClient::Exception => e
          puts 'There was a problem deleting group:' + row['uuid']
          @logger.info(get_row_count.to_s) {'There was a problem deleting group:' + row['uuid'] + ' ' + e.message}
        else
          @logger.info(get_row_count.to_s) {'Deleting group:' + row['uuid']}
          puts 'Deleting group:' + row['uuid']
        end
        return
      end
      if row.has_key?('group_clone') && row.has_key?('uuid') && !row['group_clone'].empty? && !row['uuid'].empty?
        begin
          self.group_get(row['uuid'])
          self.group_get(row['group_clone'])
        rescue RestClient::Exception => e
          puts 'The group you are trying to clone from can not be found, moving on to creating the group.'
        else
          @logger.info(get_row_count.to_s) {'Cloning settings from group: ' + row['group_clone']}
          self.group_clone(row['uuid'], row['group_clone'])
          return
        end
      elsif row.has_key?('uuid')
        puts 'Group already imported.'
        @logger.info(get_row_count.to_s) {'Group already imported.'}
        return
      end
      if row['owner_uuid']
        begin
          owner = self.user_get(row['owner_uuid'])
          raise if !owner.has_key?('uuid')
        rescue
          puts "Couldn't get group owner from UUID: " + row['owner_uuid'].to_s
          return {}
        end
      else
        puts 'Group import requires group owner'
        return {}
      end
      location = row.key_filter('address_')
      if location['zip'].nil?
        @logger.error(get_row_count.to_s) {'Location ZIP required for group import.'}
        return {}
      end

      categories = row['group_categories'].split(',') unless row['group_categories'].nil?
      if categories.nil?
        @logger.error(get_row_count.to_s) {'Group Type required for group import.'}
        return {}
      end
      more_params = {}
      more_params['group_type'] = row['group_type'] unless row['group_type'].nil?

      # Checking name duplication, if duplicate add identifier by type division, league, etc..
      # Only one level deep.
      if @groups_map.has_key?(row['group_name'])
        if row['group_type'] == 'Club'
          if @groups_map.has_key?(row['group_name'] + ' Club')
            row['group_name'] = row['group_name'] + ' Club 1'
          else
            row['group_name'] = row['group_name'] + ' Club'
          end
        elsif row['group_type'] == 'Team'
          if @groups_map.has_key?(row['group_name'] + ' Team')
            if @groups_map.has_key(row['group_name'] + ' 1')
              row['group_above'] = row['group_name'] + ' Club 1'
              row['group_name'] = row['group_name'] + ' 2'
            else
              row['group_above'] = row['group_name'] + ' Club'
              row['group_name'] = row['group_name'] + ' 1'
            end
          else
            row['group_name'] = row['group_name'] + ' Team'
          end
        end
      end

      if row.has_key?('group_uuid') && !row['group_uuid'].empty?
        more_params['groups_above'] = {'0' => row['group_uuid']}
      elsif row.has_key?('group_above') && !row['group_above'].empty?
        if @groups_map.has_key?(row['group_above'])
          @logger.info(get_row_count.to_s) {'Found group above: ' + row['group_above'] + ' at UUID ' + @groups_map[row['group_above']]}
          more_params['groups_above'] = {@groups_map[row['group_above']] => @groups_map[row['group_above']]}
        else
          response = self.group_search({:title => row['group_above']})
          if response.kind_of?(Array)
            response.each { |group|
              if group['title'] == row['group_above']
                row['group_name'] = row['group_name'] + ' ' + row['group_type'] if group['title'] == row['group_name']
                more_params['groups_above'] = {group['uuid'] => group['uuid']}
              end
            }
            if more_params['groups_above'].nil?
              puts 'Row ' + get_row_count.to_s + "Couldn't find group above: " + row['group_above']
              @logger.error(get_row_count.to_s) {"Couldn't find group above: " + row['group_above']}
              return row['group_name']
            end
          else
            puts 'Row ' + get_row_count.to_s + "Couldn't find group above: " + row['group_above']
            @logger.error(get_row_count.to_s) {"Couldn't find group above: " + row['group_above']}
            return row['group_name']
          end
        end
      end

      @logger.info(get_row_count.to_s) {'Importing group: ' + row['group_name']}
      response = self.group_create(
        row['group_name'], # Title
        row['group_description'], # Description field
        location,
        categories.last,
        more_params
      )
      @logger.info(get_row_count.to_s) {'Group UUID: ' + response['uuid']}
    rescue RestClient::Exception => e
      @logger.error(get_row_count.to_s) {'Failed to import group: ' + e.message}
    else
      if (response && response.has_key?('uuid'))
        increment_stat('Groups')
        # Writing data into a csv file
        @groups_map[row['group_name']] = response['uuid']
        FasterCSV.open("imported_groups.csv", "a") do |csv|
           csv << [get_row_count, row['group_name'],response['uuid']]
        end
        if row.has_key?('group_clone') && !row['group_clone'].empty?
          @logger.info(get_row_count.to_s) {'Cloning settings from group: ' + row['group_clone']}
          response = self.group_clone(
            response['uuid'],
            row['group_clone'],
            nil
          )
        end
      end
    end
  end

  def import_user_group_role(row)
    # Check User.
    if row.has_key?('uuid')
      user = self.user_get(row['uuid'])
    elsif row.has_key?('email_address') && !row['email_address'].respond_to?(:to_s)
      begin
        user = self.user_get_email(row['email_address'])
      rescue
        @logger.error(get_row_count.to_s) {"User " + row['email_address'] + " doesn't exist to add to group"}
        return
      end
    else
      @logger.error(get_row_count.to_s) {"User can't be added to group without email address."}
      return
    end

    # Check Group
    if row.has_key?('group_uuid')
      group_uuid = row['group_uuid']
    else
      @logger.error(get_row_count.to_s) {'User ' + row['email_address'] + " can't be added to group without group uuid."}
      return
    end

    response = {}
    # Join user to group.
    begin
      if row.has_key?('group_role')
        # Break up any comma separated list of roles into individual roles
        group_roles = row['group_role'].split(',')
        options = {}
        if row.has_key?('group_fee')
          options = case row['group_fee']
            when 'full' then {:should_pay => 1, :payment_method => :full}
            when 'plan' then {:should_pay => 1, :payment_method => :plan}
            else {}
          end
        end
        group_roles.each {|group_role|
          # Remove whitespace
          group_role = group_role.strip
          if row.has_key?('group_webform_id')
            webform_ids = row['group_webform_id'].split(',')
            response = self.user_join_group(group_uuid, user['uuid'], group_role, options, webform_ids)
          else
            response = self.user_join_group(group_uuid, user['uuid'], group_role, options)
          end
        }
      else
        response = self.user_join_group(group_uuid, user['uuid'])
      end
    rescue RestClient::Exception => e
      @logger.error(get_row_count.to_s) {'User ' + user['uuid'] + " failed to join group " + group_uuid.to_s + ': ' + e.message}
    else
      if row.has_key?('group_role')
        @logger.info(get_row_count.to_s) {'User ' + user['uuid'] + " joined group " + group_uuid.to_s + ' with role(s) ' + row['group_role']}
      else
        @logger.info(get_row_count.to_s) {'User ' + user['uuid'] + " joined group " + group_uuid.to_s}
      end
    end

    #log stuff!!

    response
  end

end
