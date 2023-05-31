# frozen_string_literal: true

#  Copyright (c) 2012-2022, German Contingent for the Worldscoutjamboree 2023. This file is part of
#  hitobito_wsjrdp_2023 and licensed under the Affero General Public License version 3
#  or later. See the COPYING file at the top-level directory or at
#  https://github.com/hitobito/hitobito_wsjrdp_2023 and https://github.com/hitobito/wsjrdp_scripts.
require 'rubygems' if RUBY_VERSION < '1.9'
require 'mysql2'
require 'csv'
require 'openssl'
require 'net/smtp'
require 'rest-client'
require 'json'
require 'securerandom'
require 'asciify'

config = YAML.load_file('./config.yml')
date = Time.now.strftime('%Y-%m-%d--%H-%M-%S--')

def concat(sql_result, column_name)
  concat_array = []
  sql_result.each do |row|
    concat_array << row[column_name] unless row[column_name].nil?
  end
  if concat_array.length.positive?
    concat_array.sort.join('|')
  else
    ' '
  end
end

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

query =   "select g.id, g.name, g.short_name
          from groups g
          where parent_id=2
          order by g.id;"
groups = client.query(query)
puts "Starte generierung für #{groups.count} Gruppen"

CSV.open("#{date}Flight.csv", 'w') do |csv|
  csv << %w[ID Name Kurzname Anzahl PLZs Orte]
  groups.each do |group|
    query = "select id, primary_group_id, zip_code, status, town
              from people
              where primary_group_id=#{group['id']};"
    accounts = client.query(query)
    puts "#{group['name']} mit #{accounts.count} Personen"

    csv << [group['id'], group['name'], group['short_name'], accounts.count, concat(accounts, 'zip_code'),
            concat(accounts, 'town')]
  end
end
