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

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

# all participants
query = 'select id, first_name, last_name, primary_group_id, role_wish, status
          from people
          where (status = "bestätigt durch KT" or status = "bestätigt durch Leitung" or status = "vollständig");'
people = client.query(query)

# all groups
query =   "select g.id, g.name, g.short_name
          from groups g
          order by g.id;"
groups = client.query(query)

puts "Personen im Status 'bestätigt durch KT' oder 'bestätigt durch Leitung' 'vollständig'"
puts 'Personen: ' + people.count.to_s
ul = people.select { |participant| (participant['role_wish'] == 'Unit Leitung') }
puts 'davon UL: ' + ul.count.to_s

kt = people.select { |participant| (participant['role_wish'] == 'Kontingentsteam') }
puts 'davon KT: ' + kt.count.to_s

ist = people.select { |participant| (participant['role_wish'] == 'IST') }
puts 'davon IST: ' + ist.count.to_s

tn = people.select { |participant| (participant['role_wish'] == 'Teilnehmende*r') }
puts 'davon TN: ' + tn.count.to_s

groups.each do |group|
  group_accounts = people.select { |participant| participant['primary_group_id'] == group['id'] }
  puts "#{group['id']}: #{group['short_name']} - #{group_accounts.size}"
end
