# frozen_string_literal: true

#  Copyright (c) 2012-2022, German Contingent for the Worldscoutjamboree 2023. This file is part of
#  hitobito_wsjrdp_2023 and licensed under the Affero General Public License version 3
#  or later. See the COPYING file at the top-level directory or at
#  https://github.com/hitobito/hitobito_wsjrdp_2023 and https://github.com/hitobito/wsjrdp_scripts.

require 'mysql2'
require 'csv'
require 'openssl'
require 'net/smtp'
require 'yaml'

config = YAML.load_file('./config.yml')

puts 'Starting to process'
puts 'Conntect to SQL'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

puts "\n Starte generierung für Unit Leitungen und KT"
query = 'select p.id, p.first_name, p.last_name, p.passport_germany, p.passport_nationality
          from people p
          where id > 1
          and p.passport_germany = 0
          and ( p.passport_nationality NOT LIKE "Deutsch%")
          and (status = "bestätigt durch KT" or status = "bestätigt durch Leitung");'
people = client.query(query)
puts "mit #{people.count} Mitgliedern"

people.each do |person|
  puts "#{person['id']}: #{person['first_name']} #{person['last_name']} - #{person['passport_germany']} - #{person['passport_nationality']}"
end
