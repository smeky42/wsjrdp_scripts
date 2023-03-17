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
start_date = Date.new(2023, 7, 28)
end_date = Date.new(2023, 8, 8)
valid_days = []

(start_date..end_date).each do |day|
  # puts "#{day.day}.#{day.month}"
  valid_days.append("#{day.day}.#{day.month}")
end

puts 'Conntect to SQL'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

puts "\n Starte generierung für Unit Leitungen und KT"
query = 'select p.id, p.first_name, p.last_name, p.birthday, p.role_wish
          from people p
          where id > 1
          and (role_wish="IST" or role_wish="Kontingentsteam")
          and (status = "bestätigt durch KT" or status = "bestätigt durch Leitung" or status = "vollständig")
          or id = 1078;' # 1078 Mio
people = client.query(query)
puts "mit #{people.count} Mitgliedern"

# puts valid_days

people.each do |person|
  birthday = person['birthday']

  # puts "#{person['birthday']} #{birthday.day}.#{birthday.month}"

  if valid_days.include? "#{birthday.day}.#{birthday.month}"
    puts "#{person['role_wish']} #{person['id']}: #{person['first_name']} #{person['last_name']} - #{person['birthday']}"
  end
end
