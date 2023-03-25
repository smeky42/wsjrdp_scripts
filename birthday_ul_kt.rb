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

def create_date_array(start_date, end_date)
  valid_days = []

  (start_date..end_date).each do |day|
    valid_days.append("#{day.day}.#{day.month}")
  end

  valid_days
end

def print_days(people, valid_days)
  people.each do |person|
    birthday = person['birthday']

    if valid_days.include? "#{birthday.day}.#{birthday.month}"
      puts "#{person['role_wish']} #{person['id']}: #{person['first_name']} #{person['last_name']} - #{person['birthday']}"
    end
  end
end

config = YAML.load_file('./config.yml')

puts 'Starting to process'

puts 'Conntect to SQL'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

puts "\n Starte generierung für Unit Leitungen und KT"
query = 'select p.id, p.first_name, p.last_name, p.birthday, p.role_wish
          from people p
          where id > 1
          and (role_wish="IST" or role_wish="Kontingentsteam" or role_wish="Unit Leitung")
          and (status = "bestätigt durch KT" or status = "bestätigt durch Leitung" or status = "vollständig")
          or id = 1078;' # 1078 Mio
people = client.query(query)
puts "mit #{people.count} Mitgliedern"

puts 'KT Wolfsburg 14. - 16.04.'
start_date = Date.new(2023, 4, 14)
end_date = Date.new(2023, 4, 16)
valid_days_kt = create_date_array(start_date, end_date)
print_days(people, valid_days_kt)

puts 'KoLa 17. - 21.05'
start_date = Date.new(2023, 5, 17)
end_date = Date.new(2023, 5, 21)
valid_days_kola = create_date_array(start_date, end_date)
print_days(people, valid_days_kola)

puts 'AKT Jamboree 29.07 - 08.12'
start_date = Date.new(2023, 7, 29)
end_date = Date.new(2023, 8, 12)
valid_days_jamb = create_date_array(start_date, end_date)
print_days(people, valid_days_jamb)
