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

def type(role_wish)
  case role_wish
  when 'Teilnehmende*r'
    '1'
  else
    '2'
  end
end

def position(role_wish)
  case role_wish
  when 'Teilnehmende*r'
    'S'
  when 'Unit Leitung'
    'L'
  when 'Kontingentsteam'
    'C'
  when 'IST'
    'S'
  else
    puts "Rolle ist #{role_wish}"
    ' '
  end
end

def name(first_name, nickname)
  name = first_name
  name = nickname if !nickname.nil? && nickname.size > 2

  name
end

def gender(gender)
  case gender
  when 'm'
    'M'
  when 'w'
    'F'
  else
    'O'
  end
end

def birthday(birthday)
  birthday
end

def nationality(passport_nationality)
  case passport_nationality
  when 'Deutsch' || 'deutsch'
    '28'
  else
    puts "Passport Nationality is #{passport_nationality} "
    '28'
  end
end

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])
puts 'Starte generierung'

query = "select id, role_wish, passport_nationality, first_name, last_name, nickname, gender, birthday, email, address, town, zip_code,
                additional_contact_name_a, additional_contact_name_b
          from people
          wher id=2;
          -- Just my Database Entry for testing"
accounts = client.query(query)

# * Your nationality, name, gender, position/role, and affiliation cannot be changed or modified.
CSV.open("#{date}Registration.csv", 'w') do |csv|
  csv << %w[No. Type NSO Position Nationality Hangeul Roman Surname MiddleName
            GivenName NameOnID Gender Birthday
            Mail Affiliation NSOJob NSOPosition Adress City State CityNationality
            HomePhone PhoneCountry PhoneNumber SNS SNSURL NameLG PhoneLG MailLG]
  accounts.each do |a|
    csv << [a['id'], type(a['role_wish']), '57', position(a['role_wish']), nationality(a['passport_nationality']), '', '', a['last_name'], '',
            a['first_name'], name(a['first_name'], a['nickname']), gender(a['gender']), a['birthday'],
            a['email'], '1', '-', '-', a['address'], a['town'], '-', '-', a['zip_code'],
            '-', '49', '-', '-', '-', a['additional_contact_name_a'], '-', a['email'],
            '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
            '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
            '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
            '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
            '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
            '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
            '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
            '-', '-', '-', '-', '-', '-', '-', '-', '-', '-',
            '-', '-']
  end
end
