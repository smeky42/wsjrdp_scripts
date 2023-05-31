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
group_id = '45'

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

query =   "select g.id, g.name, g.short_name
          from groups g
          where id=#{group_id};"
group = client.query(query).first
puts "Starte generierung für #{group['name']}"

query = "select id, first_name, last_name, gender, primary_group_id, zip_code, status
          from people
          where primary_group_id=#{group_id};"
accounts = client.query(query)

CSV.open("#{date}Flight-#{group['name'].gsub(' ', '-')}.csv", 'w') do |csv|
  csv << %w[ID Anrede Vorname Nachname]
  accounts.each do |account|
    salutation = ''
    case account['gender']
    when 'w'
      salutation = 'MRS'
    when 'm'
      salutation = 'MR'
    end

    csv << [account['id'], salutation, account['first_name'], account['last_name']]
  end
end
