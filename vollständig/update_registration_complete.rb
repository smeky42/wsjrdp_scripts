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

config = YAML.load_file('../config.yml')
group_id = '39'
group_name = 'B3'

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

query = "update people set status = 'vollständig' where primary_group_id=#{group_id} and status='bestätigt durch Leitung';"
client.query(query)

query = "select id, first_name, last_name, primary_group_id, status
          from people
          where primary_group_id=#{group_id};"
accounts = client.query(query)

date = Time.now.strftime('%Y-%m-%d--%H-%M-%S--')
puts "Set status of Participants to vollständig #{date}"

accounts.each do |account|
  if account['status'] != 'vollständig'
    puts "#{account['id']} - #{account['first_name']} #{account['last_name']} - #{account['status']}"
  end
end

CSV.open("#{date}#{group_id}-#{group_name}.csv", 'w') do |csv|
  accounts.each do |account|
    csv << [account['id'], account['first_name'], account['last_name'], account['status']]
  end
end
