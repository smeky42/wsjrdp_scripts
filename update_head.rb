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

query = "select id, primary_group_id, head, first_name, last_name, role_wish, updated_at from people
        where ((status = 'bestätigt durch KT' or status = 'bestätigt durch Leitung' or status = 'vollständig') or id = 1078)
        -- and primary_group_id = 1
        order by id;"
accounts = client.query(query)

puts 'mit ' + accounts.count.to_s + ' Personen'
counter = 0
accounts.each do |account|
  if account['updated_at'].to_date > Date.new(2023, 3, 30)
    puts account['id'].to_s + account['role_wish'] + account['updated_at'].to_s + account['head']
    counter += 1
  else
    query = "update people set head = 'Ich finde Beides gut' where id=#{account['id']}"
    client.query(query)
  end
end
puts counter
