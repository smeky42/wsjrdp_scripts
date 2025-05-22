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
sepa_text = 'Neunzehn Rate SEPA'
sepa_time = '2023-06-05 00:00:00'

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

query = "select comment, created_at
          from accounting_entries
          where comment='#{sepa_text}';"
accounts = client.query(query)

puts "#{accounts.size} Einträge mit '#{sepa_text}' gefunden"

query = "update accounting_entries
          set created_at = '#{sepa_time}'
          where comment='#{sepa_text}';"
client.query(query)
