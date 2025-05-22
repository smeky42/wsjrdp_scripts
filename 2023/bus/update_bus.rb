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

table = CSV.parse(File.read('2022-11-15--bus.csv'), headers: true)

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

table.length.times do |_i|
  puts "#{table[_i]['TN ID']} - #{table[_i]['Abfahrtsort2']}"
  query = 'update people set bus_travel = "Vorraussichtlicher Abfahrtsort: ' + table[_i]['Abfahrtsort2'] +
          '" where id=' + table[_i]['TN ID'].to_s + ';'
  client.query(query)
end
