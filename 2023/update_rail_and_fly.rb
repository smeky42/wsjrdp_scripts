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

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])
# query =   "select g.id, g.name, g.short_name
#           from groups g
#           order by g.id;"
# groups = client.query(query)

# groups.each do |group|
#   puts "#{group['id']}: #{group['short_name']}"
# end
# Grün
# 26: A5 Preußisches Laserkraut
# 38: B2 Schöner Lauch
# 39: B3 Zwergteichrose
# 40: B4 Nordischer Drachenkopf
# 41: B5 Bunte Schwertlilie
# 14: C1 Schwarze Teufelskralle
# 15: C2 Großer Hufeisenklee
# 18: C5 Sonnentau
# 19: C6 Schöner Blaustern
# 21: C8 Flammen-Röschen
# 7: D1 Lämmersalat
# 8: D2 Verarmte Segge
# 10: D4 Meeresleuchten
# 12: D6 Wunder-Veilchen
# 13: D7 Deutscher Ginster
# 55: E8 Lothringer Lein
query = 'update people set rail_and_fly = "Rail and Fly" where primary_group_id in (26,38,39,40,41,14,15,18,19,21,7,8,10,12,13,55);'
client.query(query)
# Grau
# 24: A3 Wassernuss
# 27: A6 Aufgeblasener Fuchsschwanz
# 42: B6 Fingerhut
# 43: B7 Sichelmöhre
# 44: B8 Südlicher Wasserschlauch
# 16: C3 Zwerg Goldstern
# 17: C4 Hibiskus
# 20: C7 Pyrenäen Drachenmaul
# 9: D3 Schwarzer Nachtschatten
# 51: F7 Adonis flammea
query = 'update people set rail_and_fly = "Rail and Fly nicht möglich" where primary_group_id in (24,27,42,43,44,16,17,20,9,51);'
client.query(query)

# Rot
# 22: A1 Blaue Himmesleiter
# 23: A2 Türkenbund
# 25: A4
# 28: A7 Teufelsauge
# 29: A8 Feuer-Lilie
# 37: B1 Lungenenzian
# 30: E1 Knabenkraut
# 31: E2 Sibirische Schwertlilie
# 32: E3 Berg-Laserkraut
# 47: F3 Trollblume
# 48: F4 Wollige Wolfsmilch
# 50: F6 Glanzloser Ehrenpreis
# 52: F8 Unverwechselbarer Löwenzahn
query = 'update people set rail_and_fly = "Kein Rail and Fly" where primary_group_id in (22,23,25,28,29,37,30,31,32,47,48,50,52);'
client.query(query)

# Gelb
# 11: D5 Kornrade
# 54: D8 Schlangenäuglein
# 33: E4 Kleine Seerose
# 34: E5 Schachblume
# 35: E6 Echte Bärentraube
# 36: E7 Waldmeister
# 45: F1 Edelweiß
# 46: F2 spreizender Storchschnabel
# 49: F5 Gewöhnliche Küchenschelle
query = 'update people set rail_and_fly = "Expressrail" where primary_group_id in (11,54,33,34,35,36,45,46,49);'
client.query(query)
