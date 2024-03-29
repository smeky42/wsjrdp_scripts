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

def name(first_name, nickname)
  name = first_name.split(' ')[0]
  name = nickname if !nickname.nil? && nickname.size > 2

  name
end

puts 'Starting to process'
date = Time.now.strftime('%Y-%m-%d--%H-%M-%S--')

puts 'Conntect to SQL'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

puts "\n Starte generierung für Unit Leitungen und KT"
query = 'select p.id, p.first_name, p.last_name, p.nickname, p.role_wish, p.pronoun, p.primary_group_id, p.role_wish
          from people p
          where id > 1
          and (status = "bestätigt durch KT" or status = "bestätigt durch Leitung" or status = "vollständig")
          or id = 1078;' # 1078 Mio
people = client.query(query)
puts "mit #{people.count} Mitgliedern"

# longest_names = ['', '', '', '', '']

# people.each do |person|
#   generated_name = name(person['first_name'], person['nickname'])

#   longest_names = longest_names.sort_by(&:length).reverse.take(15)

#   longest_names[4] = generated_name if longest_names[4].size < generated_name.size
# end

# puts longest_names

# all groups
query =   "select g.id, g.name, g.short_name
          from groups g
          order by g.id;"
groups = client.query(query)
puts "mit #{groups.count} Gruppen"

CSV.open("#{date}Name-Badges-komplett.csv", 'w') do |csv|
  people.each do |account|
    group = groups.select { |group| group['id'] == account['primary_group_id'] }.first
    csv << [group['id'], group['name'], account['id'], name(account['first_name'], account['nickname']),
            account['pronoun'], account['role_wish']]
  end
end

CSV.open("#{date}Name-Badges-IST.csv", 'w') do |csv|
  people.each do |account|
    next unless account['role_wish'] == 'IST'

    group = groups.select { |group| group['id'] == account['primary_group_id'] }.first
    csv << [group['id'], group['name'], account['id'], name(account['first_name'], account['nickname']),
            account['pronoun'], account['role_wish']]
  end
end

CSV.open("#{date}Name-Badges-KT.csv", 'w') do |csv|
  people.each do |account|
    next unless account['role_wish'] == 'Kontingentsteam'

    group = groups.select { |group| group['id'] == account['primary_group_id'] }.first
    csv << [group['id'], group['name'], account['id'], name(account['first_name'], account['nickname']),
            account['pronoun'], account['role_wish'], account['last_name']]
  end
end

CSV.open("#{date}Name-Badges-TN.csv", 'w') do |csv|
  people.each do |account|
    next unless account['role_wish'] == 'Teilnehmende*r'

    group = groups.select { |group| group['id'] == account['primary_group_id'] }.first
    csv << [group['id'], group['name'], account['id'], name(account['first_name'], account['nickname']),
            account['pronoun'], account['role_wish']]
  end
end

CSV.open("#{date}Name-Badges-UL.csv", 'w') do |csv|
  people.each do |account|
    next unless account['role_wish'] == 'Unit Leitung'

    group = groups.select { |group| group['id'] == account['primary_group_id'] }.first
    csv << [group['id'], group['name'], account['id'], name(account['first_name'], account['nickname']),
            account['pronoun'], account['role_wish']]
  end
end
