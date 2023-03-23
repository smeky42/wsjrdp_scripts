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

event = 217
group = 57

puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

query = "select id, name
                from event_translations
                where id=#{event};"
event_name = client.query(query).first['name']

query =   "select e.id as participation_id, e.event_id, e.person_id, p.id, p.first_name, p.last_name, p.role_wish, p.primary_group_id
          from event_participations as e
            left join people as p on p.id = e.person_id
          where e.event_id=#{event}
          -- and p.primary_group_id=#{group}
          order by p.role_wish, p.id;"
participants = client.query(query)

puts "Starte generierung für #{participants.count} Teilnehmer an #{event_name}"

CSV.open("#{date}-#{event_name}.csv", 'w') do |csv|
  participants.each do |participant|
    puts "#{participant['id']} - #{participant['role_wish']} - #{participant['first_name']} #{participant['last_name']} #{participant['participation_id']}"

    query =   "select participation_id, answer
                  from event_answers
                  where participation_id=#{participant['participation_id']};"
    answers = client.query(query)
    row = [participant['id'], participant['primary_group_id'], participant['role_wish'], participant['first_name'],
           participant['last_name']]

    answers.each do |answer|
      row << answer['answer']
    end
    csv << row
  end
end
