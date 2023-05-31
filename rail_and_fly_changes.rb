# load "/path" in rails console
require 'rubygems' if RUBY_VERSION < '1.9'
require 'mysql2'
require 'csv'
require 'openssl'
require 'net/smtp'
require 'rest-client'
require 'json'
require 'securerandom'
require 'asciify'

puts 'Starting to process'

config = YAML.load_file('./config.yml')
date = Time.now.strftime('%Y-%m-%d--%H-%M-%S--')

def group_name(groups, primary_group_id)
  groups.each do |group|
    return group['short_name'] if group['id'] == primary_group_id
  end
  ' '
end

puts 'Conntect to SQL'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

query = "select id, primary_group_id, first_name, last_name, rail_and_fly_reason from people
        -- where ((status = 'bestätigt durch KT' or status = 'bestätigt durch Leitung' or status = 'vollständig') or id = 1078)
        -- and primary_group_id = 1
        order by primary_group_id, id;"
accounts = client.query(query)

query = 'select id, short_name from groups'
groups = client.query(query)

puts 'mit ' + accounts.count.to_s + ' Personen'

CSV.open("#{date}--Rail-Fly-Change.csv", 'w') do |csv|
  csv << %w[primary_group_id group_name id first_name last_name rail_and_fly_reason]
  accounts.each do |account|
    if !account['rail_and_fly_reason'].nil? && account['rail_and_fly_reason'].length > 2
      csv << [account['primary_group_id'], group_name(groups, account['primary_group_id']),
              account['id'], account['first_name'], account['last_name'], account['rail_and_fly_reason']]
    end
  end
end
