require 'mysql2'
require 'csv'
require 'openssl'
require 'net/smtp'
require 'yaml'

config = YAML.load_file('./config.yml')

puts 'Starting to process'
days = %w[07 08 09]
month = ['10']

puts 'Conntect to SQL'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

puts "\n Starte generierung für Unit Leitungen und KT"
query = 'select p.id, p.first_name, p.last_name, p.birthday
          from people p
          where id > 1
          and (role_wish="Unit Leitung" or role_wish="Kontingentsteam")
          and (status = "bestätigt durch KT" or status = "bestätigt durch Leitung")
          or id = 1078;' # 1078 Mio
people = client.query(query)
puts "mit #{people.count} Mitgliedern"

people.each do |person|
  birthday = person['birthday'].to_s.split('-')
  birthday_day = birthday[2]
  birthday_month = birthday[1]

  if (days.include? birthday_day) && (month.include? birthday_month)
    puts "#{person['id']}: #{person['first_name']} #{person['last_name']} - #{person['birthday']}"
  end
end
