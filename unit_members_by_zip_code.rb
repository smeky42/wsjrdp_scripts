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

# zip_codes = %w[82131 82061 82152 82166 82069 82067 82065 82064 82031 82064 82064 82041 82049 85764 85716 82054 82024
#               82008 85748 85579 85649 85774 85521 85662 85521 85737 85609 85640 85635 85540 85653 85622 85630 85551]

zip_codes = %w[80995 80997 80999 81247 81249 80331 80333 80335 80336 80469 80538 80539 81541 81543 81667 81669 81671
               81675 81677 81243 81245 81249 81671 81673 81735 81825 81675 81677 81679 81925 81927 81929 80933 80935
               80995 80689 81375 81377 80686 80687 80689 80335 80336 80337 80469 80333 80335 80539 80636 80797 80798
               80799 80801 80802 80807 80809 80937 80939 80637 80638 80992 80993 80997 80634 80636 80637 80638 80639
               81539 81541 81547 81549 80687 80689 81241 81243 81245 81247 81539 81549 81669 81671 81735 81737 81739
               80538 80801 80802 80803 80804 80805 80807 80939 80796 80797 80798 80799 80801 80803 80804 80809 80335
               80339 80336 80337 80469 81369 81371 81373 81379 80686 81369 81373 81377 81379 81379 81475 81476 81477
               81479 81735 81825 81827 81829 81543 81545 81547]
puts 'SQL Client'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: config['username'],
                            password: config['password'], database: config['database'], port: config['port'])

# all participants
query = "select id, first_name, last_name, primary_group_id, zip_code, status, town
          from people
          where (role_wish='Unit Leitung' or role_wish='Teilnehmende*r')
          and (status = 'bestätigt durch KT' or status = 'bestätigt durch Leitung');"
accounts = client.query(query)
accounts_in_zip_code = accounts.select { |participant| zip_codes.include? participant['zip_code'] }
puts "Starte generierung für #{accounts_in_zip_code.count} Personen"

# all groups
query =   "select g.id, g.name, g.short_name
          from groups g
          where parent_id=2
          order by g.id;"
groups = client.query(query)

CSV.open("#{date}ZIP.csv", 'w') do |csv|
  accounts_in_zip_code.each do |account|
    group = groups.select { |group| group['id'] == account['primary_group_id'] }.first
    csv << [group['id'], group['name'], group['short_name'], account['zip_code'], account['first_name'],
            account['last_name'], account['id']]
  end
end
