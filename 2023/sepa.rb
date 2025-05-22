# load "/path" in rails console
require 'mysql2'
require 'csv'
require 'sepa_king'

puts 'Starting to process'

write_to_db = false
date = Time.now.strftime('%Y-%m-%d--%H-%M-%S--')
month_number = 14
month_text = 'Vierzehnte'
request_date = Date.new(2023, 1, 5)
date_text = '2023-01-05 00:00:00'

# include Finance Helper
def payment_array
  [
    [' ', 'Gesamtbeitrag', 'Dez 21', 'Jan 22', 'Feb 22', 'Mär 22', 'Apr 22', 'Mai 22', 'Jun 22',
     'Jul 22', 'Aug 22', 'Sep 22', 'Okt 22', 'Nov 22', 'Dez 22', 'Jan 23', 'Feb 23', 'Mär 23',
     'Apr 23', 'Mai 23'],
    ['Teilnehmende*r', ' 4.100 € ', ' 150 € ', ' 150 € ', ' 150 € ', ' 150 € ', ' 150 € ',
     ' 150 € ', ' 150 € ', ' 250 € ', ' 250 € ', ' 250 € ', ' 250 € ', ' 250 € ', ' 300 € ',
     ' 300 € ', ' 300 € ', ' 300 € ', ' 300 € ', ' 300 € '],
    ['Unit Leitung', ' 3.250 € ', ' 150 € ', ' 150 € ', ' 150 € ', ' 150 € ', ' 150 € ',
     ' 150 € ', ' 150 € ', ' 200 € ', ' 200 € ', ' 200 € ', ' 200 € ', ' 200 € ', ' 200 € ',
     ' 250 € ', ' 250 € ', ' 250 € ', ' 250 € ', ' -   € '],
    ['IST', ' 1.650 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ',
     ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ',
     ' 100 € ', ' 100 € ', ' 50 € ', ' -   € '],
    ['Kontingentsteam', ' 1.300 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ',
     ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ', ' 100 € ',
     ' -   € ', ' -   € ', ' -   € ', ' -   € ', ' -   € ']
  ]
end

def ammount(ammount)
  return 0 if ammount == '- € '

  ammount.gsub('.', '').split(' ')[0].to_i * 100
end

def payment_by_month(role, month)
  payment_data_by(role)[month]
end

def payment_data_till_month(role, month)
  payment_data_by(role)[0..month]
end

def payment_data_till_date(role, end_date)
  start_date = Time.new(2021, 12, 4)
  month = (end_date.year * 12 + end_date.month) - (start_date.year * 12 + start_date.month)
  payment_data_by(role)[0..month]
end

def accounting_balance(client, id, role)
  query = 'select a.subject_id, a.ammount
          from accounting_entries a
          where subject_id=' + id.to_s + ';'
  accounting = client.query(query)

  balance = - total_payment_by(role)
  accounting.each do |x|
    balance -= x['ammount']
  end
  balance
end

def dept(month, role)
  (total_payment_by(role) + payment_data_till_month(role, month).inject(0) { |sum, x| sum + x })
end

def total_payment_by(role)
  -1 * ammount(payment_array_by(role)[1][1])
end

def payment_data_by(role)
  payment_array_by_role = payment_array_by(role)
  payment_array_by_role[1].drop(2).map { |x| ammount(x) }
end

def payment_array_by(role)
  payment_array.select { |line| (line[0] == role || line[0] == ' ') }
end

def payment_value(role)
  payment_array_by(role)[1][1]
end
# end include Finance Helper

def next_payment(client, id, role, month_number)
  to_pay = dept(month_number - 1, role) + accounting_balance(client, id, role)

  to_pay = 0 if to_pay < 0
  to_pay
end

puts 'Conntect to SQL'
client = Mysql2::Client.new(host: 'anmeldung.worldscoutjamboree.de', username: '',
                            password: '', database: '', port: )

sct = SEPA::DirectDebit.new(
  name: 'Ring deutscher Pfadfinder*innenverbände e.V',
  bic: 'GENODE51KS1',
  iban: 'DE34520900000077228802',
  creditor_identifier: ''
)

puts 'Starte generierung für '
query = 'select p.id, p.first_name, p.last_name, p.email, p.gender, p.role_wish, p.status, p.nickname,
         p.primary_group_id, p.sepa_mail, p.sepa_iban, p.sepa_name, p.upload_sepa_pdf, p.sepa_status
          from people p
          where id>1
          and (status = "bestätigt durch KT" or status = "bestätigt durch Leitung" or status = "vollständig")
          and  (p.sepa_status = "OK" or p.sepa_status is null)
          limit 5000;'
people = client.query(query)
puts 'mit ' + people.count.to_s + ' Mitgliedern'

all = []
ul = people.select { |participant| (participant['role_wish'] == 'Unit Leitung') }
puts 'davon UL: ' + ul.count.to_s

kt = people.select { |participant| (participant['role_wish'] == 'Kontingentsteam') }
puts 'davon KT: ' + kt.count.to_s

ist = people.select { |participant| (participant['role_wish'] == 'IST') }
puts 'davon IST: ' + ist.count.to_s

tn = people.select { |participant| (participant['role_wish'] == 'Teilnehmende*r') }
puts 'davon TN: ' + tn.count.to_s

puts 'mit ' + people.count.to_s + ' Mitgliedern'

people.each do |person|
  amount = next_payment(client, person['id'].to_s, person['role_wish'], month_number)

  if amount.nil? || amount == 0
    amount = 0
    cent = '0'
    euro = '0'
    puts 'Betrag? ' + person['id'].to_s + ': ' + person['first_name'] + ' ' + person['last_name'] + ' -> ' + euro + '.' + cent + ' €'
  else
    cent = amount.to_s[-2..-1]
    euro = amount.to_s[0..-3]

    begin
      if (amount < 10_000) || (amount > 31_000)
        puts 'Betrag? ' + person['id'].to_s + ': ' + person['first_name'] + ' ' + person['last_name'] + ' -> ' + euro + '.' + cent + ' €'
      end

      infotext = month_text + ' Rate WSJ 2023 ' + person['first_name'] + ' ' + person['last_name'] + ' ' + person['role_wish'][0] + ' ' + person['id'].to_s
      infotext = 'Fehlende Raten und ' + infotext if amount > 31_000

      sct.add_transaction(
        name: person['sepa_name'],
        iban: person['sepa_iban'].upcase.gsub(' ', ''),
        amount: euro + '.' + cent,
        currency: 'EUR',
        remittance_information: infotext,
        mandate_id: 'wsjrdp' + person['id'].to_s,
        requested_date: request_date,
        mandate_date_of_signature: Date.parse(person['upload_sepa_pdf'].split('/')[-1][0..9]),
        local_instrument: 'CORE',
        # batch_booking: true,
        sequence_type: amount > 31_000 ? 'FRST' : 'RCUR'
      )

      if write_to_db
        update = 'update people
                  set sepa_status="OK"
                  where id="' + person['id'].to_s + '";'
        client.query(update)

        insert = "insert into accounting_entries (subject_id, author_id, ammount, comment, created_at )
                values (#{person['id']},2,#{amount}, '#{month_text} Rate SEPA', '#{date_text}');"
        client.query(insert)
      end
    rescue StandardError => e
      puts person['id'].to_s + ': ' + person['first_name'] + ' ' + person['last_name'] + ' -> ' + amount.to_s
      puts e
    end
  end
end

full_path = date + 'SEPA.xml'
open(full_path, 'w') do |f|
  f << sct.to_xml
end
