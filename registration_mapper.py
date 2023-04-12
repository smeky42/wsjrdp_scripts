# registration_mapper.py

def type(role_wish):
  match role_wish:
    case 'Teilnehmende*r':
      return '1'
    case _:
      return '2'

def position(role_wish):
  match role_wish:
    case 'Teilnehmende*r':
      return 'S'
    case 'Unit Leitung':
      return 'L'
    case 'Kontingentsteam':
      return 'C'
    case 'IST':
      return 'S'
    case _:
      print("Rolle ist {role_wish}")
      ' '
  
def name(first_name, nickname):
  name = first_name
  if nickname and len(nickname) > 2:
    name = nickname 

  return name

def gender(gender):
  match gender:
    case 'm':
      return 'M'
    case 'w':
      return 'F'
    case _:
      return 'O'

def nationality(passport_nationality):
  if "Deu" in passport_nationality or passport_nationality  == "D" or passport_nationality == "DE":
    return '49'
  elif "Öster" in passport_nationality or "Oester" in passport_nationality or passport_nationality == "AT":
    return '43'
  elif "Belg" in passport_nationality:
    return '32'
  elif "Pol" in passport_nationality:
    return '48'
  elif "Franz" in passport_nationality or passport_nationality == "FR":
    return '33'
  elif "Finn" in passport_nationality:
    return '358'
  elif "Syr" in passport_nationality:
    return '963'
  elif "Südkorea" in passport_nationality:
    return '82'
  elif "Span" in passport_nationality:
    return '34'
  elif "Filip" in passport_nationality:
    return '63'
  elif "Türk" in passport_nationality:
    return '90'
  elif "Nied" in passport_nationality or passport_nationality == "NL":
    return '31'
  elif "Ungar" in passport_nationality:
    return '36'
  elif "Brit" in passport_nationality:
    return '44'
  elif "Bol" in passport_nationality:
    return '591'
  elif "Ital" in passport_nationality or passport_nationality == "IT":
    return '39'
  elif "Port" in passport_nationality:
    return '351'
  elif "Aserb" in passport_nationality:
    return '994'
  elif "Russ" in passport_nationality:
    return '7'
  elif "CH" == passport_nationality:
    return '41'
  elif "IE" == passport_nationality:
    return '353'
  else:
    print (f"Error: Konnte Nationalität {passport_nationality} nicht mappen.")
    return '-'