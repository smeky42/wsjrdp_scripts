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
      print("Rolle ist #{role_wish}")
      ' '
  
def name(first_name, nickname):
  name = first_name
  if nickname is not None and nickname.len() > 2:
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