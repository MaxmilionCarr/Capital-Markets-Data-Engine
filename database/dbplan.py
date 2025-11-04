# Think about this one, but user should only be able to 
# import the database object and from here start fetching
from db import DataBase

db = DataBase()

# Example usage for exchanges TODO: This needs to change, should be db.get_exchange(name of exchange) and gets an exchange object returned
exchanges = db.exchange_repo.get_info