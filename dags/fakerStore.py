import requests
import pandas as pd

response = requests.get('https://dummyjson.com/carts')
print(response.json())
        
