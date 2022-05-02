import requests

r = requests.post('http://localhost:5000/setdatos', json={
  "Id": 78912,
  "Customer": "Jason Sweet",
  "Quantity": 1,
  "Price": 18.00
})
print(f"Status Code: {r.status_code}, Response: {r.json()}")