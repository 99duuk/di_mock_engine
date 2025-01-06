import urllib3

http = urllib3.PoolManager()
response = http.request("GET", "https://www.google.com")
print(f"Status: {response.status}")
