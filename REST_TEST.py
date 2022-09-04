import json, requests
url = 'http://192.168.2.10:6345/tempPauseDischarge'
payload = 10
r = requests.post(url, data=json.dumps(payload))
print (r)