import json, requests
url = 'http://192.168.2.10:6345/setDischargeSlot1'
payload = {}
payload["start"]="00:03"
payload["finish"]="00:05"
r = requests.post(url, data=json.dumps(payload))
print (r.text)