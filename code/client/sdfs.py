import json
f = open('config.json', 'r')
data = json.load(f)
server_list = data['nodes']
