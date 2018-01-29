import requests
import os
import time

token = os.environ['do_key']

# get a list of all active regiions right now
headers = {"Authorization":"Bearer " + token}
regions = requests.get("https://api.digitalocean.com/v2/regions",headers=headers).json()
region_slugs = []

for r in regions['regions']:
	if r['available'] == True:
		region_slugs.append(r['slug'])



data = {
  "names": [
    "xisbn-1"
  ],
  "region": region_slugs[0],
  "size": "s-1vcpu-1gb",
  "image": "ubuntu-16-04-x64",
  "ssh_keys": [813340],
  "backups": False,
  "ipv6": False,
  "user_data": None,
  "private_networking": None,
  "tags": [
    "xisbn"
  ]
}

# create = requests.post("https://api.digitalocean.com/v2/droplets",json=data,headers=headers).json()
# droplet_id = create['droplets'][0]['id']

droplet_id = 80487151


while True:
	print("looking at the status of ", droplet_id)

	status = requests.get("https://api.digitalocean.com/v2/droplets/" + str(droplet_id),headers=headers).json()

	if 'droplet' in status:
		if status['droplet']['status'] == 'active':
			print("droplet ready dude")
			break
		else:
			print(status['droplet']['status'])


	else:
		print("Resource not found: ",droplet_id)
	time.sleep(1)
