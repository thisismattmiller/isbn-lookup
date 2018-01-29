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

orginal_region_slugs = region_slugs
region_slugs = []
for x in range(0,22):
  for r in orginal_region_slugs:
    region_slugs.append(r)


data = {
  "names": [
    "xisbn-1"
  ],
  "region": region_slugs[4],
  "size": "s-1vcpu-1gb",
  "image": 31317581,
  "ssh_keys": [813340],
  "backups": False,
  "ipv6": False,
  "user_data": None,
  "private_networking": None,
  "tags": [
    "xisbn"
  ]
}

create = requests.post("https://api.digitalocean.com/v2/droplets",json=data,headers=headers).json()
print(create)
droplet_id = create['droplets'][0]['id']


sleep_time = 10

while True:
  print("looking at the status of ", droplet_id)

  status = requests.get("https://api.digitalocean.com/v2/droplets/" + str(droplet_id),headers=headers).json()

  if 'droplet' in status:
    if status['droplet']['status'] == 'active':
      print("droplet active, working")
      sleep_time = 20
    elif status['droplet']['status'] == 'off':
      print("droplet off, job done, deleting")
      status = requests.delete("https://api.digitalocean.com/v2/droplets/" + str(droplet_id),headers=headers)
      print(status.status_code)   

      break
    else:
      print(status['droplet']['status'])


  else:
    print("Resource not found: ",droplet_id)
  time.sleep(sleep_time)
