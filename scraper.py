import glob
import os
import os.path
import random
import requests
from lxml import etree

out_dir = '/home/languitar/500px-dataset'
image_dir = os.path.join(out_dir, 'images')
image_success_dir = os.path.join(image_dir, 'success')
image_failure_dir = os.path.join(image_dir, 'failure')
user_dir = os.path.join(out_dir, 'users')
user_success_dir = os.path.join(user_dir, 'success')
user_failure_dir = os.path.join(user_dir, 'failure')

try:
    os.makedirs(image_success_dir)
except FileExistsError:
    pass
try:
    os.makedirs(image_failure_dir)
except FileExistsError:
    pass
try:
    os.makedirs(user_success_dir)
except FileExistsError:
    pass
try:
    os.makedirs(user_failure_dir)
except FileExistsError:
    pass

# find the image IDs we have already scraped
processed_images = [int(os.path.basename(f))
                    for f in glob.glob(os.path.join(image_success_dir, '*'))]
processed_images += [int(os.path.basename(f))
                     for f in glob.glob(os.path.join(image_failure_dir, '*'))]
processed_images = set(processed_images)

# find the user IDs we have already scraped
processed_users = [os.path.basename(f)
                   for f in glob.glob(os.path.join(user_success_dir, '*'))]
processed_users += [os.path.basename(f)
                    for f in glob.glob(os.path.join(user_failure_dir, '*'))]
processed_users = set(processed_users)


parser = etree.HTMLParser()


def get_user_url(contents):
    return etree.fromstring(contents, parser=parser).xpath(
        '//meta[@property="five_hundred_pixels:author"]')[0].attrib['content']


def download_user(url):
    username = os.path.basename(url)
    if username in processed_users:
        print('... skipping duplicate user')
        return
    print(username)

    response = requests.get(url, allow_redirects=True)

    if response.status_code != requests.codes.ok:
        print("  error: {}".format(response.status_code))
        with open(os.path.join(user_failure_dir, username), 'w') as f:
            f.write(str(response.status_code))
        processed_users.add(username)

    with open(os.path.join(user_success_dir, username), 'w') as f:
        f.write(response.text)
    processed_users.add(username)


# initialize missing users
for image_path in glob.glob(os.path.join(image_success_dir, '*')):
    with open(image_path) as f:
        contents = f.read()
    url = get_user_url(contents)
    download_user(url)

# for image_id in range(221072303, 0, -1):
while True:
    image_id = random.randint(1, 221072303)
    if image_id in processed_images:
        print('... skipping duplicate')
        continue
    print(image_id)

    response = requests.get('https://500px.com/photo/{}'.format(image_id),
                            allow_redirects=True)

    if response.status_code != requests.codes.ok:
        print("  error: {}".format(response.status_code))
        with open(os.path.join(image_failure_dir, str(image_id)), 'w') as f:
            f.write(str(response.status_code))
        processed_images.add(image_id)
        continue

    with open(os.path.join(image_success_dir, str(image_id)), 'w') as f:
        f.write(response.text)
    processed_images.add(image_id)

    download_user(get_user_url(response.text))
