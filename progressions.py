import glob
import os
import os.path
import random
import requests
import threading
import time

out_dir = os.path.expanduser('~/500px-progressions')
try:
    os.mkdir(out_dir)
except OSError:
    pass

fresh_url = 'https://webapi.500px.com/discovery/fresh?feature=fresh&include_states=true&include_licensing=false&page=1&rpp=1'

# IDs of photos that already have been processed or are currently processing
processed_photos = [int(os.path.basename(d).split('-')[0])
                    for d in glob.glob(os.path.join(out_dir, '*'))]
processed_photos = set(processed_photos)


def get_new_photo():
    while True:
        response = requests.get(fresh_url)
        if response.status_code != requests.codes.ok:
            raise RuntimeError("Unable to get a new photo")
        photo_id = int(response.json()['photos'][0]['id'])
        user_id = response.json()['photos'][0]['user']['username']
        if photo_id not in processed_photos:
            processed_photos.add(photo_id)
            return (photo_id, user_id)
        else:
            time.sleep(2)


# (interval time, number of iterations)
schedule = [
    (60 * 10, 72),  # 10 minutes for 12 hours
    (60 * 30, 24),  # 30 minutes for 12 hours
    (60 * 60, 24),  # every hour for another days
]


class Worker(object):

    def __init__(self, worker_id, schedule, continue_event):
        self.worker_id = worker_id
        self.schedule = schedule
        self.continue_event = continue_event

    def _log(self, message):
        print('{}: Worker {}: {}'.format(int(time.time()),
                                         self.worker_id,
                                         message))

    def __call__(self):

        while not self.continue_event.is_set():
            errors = []
            photo_id, user_id = get_new_photo()
            self._log("Starting to process photo {}".format(photo_id))

            photo_dir = os.path.join(out_dir, str(photo_id))
            os.mkdir(photo_dir)

            timer = time.time()
            for sleep_time, iterations in self.schedule:
                self._log("Starting schedule {} for image {}".format(
                    (sleep_time, iterations), photo_id))
                if self.continue_event.is_set():
                    self._log("Stopping as requested")
                    return

                for _ in range(iterations):
                    if self.continue_event.is_set():
                        self._log("Stopping as requested")
                        return
                    self._log("New loop for {} at {}".format(
                        photo_id, timer))

                    try:
                        photo_response = requests.get(
                            'https://500px.com/photo/' + str(photo_id))
                        user_response = requests.get(
                            'https://500px.com/' + str(user_id))

                        timestamp_dir = os.path.join(
                            photo_dir, str(int(timer)))
                        os.mkdir(timestamp_dir)

                        if photo_response.status_code != requests.codes.ok or \
                                user_response.status_code != requests.codes.ok:
                            self._log("Error getting data. "
                                      "photo_response: {}, "
                                      "user_response: {}".format(
                                          photo_response, user_response))
                            errors.append((int(timer),
                                           photo_response.status_code,
                                           user_response.status_code))
                        else:

                            with open(os.path.join(timestamp_dir,
                                                   'photo.html'), 'w') as f:
                                f.write(photo_response.text)
                            with open(os.path.join(timestamp_dir,
                                                   'user.html'), 'w') as f:
                                f.write(user_response.text)
                    except requests.exceptions.RequestException as e:
                        errors.append((int(timer), e, e))

                    if len(errors) > 3:
                        self._log("Too many errors, skipping to next photo")
                        break

                    timer += sleep_time
                    self.continue_event.wait(max(0, timer - time.time()))

                if len(errors) > 3:
                    self._log("Too many errors, skipping to next photo")
                    break

            self._log("Photo {} finished".format(photo_id))

            if errors:
                with open(os.path.join(photo_dir, 'error'), 'w') as f:
                    for timestamp, photo_status, user_status in errors:
                        f.write('{}: {}, {}\n'.format(timestamp,
                                                      photo_status,
                                                      user_status))
            else:
                with open(os.path.join(photo_dir, 'ok'), 'w') as f:
                    f.write('OK')

            self._log("Writing data finished")


if __name__ == "__main__":

    continue_event = threading.Event()

    threads = []
    try:
        print("starting to spawn workers")
        # for worker_id in range(1):
        for worker_id in range(200):
            worker = Worker(worker_id, schedule, continue_event)
            thread = threading.Thread(target=worker,
                                      name='worker-{}'.format(worker_id))
            threads.append((thread, worker))
            thread.start()
            time.sleep(10 + random.randint(0, 40))

        print("Spawning finished, starting main loop")

        while True:
            time.sleep(100)
    except KeyboardInterrupt:
        print("Interrupt received")
        continue_event.set()
        for thread, worker in threads:
            print("Joining worker {}".format(worker.worker_id))
            thread.join()
