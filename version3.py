#! /usr/bin/env python3

# import requests
# import urllib
import urllib.request as urllib
import ssl
from datetime import datetime, timedelta
import json
from PIL import Image
import io
import os
import sys
import os.path
import queue
from threading import current_thread
from subprocess import Popen
import logging
# import signal

from concurrent.futures import wait, ProcessPoolExecutor, ThreadPoolExecutor


logging.basicConfig(
    format='%(asctime)s\t%(levelname)s:\t%(message)s',
    level=logging.INFO
)


BASE_URL = 'https://himawari8-dl.nict.go.jp/himawari8/img/'
# BASE_URL = 'https://himawari8.nict.go.jp/himawari8/img/'
BASE_URL += 'D531106'
ZOOM_LEVELS = [(1, '1d'), (4, '4d'), (8, '8d'), (16, '16d'), (20, '20d')]
ZOOM = 3
WIDTH = 550
MAX_RETRIES = 5
BASE_LOCATION = '/var/tmp/'
grid = queue.Queue()
images = list()

# # TODO:
# - remove the `images` global var
# - remove / replace the `grid` global var

LOCK_FILE = '/tmp/sat_data2.lock'


def exit_safely(signal, *args, **kwargs):
    os.remove(LOCK_FILE)
    sys.exit(0)


def fetch(url):
    return urllib.urlopen(
        url,
        timeout=60,
        context=ssl.SSLContext(ssl.PROTOCOL_TLS)
    ).read()


def get_latest():
    response = fetch(
        BASE_URL + '/latest.json'
    )

    return json.loads(response)['date']


def get_all(end, days=1):
    end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    start = end - timedelta(days=days)
    logging.info('Getting records since {start}'.format(start=start))
    future_set = []
    with ProcessPoolExecutor() as executor:
        while start < end:
            future = executor.submit(run, str(start))
            future_set.append(future)

            start = start + timedelta(minutes=10)

        wait(future_set)


def worker(base_url):
    while not grid.empty():
        tile = grid.get()
        logging.info(
            'Fetching Image {}/{} [Attempt: {}] [{}]'.format(
                ZOOM_LEVELS[ZOOM][0]**2 - len(grid.queue),
                ZOOM_LEVELS[ZOOM][0]**2,
                tile['attempts'] + 1,
                base_url + '_' + tile['name']
            )
        )
        tile['attempts'] += 1
        if tile['attempts'] > MAX_RETRIES:
            grid.task_done()
            continue
        response = None
        try:
            response = io.BytesIO(
                fetch(base_url + '_' + tile['name'])
            )
        except Exception:
            logging.exception('worker[{}] failed:'.format(current_thread()))
            grid.task_done()
            grid.put(tile)
            continue
        tile['image'] = Image.open(response)
        images.append(tile)
        grid.task_done()


def run(date):
    logging.info(date)
    start = datetime.utcnow()
    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    if os.path.exists(
        (BASE_LOCATION + '{}/satelite_{}.jpg').format(
            date.strftime('%Y_%m_%d'),
            str(date)
        )
    ):
        logging.info('file [{}{}/satelite_{}.jpg] already exists'.format(
            BASE_LOCATION, date.strftime('%Y_%m_%d'), str(date))
        )
        return
    base_url = '/'.join([
        BASE_URL, ZOOM_LEVELS[ZOOM][1], str(WIDTH),
        date.strftime('%Y'),
        date.strftime('%m'),
        date.strftime('%d'),
        date.strftime('%H%M%S')
    ])

    for x in range(0, ZOOM_LEVELS[ZOOM][0]):
        for y in range(0, ZOOM_LEVELS[ZOOM][0]):
            grid.put_nowait({
                'name': str(x) + '_' + str(y) + '.png',
                'x': x,
                'y': y,
                'attempts': 0
            })

    future_set = []
    with ThreadPoolExecutor() as executor:
        # Create the workers to process the job queue
        for i in range(grid.qsize()):
            future = executor.submit(worker, base_url)
            future_set.append(future)
    # Blocks until all the workers are completed.
    wait(future_set)
    grid.join()

    logging.info(
        'Retrieved %d images in %s seconds' % (
            len(images),
            str((datetime.utcnow() - start).total_seconds())
        )
    )
    stitch(date)


def stitch(date):
    global images
    result_width = images[0]['image'].size[0] * ZOOM_LEVELS[ZOOM][0]
    result_height = images[0]['image'].size[1] * ZOOM_LEVELS[ZOOM][0]
    result = Image.new('RGB', (result_width, result_height))
    for image in images:
        try:
            result.paste(
                im=image['image'],
                box=(
                    image['image'].size[0] * image['x'],
                    image['image'].size[1] * image['y']
                )
            )
        except Exception as e:
            logging.exception('Exception occurred: {}'.format(str(e)))
    images = list()

    filename = 'satelite_{}.jpg'.format(str(date))
    folder = BASE_LOCATION + '{}/'.format(date.strftime('%Y_%m_%d'))
    try:
        os.mkdir(folder)
    except OSError:
        pass  # folder exists
    result.save(folder + filename, 'JPEG')
    updateGnome(folder + filename, date)


def updateGnome(filename, date):
    if date == datetime.strptime(get_latest(), '%Y-%m-%d %H:%M:%S'):
        logging.info('Updating desktop')
        key_set = ['org.gnome.desktop.screensaver', 'org.gnome.desktop.background']
        env = {
            'DISPLAY': ':0'
        }
        for setting in key_set:
            print(
                (
                    'gsettings set {}'
                    ' picture-uri "file://{}"'.format(setting, filename)
                )
            )
            a = Popen(
                (
                    '. ~/.dbus/session-bus/* && '
                    'gsettings set {} picture-uri'
                    ' "file://{}"'.format(setting, filename)
                ),
                shell=True,
                executable='/bin/bash',
                env=env
            )
            a.communicate()


def create_video(date):
    os.system('''ffmpeg -pattern_type glob -i '*.jpg' -s 3840x2160 -r 8 movie.mp4''')


if __name__ == '__main__':
    if os.path.exists(LOCK_FILE):
        logging.info('already Running')
        print('exiting!')
        sys.exit(1)  # already running

    # Catch ctrl+c & clean up.
    # signal.signal(signal.SIGINT, exit_safely)

    # Create our lock & do stuff.
    open(LOCK_FILE, 'a').close()
    try:
        if len(sys.argv) > 1:
            get_all(get_latest(), int(sys.argv[1]))
        else:
            run(get_latest())
    except Exception:
        logging.exception('FAILED:')
    finally:
        exit_safely(None)
