#! /usr/bin/python

#import requests
import urllib
from datetime import datetime, timedelta
import json
from PIL import Image
import cStringIO
import os, sys
import Queue
from threading import Thread
from subprocess import Popen
from shlex import split

BASE_URL = 'http://himawari8-dl.nict.go.jp/himawari8/img/'
BASE_URL += 'D531106'
ZOOM_LEVELS = [ (1,'1d'), (4,'4d'), (8,'8d'), (16,'16d'), (20,'20d') ]
ZOOM = 4
WIDTH = 550
MAX_RETRIES = 5
MAX_THREADS = 20
BASE_LOCATION = '/var/tmp/'
grid = Queue.Queue()
images = list()


def get_latest():
    response = urllib.urlopen(BASE_URL+'/latest.json').read()
    return json.loads(response)['date']


def get_all(end, days = 1):
    end = datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
    start = end - timedelta(days = days)
    start = datetime.strptime('2016-02-13 06:00:00', '%Y-%m-%d %H:%M:%S')
    while start < end:
        run(str(start))
        start = start + timedelta(minutes = 10)


def worker(base_url):
        while not grid.empty():
            tile = grid.get()
            tile['attempts'] += 1
            if tile['attempts'] > MAX_RETRIES:
                continue;
            response = None
            try:
                response = cStringIO.StringIO(\
                        urllib.urlopen(
                        base_url+'_'+tile['name']
                ).read() )
            except Exception as e:
                print e
                grid.put(tile)
                continue
            tile['image'] = Image.open(response)
            images.append(tile)
            grid.task_done()


def run(date):
    print date
    start = datetime.utcnow()
    date = datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
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
                'name' : str(x)+'_'+str(y)+'.png',
                'x' : x,
                'y' : y,
                'attempts' : 0
            })
    for i in range(MAX_THREADS):
        t = Thread(target=worker, args = ( base_url, ))
        t.daemon = True
        t.start()

    grid.join()
    print 'Retrieved %d images in %s seconds' % ( len(images), str((datetime.utcnow()-start).total_seconds()))
    stitch(date)


def stitch(date):
    global images
    result_width = images[0]['image'].size[0] * ZOOM_LEVELS[ZOOM][0]
    result_height = images[0]['image'].size[1] * ZOOM_LEVELS[ZOOM][0]
    result = Image.new('RGB', (result_width, result_height) )
    for image in images:
        result.paste(im = image['image'], box = ( image['image'].size[0] *
        image['x'], image['image'].size[1] * image['y'] ))
    images = list()

    filename = 'satelite_{}.jpg'.format(str(date))
    folder = BASE_LOCATION+'{}/'.format(date.strftime('%Y_%m_%d'))
    try:
        os.mkdir(folder)
    except OSError:
        pass #folder exists
    result.save(folder+filename, 'JPEG')
    updateGnome(folder+filename, date)


def updateGnome(filename, date):
    if date == datetime.strptime(get_latest(), '%Y-%m-%d %H:%M:%S'):
        print 'Updating desktop'
        a = Popen('. ~/.dbus/session-bus/* && gsettings set org.gnome.desktop.background picture-uri "file://{}"'.format(filename),
        shell = True)
        a.communicate()


def create_video(date):
    #ffmpeg -pattern_type glob -i '*.jpg' -s 3840x2160 -r 8 movie.mp4
    pass


if __name__ == '__main__':
    if len(sys.argv) > 1:
        get_all(get_latest(), 2)
    else:
        run( get_latest() )
