#!/usr/bin/python3

import os
import re
import time
from urllib import parse
import datetime
from http import client

URL = "http://rikortv.cdnvideo.ru/rr/rtp_r1_hq/playlist.m3u8"
OUTPUT = '/tmp/hls_capture'

def parse_not_comment(playlist):
    return (line for line in playlist.splitlines() if not line.startswith('#'))

def parse_target_duration(playlist):
    re_result = re.search('^#EXT-X-TARGETDURATION:(\d+)$', playlist, re.MULTILINE)
    if re_result:
        duration = float(re_result.groups()[0])
        print('Got target duration', duration)
        return duration
    else:
        print('Failed to get target duration')
        return None

def parse_media_sequence(playlist):
    re_result = re.search('^#EXT-X-MEDIA-SEQUENCE:(\d+)$', playlist, re.MULTILINE)
    if re_result:
        sn = int(re_result.groups()[0])
        print('Got media sequence', sn)
        return sn
    else:
        print('Failed to get media sequence')
        return None

def download(path):
#    download_url = 'http://' + url_host + '/' + path
#    if subprocess.getoutput('wget ' + download_url + ' -O /tmp/chunk').strip().endswith('ERROR 404: Not Found.'):
#        print('Retry')
#        return download(path)
#    else:
#        c = open('/tmp/chunk', 'rb')
#        data = c.read()
#        c.close()
#        return data
    global connection
    started_at = datetime.datetime.now()
    connection.request('GET', path)
    try:
        response = connection.getresponse()
    except Exception as ex:
        print('Exception while getting response:', str(ex), 'Connection is possibly closed on server-side. Reopening connection and retrying', path, 'download')
        connection.close()
        connection = client.HTTPConnection(url_host, strict=False)
        return download(path)
    if response.status == 200:
        print('Downloaded in', datetime.datetime.now() - started_at)
        return response.read()
    else:
        print('Status', response.status, response.reason, 'Retrying', path, 'download')
        return download(path)

url_parsed = parse.urlparse(URL)
url_host = url_parsed.netloc
directory, main_playlist_filename = os.path.split(url_parsed.path)
connection = client.HTTPConnection(url_host, strict=False)
capture = open(OUTPUT, 'wb')
prev_main_list = None
sequence_in_file = None
while True:
    t1 = datetime.datetime.now()
    main_list = download(os.path.join(directory, main_playlist_filename)).decode()
    if not prev_main_list == main_list:
        chunk_list_names = tuple(parse_not_comment(main_list))
        print('Loading chunk list', chunk_list_names[0])
        chunk_list = download(os.path.join(directory, chunk_list_names[0])).decode()
        duration = parse_target_duration(chunk_list)
        sequence = parse_media_sequence(chunk_list)
        if sequence_in_file and sequence_in_file + 1 < sequence:
            print('ERROR segments skipped. Last segment in file', sequence_in_file, 'First segment in playlist', sequence)
            exit(1)
        for chunk in parse_not_comment(chunk_list):
            if sequence_in_file and sequence_in_file >= sequence:
                print('Already loaded chunk', sequence)
                sequence += 1
                continue
            chunk_url = os.path.join(directory, chunk)
            print('Writing', chunk_url, 'segment number', sequence, 'to', OUTPUT)
            data = download(chunk_url)
            capture.write(data)
            sequence_in_file = sequence
            sequence += 1
        prev_main_list = main_list
        delta = datetime.datetime.now() - t1
        if duration:
            to_sleep = duration - delta.seconds
            if to_sleep > 0:
                print('Sleeping', to_sleep, 'seconds before refresh')
                time.sleep(to_sleep)
            else:
                print('Failed to download chunks within target duration limit, refreshing playlist')
        else:
            print('No target duration obtained, refreshing playlist immediately')
    else:
        if duration:
            print('Same playlist file, waiting half-duration, according to specs')
            time.sleep(duration / 2)

