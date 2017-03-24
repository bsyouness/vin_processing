# -*- coding: utf-8 -*-

from __future__ import division
import csv, codecs, cStringIO
import requests
import simplejson as json
import pandas as pd
import click
from multiprocessing import Pool, cpu_count
import sys
from datetime import datetime
from collections import OrderedDict

# from https://docs.python.org/2/library/csv.html
class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") if type(s) == unicode
            else s for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def flatten(y):
    # from: https://medium.com/@amirziai/flattening-json-objects-in-python-f5343c794b10#.z1g4uvs4z
    def _flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                _flatten(x[a], name + a + '_')
        elif type(x) is list:
            for i, a in enumerate(x):
                _flatten(a, name + str(i) + '_')
        else:
            out[name[:-1]] = x
    out = {}
    _flatten(y)
    return out

def get_json(vin):
    base_url = 'https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvalues/{}?format=json' #&modelyear={}'
    url = base_url.format(vin)
    js = json.loads(requests.get(url).text)
    return OrderedDict(flatten(js))

def get_data_serial(vin_list):
    base_url = 'https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvalues/{}?format=json' #&modelyear={}'
    dict_list = []

    with click.progressbar(vin_list) as gb:
        for vin in gb:
            dict_list.append(get_json(vin))

    df = pd.DataFrame(dict_list)
    df.to_csv('out_serial.csv')

def get_data_parallel():
    p = Pool(cpu_count())
    n = len(vin_list)
    dict_list = []
    for i, js in enumerate(p.imap_unordered(get_json, vin_list), 1):
        dict_list.append(js)
        sys.stderr.write('\rDone {:.3%}'.format(i/n))

    # js_list = p.map(get_json, vin_list) 
    df = pd.DataFrame(dict_list)
    df.to_csv('out_parallel_in_memory.csv')

def get_data_parallel_stream(vin_list):
    p = Pool(cpu_count())
    n = len(vin_list)
    with open('out_parallel.csv', 'wb') as f:
        wr = UnicodeWriter(f, quoting=csv.QUOTE_ALL)
        wr.writerow(get_json(vin_list[0]).keys())
        for i, js in enumerate(p.imap_unordered(get_json, vin_list), 1):
            wr.writerow(js.values())
            sys.stderr.write('\rDone {:.3%}'.format(i/n))

if __name__ == "__main__":
    with open('txvpic.txt', 'r') as f:
        vins = f.readlines()
    # Get rid of the carriage return.
    vin_list = [vin[:-1] for vin in vins]
    # Truncate input
    # vin_list = vin_list[:100]

    t0 = datetime.now()
    get_data_parallel_stream(vin_list)
    t1 = datetime.now()
    print '\nParallel runtime: {:.3}'.format(t1-t0)