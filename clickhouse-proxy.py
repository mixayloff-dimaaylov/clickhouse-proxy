#!/usr/bin/env python3


#import 


from http.server import HTTPServer, BaseHTTPRequestHandler
import json


import clickhouse_driver

from math import pow as pow
from datetime import datetime


# TODO: check for uint range
SERVER_ADDRESS = ''
SERVER_PORT = int('8000')


CH_ADDRESS = '192.168.119.128'
CH_PORT = ''
CH_LOGIN = 'ionuser'
CH_PASSWORD = 'password'


# Math functions
def get_bit(bits: int, position: int):
    return int(bits / pow(2, position)) & 0x01


#
# Decode Bounding Box Integer
#
# Decode hash number into a bound box matches it. Data returned in a four-element array: [minlat, minlon, maxlat, maxlon]
# @param {Number} hashInt
# @param {Number} bitDepth
# @returns {Array}
#
def decode_bbox_int(hashInt: int, bitDepth=52):
    maxLat = 90
    minLat = -90
    maxLon = 180
    minLon = -180

    latBit = 0
    lonBit = 0
    step = bitDepth // 2

    for i in range(0, step):
        lonBit = get_bit(hashInt, ((step - i) * 2) - 1)
        latBit = get_bit(hashInt, ((step - i) * 2) - 2)

        if latBit == 0:
            maxLat = (maxLat + minLat) / 2
        else:
            minLat = (maxLat + minLat) / 2

        if lonBit == 0:
            maxLon = (maxLon + minLon) / 2
        else:
            minLon = (maxLon + minLon) / 2

    return [int(minLat), int(minLon), int(maxLat), int(maxLon)]


#
# Decode Integer
#
# Decode a hash number into pair of latitude and longitude. A javascript object is returned with keys `latitude`,
# `longitude` and `error`.
# @param {Number} hash_int
# @param {Number} bitDepth
# @returns {Object}
#
def decode_int(hash_int, bitDepth):
    bbox = decode_bbox_int(hash_int, bitDepth)
    lat = (bbox[0] + bbox[2]) / 2
    lon = (bbox[1] + bbox[3]) / 2
    latErr = bbox[2] - lat
    lonErr = bbox[3] - lon

    return {
             "latitude":  float(lat),
             "longitude": float(lon),
             "error": {
                         "latitude":  float(latErr),
                         "longitude": float(lonErr)
                      }
           }

# Requests handlers

def api_quality(req: dict) -> dict:
    resp = dict(req)
    return resp


def api_ssareas(req: dict) -> dict:
    # Base resp fields
    query_id = str(req['query_id'])

    # Get current time
    t_to = int(datetime.now().timestamp() * 1000)
    t_from = t_to - 10000 * 10

    req_str = """
select *
from (
    SELECT
        time,
        sat,
        ionpoint
    FROM
        rawdata.satxyz2
    WHERE
        time BETWEEN %i and %i
) as satxyz
ANY INNER JOIN (
    SELECT
        time,
        sat,
        avgNT,
        sigNT,
        s4
    from (
        SELECT
            toUInt64(floor(time/10000,0)*10000) as time,
            sat,
            avg(abs(sigNT)) as sigNT
        FROM
            computed.xz1
        WHERE
            time BETWEEN %i and %i
        group by
            time,
            sat
    ) as sigNT
    ANY INNER JOIN (
        SELECT
            time,
            sat,
            avgNT,
            s4
        from (
            SELECT
                toUInt64(floor(time/10000,0)*10000) as time,
                sat,
                avg(abs(avgNT)) as avgNT
            FROM
                computed.NTDerivatives
            WHERE
                time BETWEEN %i and %i
            group by
                time,
                sat
        ) as avgNT
        ANY INNER JOIN (
            SELECT
                toUInt64(floor(time/10000,0)*10000) as time,
                sat,
                avg(s4) as s4
            FROM
                computed.s4
            WHERE
                time BETWEEN %i and %i
            group by
                time,
                sat
        ) as S4
        USING time, sat
    ) as avgNT_S4
    USING time, sat
) as avgNT_sigNT_S4
USING time, sat
LIMIT 100
""" % (t_from, t_to, t_from, t_to, t_from, t_to, t_from, t_to)

    resp = db.execute(req_str)

    areas = []
    for item in resp:
        intensity = item[5]

        sats = []

        if intensity <= 0.01:
            sat_intensity = 1
        elif intensity <= 0.05:
            sat_intensity = 2
        elif intensity <= 0.2:
            sat_intensity = 3
        elif intensity <= 1.0:
            sat_intensity = 4

        sats.append(
            {
                "name":      item[1],
                "lat":       decode_int(item[2], 52)["latitude"],
                "lon":       decode_int(item[2], 52)["longitude"],
                "tec_dev":   item[3],
                "tec_mean":  item[4],
                "intensity": sat_intensity
            }
        )

        if intensity <= 0.01:
            radius = 50000
        elif intensity <= 0.05:
            radius = 100000
        elif intensity <= 0.2:
            radius = 150000
        elif intensity <= 1.0:
            radius = 200000

        # TODO:
        area = {}
        area['timestamp']          = item[0]
        area['Semi_major_axes']    = int(radius / 1000)
        area['Semi_minor_axes']    = int(radius * 1.5 / 1000)
        area['distance_to_centre'] = 270
        area['azimut']             = 43.2
        area['angle']              = 23.0
        area['sat']                = sats

        areas.append(area)

    resp_obj = {}
    resp_obj['query_id'] = query_id
    resp_obj['areas']    = areas

    return resp_obj


def api_fadingstats(req: dict) -> dict:
    resp = dict(req)
    return resp


def api_errors(req: dict) -> dict:
    resp = dict(req)
    return resp


def api_setreference(req: dict) -> dict:
    resp = dict(req)
    return resp


def api_status(req: dict) -> dict:
    # TODO: add clichouse remote check
    resp = {"query_id": req["query_id"], "status": "OK"}
    return resp


def api_dcmstats(req: dict) -> dict:
    resp = dict(req)
    return resp


cases = {
    '/api/quality':       api_quality,
    '/api/ss-areas':      api_ssareas,
    '/api/fading-stats':  api_fadingstats,
    '/api/errors':        api_errors,
    '/api/set-reference': api_setreference,
    '/api/status':        api_status,
    '/api/dcm-stats':     api_dcmstats
}


def db_connect():
    client = clickhouse_driver.Client(host=CH_ADDRESS, user=CH_LOGIN, password=CH_PASSWORD)
    return client


# TODO: change to Threaded*
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    def _set_response(self, code: int, content):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        self.wfile.write(content)

    def do_POST(self):
        if self.path not in cases:
            self._set_response(404, '{}'.encode('utf-8'))
            return

        content_length = int(self.headers['Content-Length'])

        if content_length is 0:
            self._set_response(404, '{}'.encode('utf-8'))
            return

        post_data = self.rfile.read(content_length)
        decoded = post_data.decode('utf-8')

        resp = cases[self.path](json.loads(decoded))

        dumped = json.dumps(resp)

        self._set_response(200, dumped.encode('utf-8'))

if __name__ == '__main__':
    global db
    # TODO: Config parsing

    db = db_connect()

    httpd = HTTPServer((SERVER_ADDRESS, SERVER_PORT), SimpleHTTPRequestHandler)
    httpd.serve_forever()

