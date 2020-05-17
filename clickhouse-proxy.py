#!/usr/bin/env python3


#import 


from http.server import HTTPServer, BaseHTTPRequestHandler
import json


import clickhouse_driver


# TODO: check for uint range
SERVER_ADDRESS = 'localhost'
SERVER_PORT = int('8000')


CH_ADDRESS = '192.168.119.128'
CH_PORT = ''
CH_LOGIN = 'ionuser'
CH_PASSWORD = 'password'


def api_quality(req: dict) -> dict:
    resp = dict(req)
    return resp


def api_ssareas(req: dict) -> dict:
    resp = dict(req)
    return resp


def api_fadingstat(req: dict) -> dict:
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
    '/api/fading-stat':   api_fadingstat,
    '/api/errors':        api_errors,
    '/api/set-reference': api_setreference,
    '/api/status':        api_status,
    '/api/dcm-stats':     api_dcmstats
}


def db_connect():
    client = Client(host=CH_ADDRESS, user=CH_LOGIN, password=CH_PASSWORD)
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

        self._set_response()
        self.wfile.write(dumped.encode('utf-8'))    

if __name__ == '__main__':
    global db
    # TODO: Config parsing

    httpd = HTTPServer((SERVER_ADDRESS, SERVER_PORT), SimpleHTTPRequestHandler)
    httpd.serve_forever()

    db = db_connect()

