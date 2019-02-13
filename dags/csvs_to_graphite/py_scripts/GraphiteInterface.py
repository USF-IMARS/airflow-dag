"""
Based on https://gist.github.com/agleyzer/8697616
"""
import pickle
import socket
import struct
import threading
import time
try:
    from urllib.request import urlopen
    from urllib.request import Request
except ImportError:
    from urllib2 import urlopen
    from urllib2 import Request


class GraphiteInterface(object):

    def __init__(self, host, port, event_url=None):
        '''Initialize Carbon Interface.
        host: host where the carbon daemon is running
        port: port where carbon daemon is listening for pickle protocol on host
        event_url: web app url where events can be added.
            It must be provided if add_event(...) is to be used.
            Otherwise an exception by urllib will raise.
        '''
        self.host = host
        self.port = port
        self.event_url = event_url
        self.__data = []
        self.__data_lock = threading.Lock()

    def add_data(self, metric, value, ts=None):
        if not ts:
            ts = time.time()
        if self.__data_lock.acquire():
            self.__data.append((metric, (ts, value)))
            self.__data_lock.release()
            return True
        return False

    def add_data_dict(self, dd):
        '''
        dd must be a dictionary where keys are the metric name,
        each key contains a dictionary which at least must have
        'value' key (optionally 'ts')

        dd = {
            'experiment1.subsystem.block.metric1': {
                'value': 12.3, 'ts': 1379491605.55
            },
            'experiment1.subsystem.block.metric2': {
                'value': 1.35
            },
            ...
        }
        '''
        if self.__data_lock.acquire():
            for k, v in dd.items():
                ts = v.get('ts', time.time())
                value = v.get('value')
                self.__data.append((k, (ts, value)))
            self.__data_lock.release()
            return True
        return False

    def add_data_list(self, dl):
        '''
        dl must be a list of tuples like:
        dl = [('metricname', (timestamp, value)),
              ('metricname', (timestamp, value)),
              ...]
        '''
        if self.__data_lock.acquire():
            self.__data.extend(dl)
            self.__data_lock.release()
            return True
        return False

    def send_data(self, data=None, verbose=False):
        '''
        If data is empty, current buffer is sent. Otherwise data must be like:
        data = [('metricname', (timestamp, value)),
              ('metricname', (timestamp, value)),
              ...]

        returns
        -------
        False if send failed, else number of bytes sent.
        '''
        save_in_error = False
        if not data:
            if self.__data_lock.acquire():
                data = self.__data
                self.__data = []
                save_in_error = True
                self.__data_lock.release()
            else:
                return False
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        payload = pickle.dumps(data)
        header = struct.pack("!L", len(payload))
        message = header + payload
        s.connect((self.host, self.port))
        try:
            s.send(message)
        except:
            print('Error when sending data to carbon')
            if save_in_error:
                self.__data.extend(data)
            return False
        else:
            if verbose:
                print(
                    'Sent data to {host}:{port}: {0} metrics {1} bytes'.format(
                        len(data), len(message), host=self.host, port=self.port
                    )
                )
                # print(data)
            return len(message)
        finally:
            s.close()

    def add_event(self, what, data=None, tags=None, when=None):
        if not when:
            when = time.time()
        postdata = '{{"what":"{0}", "when":{1}'.format(what, when)
        if data:
            postdata += ', "data":"{0}"'.format(str(data))
        if tags:
            postdata += ', "tags": "{0}"'.format(str(tags))
        postdata += '}'
        req = Request(self.url_post_event)
        req.add_data(postdata)

        try:
            urlopen(req)
        except Exception as ex:
            print('Error when sending event to carbon:\n\t{}'.format(ex))
            pass
