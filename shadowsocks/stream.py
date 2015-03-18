#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2015 YAO Wei
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
import sys
import time
import logging
import multiprocessing
import random
import uuid
import redis
import hashlib
from shadowsocks import shell

STREAM_CONNECT      = 0x01
STREAM_UP           = 0x02
STREAM_DOWN         = 0x04
STREAM_AGAIN        = 0x08
STREAM_CLOSE        = 0x10

MAX_OUT_BYTES = 1 << 19 #512KB
MAX_EXPIRE_TIME = 3600 * 24

class BaseHandler(object):
    def __init__(self, local_server, local_port, server_address, server_port):
        self.indata = []
        self.outdata = []
        self.st = int(time.time())
        self.local_server = local_server
        self.local_port = local_port
        self.server_address = server_address
        self.server_port = server_port
        self.seq = 1
        self.in_len = 0
        self.out_len = 0
        self.md5 = None

    def write(self, data, type):
        '''
        if type & STREAM_AGAIN:
            self.infd.close()
            self.outfd.close()
            self.seq += 1
        '''
        if type & STREAM_UP:
            self.in_len += len(data)
            self.indata.append(data)
        elif type & STREAM_DOWN:
            self.out_len += len(data)
            if self.out_len <= MAX_OUT_BYTES:
                self.outdata.append(data)
            else:
                if(self.md5 == None):
                    self.md5 = hashlib.md5(b''.join(self.outdata))
                    self.outdata = []
                self.md5.update(data)
        elif type & STREAM_CLOSE:
            pass


    def destroy(self):
        logging.info('%d: (%s:%d <--> %s:%d) %d/%d bytes'
                     %(os.getpid(), self.local_server, self.local_port, self.server_address, self.server_port,
                       self.in_len, self.out_len))
        del self.indata
        del self.outdata

class WriteFileHandler(BaseHandler):
    def __init__(self, local_server, local_port, server_address, server_port):
        BaseHandler.__init__(self,  local_server, local_port, server_address, server_port)
        self.day = time.strftime('%Y%m%d', time.localtime(self.st))
        try:
            os.mkdir(self.day)
        except OSError:
            pass
        self.folder = self.day + '/%d_%s_%s_%s'%(self.st, local_server, server_address, str(uuid.uuid1()))
        os.mkdir(self.folder, 0777)
        self.fd0 = open(self.folder + "/endpoint", "wb")
        self.fd0.write('%d %s:%d %s:%d ' %(self.st, local_server, local_port, server_address, server_port))
        self.infile = self.folder + "/in"
        self.outfile = self.folder + "/out"
        self.infd  = open(self.infile + ".dat", "wb")
        self.outfd = open(self.outfile + ".dat", "wb")

    def write(self, data, type):
        BaseHandler.write(self, data, type)

        '''
        if type & STREAM_AGAIN:
            self.infd  = open('%s-%d.dat' %(self.infile, self.seq), "wb")
            self.outfd = open('%s-%d.dat' %(self.outfile, self.seq), "wb")
       '''
        if type & STREAM_UP:
            self.infd.write(data)
            self.infd.flush()
        elif type & STREAM_DOWN:
            if self.out_len <= MAX_OUT_BYTES:
                self.outfd.write(data)
            else:
                self.outfd.truncate(0)
                self.md5.update(data)

    def destroy(self):
        self.fd0.close()
        self.infd.close()
        if self.md5 != None:
            self.outfd.write('datalen=%d digest=%s' %(self.out_len, self.md5.hexdigest()))
        self.outfd.close()
        BaseHandler.destroy(self)

class RedisHandler(BaseHandler):
    r = redis.Redis("127.0.0.1")

    def __init__(self, local_server, local_port, server_address, server_port):
        BaseHandler.__init__(self, local_server, local_port, server_address, server_port)
        self.key = '%s:%d:%s_%d_%d' %(server_address, self.st, local_server, local_port, server_port)
        self.key_in = self.key + "_in"
        self.key_out = self.key + "_out"

    def write(self, data, type):
        BaseHandler.write(self, data, type)

    def destroy(self):
        if self.in_len == 0 and self.out_len == 0:
            return
        pipe = RedisHandler.r.pipeline()
        val =  '%d %s:%d %s:%d ' %(self.st, self.local_server, self.local_port, self.server_address, self.server_port)
        RedisHandler.r.set(self.key, val, ex = MAX_EXPIRE_TIME)
        RedisHandler.r.set(self.key_in, b''.join(self.indata), ex = MAX_EXPIRE_TIME)
        if self.md5 == None:
            RedisHandler.r.set(self.key_out, b''.join(self.outdata), ex = MAX_EXPIRE_TIME)
        else:
            val = 'datalen=%d digest=%s' %(self.out_len, self.md5.hexdigest())
            RedisHandler.r.set(self.key_out, val, ex = MAX_EXPIRE_TIME)
        pipe.execute()
        BaseHandler.destroy(self)

class StreamData(object):
    def __init__(self, data, local_server, local_port, server_address, server_port, type):
        self.data = data
        self.local_server = local_server
        self.local_port = local_port
        self.server_address = server_address
        self.server_port = server_port
        self.type = type
        self.st = int(time.time())

def process_stream(messages):
    handlers = {}
    HandlerClass = RedisHandler
    while True:
        try:
            if messages.empty():
                time.sleep(0.1)
                continue
            one = messages.get()
            k = '%s_%d_%s_%d' %(one.local_server, one.local_port, one.server_address , one.server_port)
            m = handlers.get(k, None)
            if m == None or (one.type & STREAM_CONNECT):
                m = HandlerClass(one.local_server, one.local_port, one.server_address , one.server_port)
                handlers[k] = m
            m.write(one.data, one.type)
            if one.type & STREAM_CLOSE:
                m.destroy()
                del handlers[k]
            del one
        except Exception as e:
            shell.print_exception(e)
            pass

_cpu_count = multiprocessing.cpu_count()
_msg_queue = [multiprocessing.Queue() for x in range(_cpu_count)]
_process = []

def add_stream(data, local_server, local_port, server_address, server_port, type):
    global _msg_queue

    #idx = random.randint(0, _cpu_count-1)
    idx = (hash(local_server) + local_port) % _cpu_count
    one = StreamData(data, local_server, local_port, server_address, server_port, type)
    _msg_queue[idx].put(one)

def children_of_stream_handler():
    global _process
    
    return [p.ident for p in _process]


def start_stream_handler():
    global _process
    global _msg_queue

    for i in range(_cpu_count):
        p = multiprocessing.Process(target=process_stream, args=(_msg_queue[i],))
        p.start()
        _process.append(p)

def stop_stream_handler():
    global _process

    for p in _process:
        p.terminate()
