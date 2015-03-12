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
from shadowsocks import shell

class StreamData(object):
    def __init__(self, data, local_server, local_port, server_address, server_port, up):
        self.data = data
        self.local_server = local_server
        self.local_port = local_port
        self.server_address = server_address
        self.server_port = server_port
        self.up = up
        self.st = int(time.time())

class StreamOutput(object):

    @classmethod
    def write(cls, one):
        if(one.up == 1):
            logging.info('%d: %s:%d --> %s:%d %d bytes'
                         %(one.st, one.local_server, one.local_port, one.server_address, one.server_port, len(one.data)))
        else:
            logging.info('%d: %s:%d --> %s:%d %d bytes'
                        %(one.st, one.server_address, one.server_port, one.local_server, one.local_port, len(one.data)))
        

def process_stream(messages):
    while True:
        try:
            if messages.empty():
                time.sleep(0.1)
                continue
            one = messages.get()
            StreamOutput.write(one)
            del one
        except Exception as e:
            shell.print_exception(e)
            pass

_cpu_count = multiprocessing.cpu_count()
_msg_queue = [multiprocessing.Queue() for x in range(_cpu_count)]
_process = []

def add_stream(data, local_server, local_port, server_address, server_port, up):
    global _msg_queue

    #idx = random.randint(0, _cpu_count-1)
    idx = (hash(local_server) + local_port) % _cpu_count
    one = StreamData(data, local_server, local_port, server_address, server_port, up)
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

'''
class StreamHandler(object):
    _msg_queue = [multiprocessing.Queue() for x in range(_cpu_count)]
    _process = []

    @classmethod
    def insert(cls, data, local_server, local_port, server_address, server_port, up):
        idx = (hash(local_server) + local_port) % _cpu_count
        #idx = random.randint(0, _cpu_count-1)
        cls._msg_queue[idx].put(StreamData(data, local_server, local_port, server_address, server_port, up))

    @classmethod
    def start(cls):
        for i in range(_cpu_count):
            p = multiprocessing.Process(target=process_stream, args=(cls._msg_queue[i],))
            p.start()
            cls._process.append(p)

    @classmethod
    def stop(cls):
        for p in cls._process:
            p.terminate()

    @classmethod
    def children(cls):
        return [p.ident for p in cls._process]

add_stream = StreamHandler.insert
start_stream_handler = StreamHandler.start
stop_stream_handler = StreamHandler.stop
children_of_stream_handler = StreamHandler.children
'''
