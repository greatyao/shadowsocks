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
import time
import logging
import multiprocessing
import datetime
import uuid
import redis
import hashlib
from ctypes import Structure, c_int
from shadowsocks import shell

STREAM_CONNECT      = 0x01
STREAM_UP           = 0x02
STREAM_DOWN         = 0x04
STREAM_AGAIN        = 0x08
STREAM_CLOSE        = 0x10
STREAM_ERROR        = 0x20

MAX_OUT_BYTES = 1 << 19 #512KB
MAX_EXPIRE_TIME = 3600 * 24

class StatOfAccessObj(Structure):
    _fields_ = [('succeed', c_int), ('failed', c_int)]

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

    @classmethod
    def write_hourly_stat(cls, stat, date0, hour):
        logging.info('hourly statistics in %s %02d: succeed %d, failed %d'
                     %(date0, hour, stat.succeed, stat.failed))

    @classmethod
    def write_daily_stat(cls, stat, date0):
        logging.info('daily statistics in %s: succeed %d, failed %d'
                     %(date0, stat.succeed, stat.failed))

    def destroy(self):
        logging.info('(%s:%d, %s:%d) %d/%d bytes'
                     %(self.local_server, self.local_port, self.server_address, self.server_port,
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

    @classmethod
    def write_hourly_stat(cls, stat, date0, hour):
        BaseHandler.write_hourly_stat(stat, date0, hour)
        key0 = 'statistics:hourly:%s:%02d' %(date0, hour)
        try:
            pipe = RedisHandler.r.pipeline()
            RedisHandler.r.delete(key0)
            RedisHandler.r.rpush(key0, stat.succeed, stat.failed)
            pipe.execute()
        except Exception as ex:
            shell.print_exception(ex)

    @classmethod
    def write_daily_stat(cls, stat, date0):
        BaseHandler.write_daily_stat(stat, date0)
        key0 = 'statistics:daily:%s' %(date0)
        try:
            pipe = RedisHandler.r.pipeline()
            RedisHandler.r.delete(key0)
            RedisHandler.r.rpush(key0, stat.succeed, stat.failed)
            pipe.execute()
        except Exception as ex:
            shell.print_exception(ex)

    def destroy(self):
        if self.in_len == 0 and self.out_len == 0:
            return
        try:
            pipe = RedisHandler.r.pipeline()
            val =  '%d %s:%d %s:%d ' %(self.st, self.local_server, self.local_port, self.server_address, self.server_port)
            RedisHandler.r.set(self.key, val)
            RedisHandler.r.set(self.key_in, b''.join(self.indata))
            if self.md5 == None:
                RedisHandler.r.set(self.key_out, b''.join(self.outdata))
            else:
                val = 'datalen=%d digest=%s' %(self.out_len, self.md5.hexdigest())
                RedisHandler.r.set(self.key_out, val)
            pipe.execute()
        except Exception as ex:
            shell.print_exception(ex)
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

def process_stream_with_array(messages, global_stats, shared_hour, shared_day):
    handlers = {}
    HandlerClass = RedisHandler
    this_hour = last_hour = datetime.datetime.now().hour
    this_date = last_date = datetime.datetime.now().date()
    last_time = time.time()
    local_stats = [0] * 48

    def update_statistics(dt, last_date, last_hour, local_stats, global_stats,
                          shared_hour, shared_day, mask = 0, out_len = -1):
        now_date = dt.date()
        now_hour = dt.hour
        try:
            if last_date < now_date:
                with global_stats.get_lock():
                    global_stats[last_hour*2  ] += local_stats[last_hour*2  ]
                    global_stats[last_hour*2+1] += local_stats[last_hour*2+1]
                flag = False
                logging.info("%d: local hourly %d->%d: %d/%d" %(os.getpid(), last_hour, now_hour, local_stats[2*last_hour], local_stats[2*last_hour+1]))
                with shared_hour.get_lock():
                    shared_hour[last_hour] += 1
                    flag = (shared_hour[last_hour] % _cpu_count == 0)
                if flag:
                    stat = StatOfAccessObj(global_stats[last_hour*2], global_stats[last_hour*2+1])
                    HandlerClass.write_hourly_stat(stat, last_date, last_hour)
                flag = False
                with shared_day.get_lock():
                    shared_day[last_date.day] += 1
                    flag = (shared_day[last_date.day] % _cpu_count == 0)
                if flag:
                    stat = StatOfAccessObj(sum(global_stats[0::2]), sum(global_stats[1::2]))
                    HandlerClass.write_daily_stat(stat, last_date)
                    with global_stats.get_lock():
                        for i in range(len(global_stats)): global_stats[i] = 0
                for i in range(len(local_stats)): local_stats[i] = 0
                last_date = now_date
                last_hour = now_hour
            elif last_date == now_date and last_hour < now_hour:
                logging.info("%d: local hourly %d->%d: %d/%d" %(os.getpid(), last_hour, now_hour, local_stats[2*last_hour], local_stats[2*last_hour+1]))
                with global_stats.get_lock():
                    global_stats[last_hour*2  ] += local_stats[last_hour*2  ]
                    global_stats[last_hour*2+1] += local_stats[last_hour*2+1]
                flag = False
                with shared_hour.get_lock():
                    shared_hour[last_hour] += 1
                    flag = (shared_hour[last_hour] % _cpu_count == 0)
                if flag:
                    stat = StatOfAccessObj(global_stats[last_hour*2], global_stats[last_hour*2+1])
                    HandlerClass.write_hourly_stat(stat, now_date, last_hour)
                last_hour = now_hour
            if now_date == last_date and now_hour == last_hour and mask and out_len >= 0:
                if (mask & STREAM_ERROR) or (out_len == 0):
                    local_stats[last_hour*2+1] += 1
                else:
                    local_stats[last_hour*2  ] += 1
                #logging.info("%d: local hourly %d: %d/%d" %(os.getpid(), last_hour, local_stats[2*last_hour], local_stats[2*last_hour+1]))
        except Exception as ex:
            logging.warn("update_statistics")
            shell.print_exception(ex)
        finally:
            return (last_date, last_hour)

    while True:
        try:
            if time.time() - last_time >= 2:
                this_dt = datetime.datetime.now()
                last_date, last_hour = update_statistics(this_dt, last_date, last_hour,
                                                         local_stats, global_stats,
                                                         shared_hour, shared_day)
                last_time = time.time()
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
                dt = datetime.datetime.fromtimestamp(m.st)
                last_date, last_hour = update_statistics(dt, last_date, last_hour,
                                                         local_stats, global_stats,
                                                         shared_hour, shared_day,
                                                         mask = one.type, out_len = m.out_len)
                m.destroy()
                del handlers[k]
            del one
        except Exception as e:
            logging.warn("process_stream")
            shell.print_exception(e)
            pass

def process_stream_with_manager(messages, d, lock):
    handlers = {}
    HandlerClass = RedisHandler
    this_hour = last_hour = datetime.datetime.now().hour
    this_date = last_date = datetime.datetime.now().date()
    last_time = time.time()
    manager = multiprocessing.Manager()

    def update_statistics(dt, last_date, last_hour, d, lock, mask = 0, out_len = -1):
        now_date = dt.date()
        now_hour = dt.hour
        try:
            lock.acquire()
            if d.get(now_date, None) == None:
                d[now_date] =  manager.list([0]*48)
            this_dict = d[now_date]
            if last_date < now_date:
                last_dict = d[last_date]
                stat = StatOfAccessObj(last_dict[last_hour*2], last_dict[last_hour*2+1])
                HandlerClass.write_hourly_stat(stat, last_date, last_hour)
                stat = StatOfAccessObj(sum(last_dict[0::2]), sum(last_dict[1::2]))
                HandlerClass.write_daily_stat(stat, last_date)
                last_date = now_date
                last_hour = now_hour
            elif last_hour < now_hour:
                stat = StatOfAccessObj(this_dict[last_hour*2], this_dict[last_hour*2+1])
                HandlerClass.write_hourly_stat(stat, now_date, last_hour)
                last_hour = now_hour
            if now_date == last_date and now_hour == last_hour and mask and out_len >= 0:
                if (mask & STREAM_ERROR) or (out_len == 0):
                    this_dict[last_hour*2+1] += 1
                else:
                    this_dict[last_hour*2  ] += 1
                #logging.info("%d: %d hour %d/%d" %(os.getpid(), last_hour, this_dict[2*last_hour], this_dict[2*last_hour+1]))
        except Exception as ex:
            shell.print_exception(ex)
        finally:
            lock.release()
            return (last_date, last_hour)

    while True:
        try:
            if time.time() - last_time >= 2:
                this_dt = datetime.datetime.now()
                this_date, this_hour = update_statistics(this_dt, this_date, this_hour, d, lock)
                last_time = time.time()
                if this_dt.minute == 59 and this_dt.second >= 58:
                    logging.info("pid=%d statistics of hour %d: %d/%d"
                                 %(os.getpid(), this_hour, d[this_date][2*this_hour], d[this_date][2*this_hour+1]))
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
                dt = datetime.datetime.fromtimestamp(m.st)
                last_date, last_hour = update_statistics(dt, last_date, last_hour, d, lock,
                                                         mask = one.type, out_len = m.out_len)
                m.destroy()
                del handlers[k]
            del one
        except Exception as e:
            shell.print_exception(e)
            pass
    logging.warn("stream handler worker exit pid=%d" %(os.getpid()))

_cpu_count = multiprocessing.cpu_count()
_msg_queue = [multiprocessing.Queue() for x in range(_cpu_count)]
_process = []
_global_stats = multiprocessing.Array('i', [0] * 48, lock = True)
_shared_hour = multiprocessing.Array('i', [0] * 24, lock = True)
_shared_day = multiprocessing.Array('i', [0] * 32, lock = True)

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
    global _shared_hour
    global _shared_day
    global _global_stats

    #manager = multiprocessing.Manager()
    #d = manager.dict()
    #lock = multiprocessing.Lock()
    for i in range(_cpu_count):
        #p = multiprocessing.Process(target=process_stream_with_manager, args=(_msg_queue[i], d, lock))
        p = multiprocessing.Process(target=process_stream_with_array,
                                    args=(_msg_queue[i], _global_stats, _shared_hour, _shared_day))
        p.start()
        logging.info('stream handler worker started pid=%d' %(p.ident))
        _process.append(p)

def kill_stream_handler():
    global _process

    for p in _process:
        try:
            p.terminate()
        except OSError:
            pass

def stop_stream_handler():
    global _process

    for p in _process:
        try:
            p.join()
        except Exception as ex:
            logging.warn("pid=%d child exit unexcepted" %(p.ident))
            logging.error(ex)

