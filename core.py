#!/usr/bin/env python

# coding: utf8

# from gevent import monkey
# monkey.patch_all()

import socket
import os, sys
import random, struct
import logging
from collections import deque, Counter, defaultdict

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.setLevel(logging.ERROR)


class UdpIpParser(object):
    """parse IP+UDP"""

    def __init__(self, data):
        self.data = data
        self.ip_hdrl = ip_hdrl = ((data[0]) & 0x0F) * 4
        self.udp_payload_len = struct.unpack(
            '!H',
            data[ip_hdrl + 4:ip_hdrl + 6])[0]

    @property
    def payload(self):
        udp_hdrl = 8
        return self.data[self.ip_hdrl + udp_hdrl:self.ip_hdrl + self.udp_payload_len]


class IpPacket(object):
    def __init__(self, data):
        self.data = data
        self.hdrl = (0x0F & (data[0])) * 4
        self.payload = self.data[self.hdrl:]
        self.ttl = self.data[8]

    @property
    def src_ip(self):
        return socket.inet_ntoa(str(self.data[12:16]))

    @property
    def dst_ip(self):
        return socket.inet_ntoa(str(self.data[16:20]))


class IcmpParser(object):
    hdrl = 8

    def __init__(self, data):
        self.data = data

    @property
    def type(self):
        return self.data[0]

    @property
    def payload(self):
        return self.data[8:14]

    @property
    def id(self):
        return struct.unpack('>H', self.data[4:6])[0]


def checksum(msg):
    # simplest rfc1071. msg is bytearray
    s = 0
    for i in range(0, len(msg), 2):
        w = msg[i] + (msg[i + 1] << 8)
        c = s + w
        s = (c & 0xffff) + (c >> 16)
    return ~s & 0xffff


def create_ping(id=None):
    id = id or random.randint(30000, 65500)
    icmp_type = 8
    icmp_code = 0
    icmp_checksum = 0
    icmp_seq = 1
    icmp_timestamp = 0
    data = '%06d' % id
    s = struct.Struct('!bbHHhQ%ss' % len(data))
    msg = bytearray(s.size)
    s.pack_into(
        msg, 0,
        icmp_type, icmp_code, icmp_checksum, id,
        icmp_seq, icmp_timestamp, data)
    # calculate ICMP checksum, which can not be offloaded
    cs = checksum(msg)
    struct.pack_into('<H', msg, 2, cs)
    return msg


def guess_hop(ttl):
    if not ttl:
        return
    if ttl >= 128:
        return 256 - ttl
    elif 64 < ttl < 128:
        return 128 - ttl
    else:
        return 64 - ttl

MAX_RETRY = 5


class Tracer(object):
    MAX_TTL = 32

    def __init__(self):
        """
        packet send rate = self.batch_size/self.timeout
         - hosts is iterable target IPs
        """
        self.batch_size = 100
        self.max_retry = 10
        self.timeout = 1
        self.running = self.timeout * self.max_retry

        self.max_ttl = defaultdict(lambda: self.MAX_TTL)

        self.echo_map = {}

        self.in_flight = deque(maxlen=self.batch_size)  # a list of ip-ttl tuples
        self.retries = Counter()  # remaining retries
        self.result = defaultdict(dict)  # {ip: [hop1, hop2, ...]}

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
        self.sock.bind(('', 0))
        self.sock.settimeout(self.timeout)

    def _iter_ip_and_ttl(self, hosts):
        """generate all IPs and their hops need to ping
        Need consider retries.
        """
        for ip in hosts:
            for ttl in xrange(5, self.MAX_TTL + 1):
                if ttl >= self.max_ttl[ip]:
                    break
                resp = (ip.strip(), ttl)
                self.in_flight.append(resp)
                yield resp

    def run(self, hosts):
        """would block"""
        self.ip_and_ttl = self._iter_ip_and_ttl(hosts)
        self.tick()
        while self.running > 0:
            data = bytearray(1024)
            try:
                nbytes, addr = self.sock.recvfrom_into(data)
                self.on_data(data, addr[0])
            except socket.timeout:
                self.tick()
        return self.result

    def _iter_retry(self):
        i = 0
        while self.in_flight and self.retries:
            if not i < len(self.in_flight):
                return
            key = self.in_flight[i]
            if self.retries[key] > 0:
                self.retries[key] -= 1
                yield key
                i += 1

            if self.retries[key] <= 0:
                self.on_retry_fail(*key)
                i -= 1

    def on_retry_fail(self, ip, ttl):
        self.retries.pop((ip, ttl), None)
        self.in_flight.remove((ip, ttl))
        if ttl <= self.max_ttl[ip]:
            self.result[ip][ttl] = '?'

    @property
    def on_tick(self):
        return getattr(self, '_on_tick', None) or (lambda *args: None)

    @on_tick.setter
    def on_tick(self, func):
        self._on_tick = func

    def tick(self):
        logger.debug('in_flight=%s, retries=%s', len(self.in_flight), self.retries.most_common(4))

        sent = 0
        for ip, ttl in self._iter_retry():
            self.ping(ip, ttl)
            sent += 1
            if sent >= self.batch_size:
                break

        while sent < self.batch_size:
            try:
                ip, ttl = self.ip_and_ttl.next()
            except StopIteration:
                self.running -= self.timeout
                return
            self.ping(ip, ttl)
            self.retries[(ip, ttl)] = self.max_retry
            sent += 1
        self.on_tick()

    def ping(self, ip, ttl):
        logger.debug("Ping %s, ttl=%s", ip, ttl)
        key = (ip, ttl)
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
        sock.bind(('', 0))
        sock.setsockopt(socket.SOL_IP, socket.IP_TTL, ttl)

        icmp_id = random.randint(30000, 60000)
        self.echo_map[icmp_id] = (ip, ttl)
        packet = create_ping(icmp_id)
        sock.sendto(packet, (ip, 0))
        sock.close()
        return icmp_id

    def pong(self, ping_ip, pong_ip, ttl):
        # @ToDo: handle multi-path trace-route
        if ping_ip == pong_ip:
            ttl = min(ttl, self.max_ttl[ping_ip])
            self.max_ttl[ping_ip] = ttl
            for k in xrange(1, self.MAX_TTL):
                ip = self.result[ping_ip].get(k)
                if k > ttl or ip == ping_ip:
                    self.result[ping_ip].pop(k, None)
                    key = ping_ip, ttl
                    try:
                        self.in_flight.remove(key)
                    except ValueError:
                        pass
                    self.retries.pop(key, None)
        else:
            key = ping_ip, ttl
            try:
                self.in_flight.remove(key)
            except ValueError:
                pass
            self.retries.pop(key, None)
        self.result[ping_ip][ttl] = pong_ip

    def on_data(self, data, addr):
        # get IP packet inside returned IP
        outer_ip = IpPacket(data)
        inner_ip = IpPacket(outer_ip.payload[IcmpParser.hdrl:])
        # the raw structure is: IP(ICMP(IP(ICMP)))
        icmp = IcmpParser(inner_ip.payload)
        icmp_id = None
        if icmp.payload.isdigit():
            icmp_id = int(icmp.payload)
        if not icmp_id:
            icmp_id = icmp.id
        if icmp_id in self.echo_map:
            ip, ttl = self.echo_map[icmp_id]
            logger.debug('Pong %s, ip=%s, hop=%s', ip, addr, ttl)
            # f.write('%s\t%s\t%s\n' % (ip, ttl, addr))
            self.pong(ip, addr, ttl)
        else:
            logger.debug('Pong unknown %s -> %s type %s' % (
                inner_ip.src_ip, inner_ip.dst_ip, icmp.type))


def get_hops(res):
    return [res.get(i) or '?' for i in xrange(max(res.keys()), 0, -1)]
