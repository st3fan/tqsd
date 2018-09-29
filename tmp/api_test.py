#!/usr/bin/env python

import time
import requests

def create_queue(name):
    return requests.post("http://127.0.0.1:8080/queues", json={"Name": name})

def delete_queue(name):
    return requests.delete("http://127.0.0.1:8080/queues/%s" % name)

def get_queues():
    return requests.get("http://127.0.0.1:8080/queues")

def put_messages(name, messages):
    return requests.post("http://127.0.0.1:8080/queues/%s/messages" % name, json={"Messages": messages})

def get_messages(name, n):
    r = requests.get("http://127.0.0.1:8080/queues/%s/messages?MaxNumberOfMessages=%d" % (name, n))
    r.raise_for_status()
    j = r.json()
    assert type(j) == dict
    assert "Messages" in j
    assert j["Messages"] is not None
    assert type(j["Messages"]) == list
    for message in j["Messages"]:
        assert type(message) == dict
        assert "Body" in message
        assert type(message["Body"]) is str
    return r, r.json()

#

def test_create_queue():
    r = create_queue("test")
    assert r.status_code == 200

def test_get_queues():
    r = get_queues()
    assert r.status_code == 200
    j = r.json()
    print(j)
    assert type(j) == list
    assert len(j) == 1
    assert 'Name' in j[0]
    assert j[0]["Name"] == "test"

def test_put_messages():
    r = put_messages("test", [{"Body": "This is message #%d" % i} for i in range(5)])
    assert r.status_code == 200

def test_get_messages():
    r, j = get_messages("test", 4)
    assert len(j["Messages"]) == 4
    assert j["Messages"][0]["Body"] == "This is message #0"
    assert j["Messages"][1]["Body"] == "This is message #1"
    assert j["Messages"][2]["Body"] == "This is message #2"
    assert j["Messages"][3]["Body"] == "This is message #3"
    r, j = get_messages("test", 4)
    assert len(j["Messages"]) == 1
    assert j["Messages"][0]["Body"] == "This is message #4"
    r, j = get_messages("test", 25)
    assert len(j["Messages"]) == 0

def test_lease_expiration():
    time.sleep(35)
    r, j = get_messages("test", 25)
    assert len(j["Messages"]) == 5
    assert j["Messages"][0]["Body"] == "This is message #0"
    assert j["Messages"][1]["Body"] == "This is message #1"
    assert j["Messages"][2]["Body"] == "This is message #2"
    assert j["Messages"][3]["Body"] == "This is message #3"
    assert j["Messages"][4]["Body"] == "This is message #4"

def test_message_expiration():
    time.sleep(95) # Lease expiration plus message expiration
    r, j = get_messages("test", 25)
    assert len(j["Messages"]) == 0

def test_delete_queue():
    r = delete_queue("test")
    assert r.status_code == 200
