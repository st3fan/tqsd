#!/usr/bin/env python

import sys
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
    return requests.get("http://127.0.0.1:8080/queues/%s/messages?MaxNumberOfMessages=%d" % (name, n))

def delete_lease(queue_name, lease_id):
    return requests.delete("http://127.0.0.1:8080/queues/%s/leases/%s" % (queue_name, lease_id))

if __name__ == "__main__":

    if sys.argv[1] == "create":
        r = create_queue("test")
        assert r.status_code == 200

    if sys.argv[1] == "fill":
        for i in range(0,10):
            r = put_messages("test", [{"Body": "Hello, world!"} for j in range(25)])
            r.raise_for_status()

    if sys.argv[1] == "drain":
        while True:
            r = get_messages("test", 20)
            r.raise_for_status()
            j = r.json()
            if len(j["Messages"]) == 0:
                   break
            for lease in j["Leases"]:
                r = delete_lease("test", lease["ID"])
                r.raise_for_status()
            print("Got some")
