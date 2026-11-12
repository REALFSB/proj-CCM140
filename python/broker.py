"""Broker REQ/REP: ROUTER (clientes, 5555) <-> DEALER (servidores, 5556), round-robin."""
import zmq

ctx = zmq.Context()
frontend = ctx.socket(zmq.ROUTER)
frontend.bind("tcp://*:5555")
backend = ctx.socket(zmq.DEALER)
backend.bind("tcp://*:5556")

print("[broker] frontend (clientes) :5555 | backend (servidores) :5556", flush=True)
zmq.proxy(frontend, backend)
