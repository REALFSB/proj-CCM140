"""Proxy Pub/Sub: servidores publicam em :5557 (XSUB), bots assinam em :5558 (XPUB)."""
import zmq

ctx = zmq.Context()
xsub = ctx.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")
xpub = ctx.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("[proxy] XSUB (publicadores) :5557 | XPUB (assinantes) :5558", flush=True)
zmq.proxy(xsub, xpub)
