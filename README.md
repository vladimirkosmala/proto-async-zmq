Prototype of async client/server load-balanced 0MQ arch

- one client send request (path of requested object)
- server proxy it to one worker
- response goes to client (path, http code, payload)
