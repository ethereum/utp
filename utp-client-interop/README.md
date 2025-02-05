# utp interop testing

if you are trying to use C, Rust, Nim's client it will be assumed you have there respective development depends installed.

### Test ethereum/utp  send to bittorrent/libutp

Terminal 1
```
make rcrecv
```

Terminal 2
```
make rrsend
```

### Test  bittorrent/libutp send to ethereum/utp

Terminal 1
```
make rrrecv
```

Terminal 2
```
make rcsend
```

### Test bittorrent/libutp send to bittorrent/libutp

Terminal 1
```
make rcrecv
```

Terminal 2
```
make rcsend
```

### etc, etc