# r - means run
# rxy - x means which language c=c r=rust n=nim
# rxy - y is either recv or send which will either setup the library to receive data or send it
# goal? as tooling is built up till it will possibly allow for test automation?

# instructions run a recv then a sender

rcrecv:
	cd libutp && make
	./libutp/ucat-static -ddddd -l -p 9078

rcsend:
	cd libutp && make
	./libutp/portal-test

rrsend:
	cd rust-utp && RUST_LOG=trace cargo run -- send

rrrecv:
	cd rust-utp && RUST_LOG=trace cargo run -- recv

rnsend:
	cd nim-utp && nimble c src/send.nim
	./nim-utp/src/send

# currently doesn't work I need to understand nim syntax more
rnrecv:
	cd nim-utp && nimble c src/recv.nim
	./nim-utp/src/recv

clean:
	rm nim-utp/src/send nim-utp/src/recv -f
	rm rust-utp/target -rf
	cd libutp && make clean