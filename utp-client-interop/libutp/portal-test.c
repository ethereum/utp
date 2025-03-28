#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <poll.h>
#include <netdb.h>
#include <signal.h>

#ifdef __linux__
	#include <linux/errqueue.h>
	#include <netinet/ip_icmp.h>
#endif

#include "utp.h"

// options
int o_debug = 10;
char *o_local_address,  *o_local_port,
        *o_remote_address, *o_remote_port;
int o_listen;
int o_buf_size = 4096;
int o_numeric;

utp_context *ctx;
utp_socket *s;

int fd;
int buf_len = 0;
unsigned char *buf, *p;
int eof_flag, utp_eof_flag, utp_shutdown_flag, quit_flag, exit_code;

void die(char *fmt, ...)
{
    va_list ap;
    fflush(stdout);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    exit(1);
}

void debug(char *fmt, ...)
{
    va_list ap;
    if (o_debug) {
        fflush(stdout);
        fprintf(stderr, "debug: ");
        va_start(ap, fmt);
        vfprintf(stderr, fmt, ap);
        va_end(ap);
        fflush(stderr);
    }
}

void pdie(char *err)
{
    debug("errno %d\n", errno);
    fflush(stdout);
    perror(err);
    exit(1);
}

void hexdump(const void *p, size_t len)
{
    int count = 1;

    while (len--) {
        if (count == 1)
            fprintf(stderr, "    %p: ", p);

        fprintf(stderr, " %02x", *(unsigned char*)p++ & 0xff);

        if (count++ == 16) {
            fprintf(stderr, "\n");
            count = 1;
        }
    }

    if (count != 1)
        fprintf(stderr, "\n");
}

void write_data(void)
{
    if (!s)
        goto out;

    while (p < buf+buf_len) {
        size_t sent;

        sent = utp_write(s, p, buf+buf_len-p);
        if (sent == 0) {
            debug("socket no longer writable\n");
            return;
        }

        p += sent;

        if (p == buf+buf_len) {
            debug("wrote %zd bytes; buffer now empty\n", sent);
            p = buf;
            buf_len = 0;
        }
        else
            debug("wrote %zd bytes; %d bytes left in buffer\n", sent, buf+buf_len-p);
    }

    out:
    if (buf_len == 0 && eof_flag) {
        if (s) {
            debug("Buffer empty, and previously found EOF.  Shutdown socket\n");
            utp_shutdown_flag = 1;
            if (!utp_eof_flag) {
                utp_shutdown(s, SHUT_WR);
            } else {
                utp_close(s);
            }
        }
        else {
            quit_flag = 1;
        }
    }
}

uint64 callback_on_read(utp_callback_arguments *a)
{
    const unsigned char *p;
    ssize_t len, left;

    left = a->len;
    p = a->buf;

    while (left) {
        len = write(STDOUT_FILENO, p, left);
        left -= len;
        p += len;
        debug("Wrote %d bytes, %d left\n", len, left);
    }
    utp_read_drained(a->socket);
    return 0;
}

uint64 callback_on_firewall(utp_callback_arguments *a)
{
    if (! o_listen) {
        debug("Firewalling unexpected inbound connection in non-listen mode\n");
        return 1;
    }

    if (s) {
        debug("Firewalling unexpected second inbound connection\n");
        return 1;
    }

    debug("Firewall allowing inbound connection\n");
    return 0;
}

uint64 callback_on_accept(utp_callback_arguments *a)
{
    assert(!s);
    s = a->socket;
    debug("Accepted inbound socket %p\n", s);
    write_data();
    return 0;
}

uint64 callback_on_error(utp_callback_arguments *a)
{
    fprintf(stderr, "Error: %s\n", utp_error_code_names[a->error_code]);
    utp_close(s);
    s = NULL;
    quit_flag = 1;
    exit_code++;
    return 0;
}

uint64 callback_on_state_change(utp_callback_arguments *a)
{
    debug("state %d: %s\n", a->state, utp_state_names[a->state]);
    utp_socket_stats *stats;

    switch (a->state) {
        case UTP_STATE_CONNECT:
        case UTP_STATE_WRITABLE:
            write_data();
            break;

        case UTP_STATE_EOF:
            debug("Received EOF from socket\n");
            utp_eof_flag = 1;
            if (utp_shutdown_flag) {
                utp_close(a->socket);
            }
            break;

        case UTP_STATE_DESTROYING:
            debug("UTP socket is being destroyed; exiting\n");

            stats = utp_get_stats(a->socket);
            if (stats) {
                debug("Socket Statistics:\n");
                debug("    Bytes sent:          %d\n", stats->nbytes_xmit);
                debug("    Bytes received:      %d\n", stats->nbytes_recv);
                debug("    Packets received:    %d\n", stats->nrecv);
                debug("    Packets sent:        %d\n", stats->nxmit);
                debug("    Duplicate receives:  %d\n", stats->nduprecv);
                debug("    Retransmits:         %d\n", stats->rexmit);
                debug("    Fast Retransmits:    %d\n", stats->fastrexmit);
                debug("    Best guess at MTU:   %d\n", stats->mtu_guess);
            }
            else {
                debug("No socket statistics available\n");
            }

            s = NULL;
            quit_flag = 1;
            break;
    }

    return 0;
}

uint64 callback_sendto(utp_callback_arguments *a)
{
    struct sockaddr_in *sin = (struct sockaddr_in *) a->address;

    debug("sendto: %zd byte packet to %s:%d%s\n", a->len, inet_ntoa(sin->sin_addr), ntohs(sin->sin_port),
          (a->flags & UTP_UDP_DONTFRAG) ? "  (DF bit requested, but not yet implemented)" : "");

    if (o_debug >= 3)
        hexdump(a->buf, a->len);

    sendto(fd, a->buf, a->len, 0, a->address, a->address_len);
    return 0;
}

uint64 callback_log(utp_callback_arguments *a)
{
    fprintf(stderr, "log: %s\n", a->buf);
    return 0;
}

void handler(int number)
{
    debug("caught signal\n");
    if (s)
        utp_close(s);
    quit_flag = 1;
    exit_code++;
}

#ifdef __linux__
void handle_icmp()
{
	while (1) {
		unsigned char vec_buf[4096], ancillary_buf[4096];
		struct iovec iov = { vec_buf, sizeof(vec_buf) };
		struct sockaddr_in remote;
		struct msghdr msg;
		ssize_t len;
		struct cmsghdr *cmsg;
		struct sock_extended_err *e;
		struct sockaddr *icmp_addr;
		struct sockaddr_in *icmp_sin;

		memset(&msg, 0, sizeof(msg));

		msg.msg_name = &remote;
		msg.msg_namelen = sizeof(remote);
		msg.msg_iov = &iov;
		msg.msg_iovlen = 1;
		msg.msg_flags = 0;
		msg.msg_control = ancillary_buf;
		msg.msg_controllen = sizeof(ancillary_buf);

		len = recvmsg(fd, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

		if (len < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			else
				pdie("recvmsg");
		}

		for (cmsg = CMSG_FIRSTHDR(&msg);
			 cmsg;
			 cmsg = CMSG_NXTHDR(&msg, cmsg))
		{
			if (cmsg->cmsg_type != IP_RECVERR) {
				debug("Unhandled errqueue type: %d\n", cmsg->cmsg_type);
				continue;
			}

			if (cmsg->cmsg_level != SOL_IP) {
				debug("Unhandled errqueue level: %d\n", cmsg->cmsg_level);
				continue;
			}

			debug("errqueue: IP_RECVERR, SOL_IP, len %zd\n", cmsg->cmsg_len);

			if (remote.sin_family != AF_INET) {
				debug("Address family is %d, not AF_INET?  Ignoring\n", remote.sin_family);
				continue;
			}

			debug("Remote host: %s:%d\n", inet_ntoa(remote.sin_addr), ntohs(remote.sin_port));

			e = (struct sock_extended_err *) CMSG_DATA(cmsg);

			if (!e) {
				debug("errqueue: sock_extended_err is NULL?\n");
				continue;
			}

			if (e->ee_origin != SO_EE_ORIGIN_ICMP) {
				debug("errqueue: Unexpected origin: %d\n", e->ee_origin);
				continue;
			}

			debug("    ee_errno:  %d\n", e->ee_errno);
			debug("    ee_origin: %d\n", e->ee_origin);
			debug("    ee_type:   %d\n", e->ee_type);
			debug("    ee_code:   %d\n", e->ee_code);
			debug("    ee_info:   %d\n", e->ee_info);	// discovered MTU for EMSGSIZE errors
			debug("    ee_data:   %d\n", e->ee_data);

			// "Node that caused the error"
			// "Node that generated the error"
			icmp_addr = (struct sockaddr *) SO_EE_OFFENDER(e);
			icmp_sin = (struct sockaddr_in *) icmp_addr;

			if (icmp_addr->sa_family != AF_INET) {
				debug("ICMP's address family is %d, not AF_INET?\n", icmp_addr->sa_family);
				continue;
			}

			if (icmp_sin->sin_port != 0) {
				debug("ICMP's 'port' is not 0?\n");
				continue;
			}

			debug("msg_flags: %d", msg.msg_flags);
			if (o_debug) {
				if (msg.msg_flags & MSG_TRUNC)		fprintf(stderr, " MSG_TRUNC");
				if (msg.msg_flags & MSG_CTRUNC)		fprintf(stderr, " MSG_CTRUNC");
				if (msg.msg_flags & MSG_EOR)		fprintf(stderr, " MSG_EOR");
				if (msg.msg_flags & MSG_OOB)		fprintf(stderr, " MSG_OOB");
				if (msg.msg_flags & MSG_ERRQUEUE)	fprintf(stderr, " MSG_ERRQUEUE");
				fprintf(stderr, "\n");
			}

			if (o_debug >= 3)
				hexdump(vec_buf, len);

			if (e->ee_type == 3 && e->ee_code == 4) {
				debug("ICMP type 3, code 4: Fragmentation error, discovered MTU %d\n", e->ee_info);
				utp_process_icmp_fragmentation(ctx, vec_buf, len, (struct sockaddr *)&remote, sizeof(remote), e->ee_info);
			}
			else {
				debug("ICMP type %d, code %d\n", e->ee_type, e->ee_code);
				utp_process_icmp_error(ctx, vec_buf, len, (struct sockaddr *)&remote, sizeof(remote));
			}
		}
	}
}
#endif

void network_loop(void)
{
    unsigned char socket_data[4096];
    struct sockaddr_in src_addr;
    socklen_t addrlen = sizeof(src_addr);
    ssize_t len;
    int ret;

    struct pollfd p[2];

    p[0].fd = STDIN_FILENO;
    p[0].events = (o_buf_size-buf_len && !eof_flag) ? POLLIN : 0;

    p[1].fd = fd;
    p[1].events = POLLIN;

    ret = poll(p, 2, 500);
    if (ret < 0) {
        if (errno == EINTR)
            debug("poll() returned EINTR\n");
        else
            pdie("poll");
    }
    else if (ret == 0) {
        if (o_debug >= 3)
            debug("poll() timeout\n");
    }
    else {
        if ((p[0].revents & POLLIN) == POLLIN) {
            len = read(STDIN_FILENO, buf+buf_len, o_buf_size-buf_len);
            if (len < 0 && errno != EINTR)
                pdie("read stdin");
            if (len == 0) {
                debug("EOF from file\n");
                eof_flag = 1;
                close(STDIN_FILENO);
            }
            else {
                buf_len += len;
                debug("Read %d bytes, buffer now %d bytes long\n", len, buf_len);
            }
            write_data();
        }

#ifdef __linux__
        if ((p[1].revents & POLLERR) == POLLERR)
			handle_icmp();
#endif

        if ((p[1].revents & POLLIN) == POLLIN) {
            while (1) {
                len = recvfrom(fd, socket_data, sizeof(socket_data), MSG_DONTWAIT, (struct sockaddr *)&src_addr, &addrlen);
                if (len < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        utp_issue_deferred_acks(ctx);
                        break;
                    }
                    else
                        pdie("recv");
                }

                debug("Received %zd byte UDP packet from %s:%d\n", len, inet_ntoa(src_addr.sin_addr), ntohs(src_addr.sin_port));
                if (o_debug >= 3)
                    hexdump(socket_data, len);

                if (! utp_process_udp(ctx, socket_data, len, (struct sockaddr *)&src_addr, addrlen))
                    debug("UDP packet not handled by UTP.  Ignoring.\n");
            }
        }
    }

    utp_check_timeouts(ctx);
}

int main(int argc, char *argv[])
{
    o_local_address = "0.0.0.0";

    // setup start
    struct addrinfo hints, *res;
    struct sockaddr_in sin, *sinp;
    int error;
    struct sigaction sigIntHandler;

    sigIntHandler.sa_handler = handler;
    sigemptyset(&sigIntHandler.sa_mask);
    sigIntHandler.sa_flags = 0;

    sigaction(SIGINT, &sigIntHandler, NULL);

    p = buf = malloc(o_buf_size);
    if (!buf)
        pdie("malloc");
    debug("Allocatd %d buffer\n", o_buf_size);

    fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (fd < 0)
        pdie("socket");

#ifdef __linux__
    int on = 1;
	if (setsockopt(fd, SOL_IP, IP_RECVERR, &on, sizeof(on)) != 0)
		pdie("setsockopt");
#endif

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;
    if (o_numeric)
        hints.ai_flags |= AI_NUMERICHOST;

    o_local_address = "127.0.0.1";
    o_local_port = "9079";

    if ((error = getaddrinfo(o_local_address, o_local_port, &hints, &res)))
        die("getaddrinfo: %s\n", gai_strerror(error));

    if (bind(fd, res->ai_addr, res->ai_addrlen) != 0)
        pdie("bind");

    freeaddrinfo(res);

    socklen_t len = sizeof(sin);
    if (getsockname(fd, (struct sockaddr *) &sin, &len) != 0)
        pdie("getsockname");
    debug("Bound to local %s:%d\n", inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

    ctx = utp_init(2);
    assert(ctx);
    debug("UTP context %p\n", ctx);

    utp_set_callback(ctx, UTP_LOG,				&callback_log);
    utp_set_callback(ctx, UTP_SENDTO,			&callback_sendto);
    utp_set_callback(ctx, UTP_ON_ERROR,			&callback_on_error);
    utp_set_callback(ctx, UTP_ON_STATE_CHANGE,	&callback_on_state_change);
    utp_set_callback(ctx, UTP_ON_READ,			&callback_on_read);
    utp_set_callback(ctx, UTP_ON_FIREWALL,		&callback_on_firewall);
    utp_set_callback(ctx, UTP_ON_ACCEPT,		&callback_on_accept);

    if (o_debug >= 2) {
        utp_context_set_option(ctx, UTP_LOG_NORMAL, 1);
        utp_context_set_option(ctx, UTP_LOG_MTU,    1);
        utp_context_set_option(ctx, UTP_LOG_DEBUG,  1);
    }

    s = utp_create_socket(ctx);
    assert(s);
    debug("UTP socket %p\n", s);

    // node we are connecting to
    o_remote_address = "127.0.0.1";
    o_remote_port = "9078";
    if ((error = getaddrinfo(o_remote_address, o_remote_port, &hints, &res)))
        die("getaddrinfo: %s\n", gai_strerror(error));

    sinp = (struct sockaddr_in *)res->ai_addr;
    debug("Connecting to %s:%d\n", inet_ntoa(sinp->sin_addr), ntohs(sinp->sin_port));
    debug("bing2\n");
    int connect_number = utp_connect(s, res->ai_addr, res->ai_addrlen);
    debug("connect_number %zd\n", connect_number);
    freeaddrinfo(res);
    // setup end

    // write code comment out to observe keep alive
    unsigned char * bob = "big breadBBAABBAA\0";
    strcpy((char*) p, (const char*) bob); // compiles (but bad practice)
    strcpy((char*) buf, (const char*) bob); // compiles (but bad practice)
    buf_len = 19;
    // write code end

    while (!quit_flag)
        network_loop();

    debug("Destroying context\n");
    utp_destroy(ctx);
    return exit_code;
}
