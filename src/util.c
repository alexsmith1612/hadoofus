#include <sys/types.h>
#include <sys/socket.h>

#include <netdb.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>

#include <execinfo.h>

#include <hadoofus/lowlevel.h>

#include "util.h"

uint64_t
_now_ms(void)
{
	struct timespec ts;
	int rc;

	rc = clock_gettime(CLOCK_REALTIME, &ts);
	ASSERT(rc == 0);

	return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / (1000*1000);
}

void
assert_fail(const char *fmt, ...)
{
	void *stack[16];
	size_t nframes;
	va_list ap;

	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);

	fprintf(stderr, "Stack:\n--------------------------------------\n");
	fflush(stderr);

	nframes = backtrace(stack, nelem(stack));
	backtrace_symbols_fd(stack, nframes, fileno(stderr));

	/* Dump core and exit with signal status. */
	abort();
	/* If SIGABRT is masked, go ahead and exit. */
	exit(EX_SOFTWARE);
}

EXPORT_SYM const char *
hdfs_error_str_kind(struct hdfs_error error)
{
	switch (error.her_kind) {
	case he_errno:
		return "errno";
	case he_gaierr:
		return "getaddrinfo";
	case he_saslerr:
		return "sasl";
	case he_hdfserr:
		return "hadoofus";
	}
	ASSERT(false);	// unreachable
}

// TODO standardize the capitalization in these error messages
static const char *hdfs_strerror_table[] = {
	[HDFS_ERR_AGAIN] = "Operation in progress: continue with API when resources are available", // TODO reconsider this message
	[HDFS_ERR_END_OF_STREAM] = "EOS; socket was closed",
	[HDFS_ERR_END_OF_FILE] = "unexpected EOF from file",

	[HDFS_ERR_NAMENODE_UNCONNECTED] = "Invalid use of hdfs_namenode_invoke: must connect Namenode first",
	[HDFS_ERR_NAMENODE_UNAUTHENTICATED] = "Invalid use of hdfs_namenode_invoke: must authenticate first",

	[HDFS_ERR_DATANODE_NO_CRCS] = "Server doesn't send CRCs, can't verify",
	[HDFS_ERR_ZERO_DATANODES] = "LocatedBlock has zero datanodes",
	[HDFS_ERR_DATANODE_UNSUPPORTED_CHECKSUM] = "Server sent an unsupported checksum type",

	[HDFS_ERR_DN_ERROR] = "Datanode error, aborting write",
	[HDFS_ERR_DN_ERROR_CHECKSUM] = "Datanode checksum error, aborting write",
	[HDFS_ERR_DN_ERROR_INVALID] = "Datanode error 'invalid', aborting write",
	[HDFS_ERR_DN_ERROR_EXISTS] = "Datanode error 'exists', aborting write",
	[HDFS_ERR_DN_ERROR_ACCESS_TOKEN] = "Datanode access token error, aborting write",
	[HDFS_ERR_UNRECOGNIZED_DN_ERROR] = "Unrecognized datanode status value",
	[HDFS_ERR_INVALID_DN_ERROR] = "Invalid datanode status value",

	[HDFS_ERR_INVALID_VLINT] = "bad protocol: invalid vlint",
	[HDFS_ERR_INVALID_BLOCKOPRESPONSEPROTO] = "bad protocol: could not decode BlockOpResponseProto",
	[HDFS_ERR_INVALID_PACKETHEADERPROTO] = "bad protocol: could not decode PacketHeaderProto",
	[HDFS_ERR_INVALID_PIPELINEACKPROTO] = "bad protocol: could not decode PipelineAckProto",
	[HDFS_ERR_V1_DATANODE_PROTOCOL] = "invalid Datanode protocol data",
	[HDFS_ERR_NAMENODE_BAD_MSGNO] = "unexpected msgno received from Namenode",
	[HDFS_ERR_NAMENODE_PROTOCOL] = "could not parse response from Namenode",

	[HDFS_ERR_INVALID_DN_OPRESP_MSG] = "successful datanode operation had non-empty error message",
	[HDFS_ERR_DATANODE_PACKET_SIZE] = "got bogus packet size",
	[HDFS_ERR_DATANODE_CRC_LEN] = "got bogus packet crc data",
	[HDFS_ERR_DATANODE_UNEXPECTED_CRC_LEN] = "didn't expect crc data but got some anyway",
	[HDFS_ERR_DATANODE_UNEXPECTED_READ_OFFSET] = "Server started read before requested offset",
	[HDFS_ERR_DATANODE_BAD_CHECKSUM] = "Got bad checksum",
	[HDFS_ERR_DATANODE_BAD_SEQNO] = "Got invalid sequence number in ACK",
	[HDFS_ERR_DATANODE_BAD_ACK_COUNT] = "Got unexpected number of ACKs from pipeline",
	[HDFS_ERR_DATANODE_BAD_LASTPACKET] = "Got 'last packet' flag before expected end of block",
	[HDFS_ERR_KERBEROS_DOWNGRADE] = "Attempt was made to drop kerberos, but the client required it",
	[HDFS_ERR_KERBEROS_NEGOTIATION] = "Unhandled error negotiating kerberos with server",
};

EXPORT_SYM const char *
hdfs_error_str(struct hdfs_error error)
{
	int ecode;

	ecode = error.her_num;

	switch (error.her_kind) {
	case he_errno:
		return strerror(ecode);
	case he_gaierr:
		return gai_strerror(ecode);
	case he_saslerr:
		return sasl_errstring(ecode, NULL, NULL);
	case he_hdfserr:
		ASSERT(ecode >= _HDFS_ERR_MINIMUM && ecode <= _HDFS_ERR_MAXIMUM);
		ASSERT(hdfs_strerror_table[ecode] != NULL);
		return hdfs_strerror_table[ecode];
	}
	ASSERT(false);	// unreachable
}

char *
_proto_str(ProtobufCBinaryData blob)
{
	char *res;

	res = malloc(blob.len + 1);
	ASSERT(res);

	memcpy(res, blob.data, blob.len);
	res[blob.len] = '\0';

	/* No embedded NULs */
	ASSERT(strlen(res) == blob.len);
	return res;
}
