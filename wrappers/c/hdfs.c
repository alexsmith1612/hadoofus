#ifdef NDEBUG
# undef NDEBUG
#endif

#include <sys/types.h>
#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <hadoofus/highlevel.h>
#include <hadoofus/lowlevel.h>
#include <hadoofus/objects.h>

#include "hdfs.h"

#define ERR(ecode, msg, args...) \
do { \
	errno = ecode; \
	fprintf(stderr, "%s:%d: In %s, error: " msg " (errno = %s)\n", \
	    __FILE__, __LINE__, __func__, ##args, strerror(errno)); \
} while (0)

#define WARN(msg, args...) \
do { \
	fprintf(stderr, "%s:%d: In %s, warning: " msg "\n", \
	    __FILE__, __LINE__, __func__, ##args); \
} while (0)

#define nelem(arr) (sizeof(arr)/sizeof(arr[0]))

#define DEFAULT_BLOCK_SIZE (64*1024*1024)
#define DEFAULT_REPLICATION 3

enum hdfsFile_mode {
	FILE_READ, FILE_WRITE, FILE_APPEND
};

struct hdfsFile_internal {
	tOffset fi_offset;
	struct hdfs_object *fi_lastblock;
	char *fi_path,
	     *fi_client,
	     *fi_wbuf;
	size_t fi_wbuf_used;
	tSize fi_blocksize;
	enum hdfsFile_mode fi_mode;
	short fi_replication;
};

struct hdfsFS_internal {
	struct hdfs_namenode *fs_namenode;
	char *fs_cwd;
};

static int	_flush(struct hdfs_namenode *, struct hdfsFile_internal *, const void *, size_t);
static void	_hadoofus_file_status_to_libhdfs(struct hdfs_object *, hdfsFileInfo *);
static char *	_makeabs(struct hdfsFS_internal *, const char *path);
static char *	_makehome(const char *user);
static void	_urandbytes(char *, size_t);
static char *	_xstrdup(const char *);

static inline int
_imin(int a, int b)
{
	if (a < b)
		return a;
	return b;
}

/**
 * hdfsConnectAsUser - Connect to a hdfs file system as a specific user
 *
 * @param host A string containing either a host name, or an ip address
 *     of the namenode of a hdfs cluster.
 * @param port The port on which the server is listening.
 * @param user the user name (this is hadoop domain user).
 * @return Returns a handle to the filesystem or NULL on error.
 */
hdfsFS
hdfsConnectAsUser(const char* host, tPort port, const char *user)
{
	const char *err = NULL;
	char sport[40];
	bool port_set = false;
	struct hdfs_namenode *nn;
	struct hdfsFS_internal *res;

	if (!host || !strcmp(host, "default")) {
		char *e_port;
		host = getenv("HDFS_DEFAULT_HOST");
		e_port = getenv("HDFS_DEFAULT_PORT");

		if (!host) {
			ERR(EINVAL, "Set HDFS_DEFAULT_HOST if you want NULL"
			    "/\"default\" to work with libhadoofus");
			return NULL;
		}
		if (e_port) {
			strncpy(sport, e_port, nelem(sport));
			sport[nelem(sport)-1] = '\0';
			port_set = true;
		} else {
			port = 8020;
		}
	}

	if (!port_set)
		sprintf(sport, "%d", port);

	nn = hdfs_namenode_new(host, sport, user, &err);
	if (!nn) {
		ERR(ECONNABORTED, "%s", err);
		return NULL;
	}

	res = malloc(sizeof *res);
	assert(res);

	res->fs_namenode = nn;
	res->fs_cwd = _makehome(user);

	return (hdfsFS)res;
}

/**
 * hdfsConnect
 *
 * Left as part of the libhdfs API/ABI, but this will always fail with
 * libhadoofus. Use hdfsConnectAsUser() instead.
 */
hdfsFS
hdfsConnect(const char* host, tPort port)
{
	const char *user = getenv("HDFS_DEFAULT_USER");

	if (!user) {
		ERR(EINVAL, "Set HDFS_DEFAULT_USER if you must use "
		    "hdfsConnect()");
		return NULL;
	}

	return hdfsConnectAsUser(host, port, user);
}

/**
 * hdfsDisconnect - Disconnect from the hdfs file system.
 *
 * @param fs The configured filesystem handle.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsDisconnect(hdfsFS fs)
{
	struct hdfsFS_internal *client = fs;

	hdfs_namenode_delete(client->fs_namenode);
	free(client->fs_cwd);
	free(client);

	return 0;
}

/**
 * hdfsOpenFile - Open a file. File handles are not thread-safe. Use your own
 * synchonrization if you want to share them across threads.
 *
 * @param fs The configured filesystem handle.
 * @param path The full path to the file.
 * @param flags - OR of O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 *     O_WRONLY|O_APPEND. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT))
 *     which return NULL and set errno equal ENOTSUP.
 * @param bufferSize Size of buffer for read/write - pass 0 if you want to use the default values.
 * @param replication Block replication - pass 0 if you want to use the default values.
 * @param blocksize Size of block - pass 0 if you want to use the default values.
 * @return Returns the handle to the open file or NULL on error.
 */
hdfsFile
hdfsOpenFile(hdfsFS fs, const char* path, int flags, int bufferSize,
	short replication, tSize blocksize)
{
	struct hdfs_object *ex = NULL, *lb = NULL;
	struct hdfsFile_internal *res;
	struct hdfsFS_internal *fsclient = fs;
	enum hdfsFile_mode mode;
	char *client = NULL, *path_abs;

	path_abs = _makeabs(fs, path);

	union {
		int64_t num;
		char bytes[8];
	} client_u;

	const int clientlen = 32;

	if (flags & O_RDWR) {
		ERR(ENOTSUP, "Cannot open an hdfs file in O_RDWR mode");
		return NULL;
	}

	if ((flags & O_RDONLY) && (flags & O_WRONLY)) {
		ERR(EINVAL, "O_RDONLY and O_WRONLY are mutually exclusive");
		return NULL;
	}

	if ((flags & O_CREAT) || (flags & O_EXCL))
		WARN("hdfs does not really support O_CREAT and O_EXCL");

	if (flags & O_WRONLY) {
		if (flags & O_APPEND)
			mode = FILE_APPEND;
		else
			mode = FILE_WRITE;
	} else {
		// Assume READ (even without O_RDONLY) to match libhdfs:
		mode = FILE_READ;
	}

	// Defaults
	if (replication == 0)
		replication = DEFAULT_REPLICATION;
	if (blocksize == 0)
		blocksize = DEFAULT_BLOCK_SIZE;

	_urandbytes(client_u.bytes, nelem(client_u.bytes));

	client = malloc(clientlen);
	assert(client);
	snprintf(client, clientlen-1, "DFSClient_%ld", client_u.num);
	client[clientlen-1] = '\0';

	if (mode == FILE_WRITE) {
		hdfs_create(fsclient->fs_namenode, path_abs, 0644, client, true/* overwrite */,
		    true/* create parent */, replication, blocksize, &ex);
		if (ex) {
			ERR(EINVAL, "Error opening %s for writing: %s",
			    path_abs, hdfs_exception_get_message(ex));
			hdfs_object_free(ex);
			free(client);
			return NULL;
		}
	} else if (mode == FILE_APPEND) {
		lb = hdfs_append(fsclient->fs_namenode, path_abs, client, &ex);
		if (ex) {
			ERR(EINVAL, "Error opening %s for appending: %s",
			    path_abs, hdfs_exception_get_message(ex));
			hdfs_object_free(ex);
			free(client);
			return NULL;
		}
	}

	res = malloc(sizeof *res);
	assert(res);

	res->fi_offset = 0;
	res->fi_path = strdup(path_abs);
	assert(res->fi_path);

	res->fi_mode = mode;
	res->fi_blocksize = blocksize;
	res->fi_replication = replication;

	if (lb && lb->ob_type == H_NULL) {
		hdfs_object_free(lb);
		lb = NULL;
	}

	res->fi_lastblock = lb;
	if (mode == FILE_READ) {
		res->fi_wbuf = NULL;
	} else {
		res->fi_wbuf = malloc(blocksize);
		assert(res->fi_wbuf);
	}
	res->fi_wbuf_used = 0;

	res->fi_client = client;

	if (lb)
		hdfs_object_free(lb);
	if (path_abs != path)
		free(path_abs);

	return res;
}

/**
 * hdfsCloseFile - Close an open file.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsCloseFile(hdfsFS fs, hdfsFile file)
{
	struct hdfsFile_internal *f = file;
	struct hdfsFS_internal *client = fs;
	int res = 0;

	if (f->fi_mode == FILE_WRITE || f->fi_mode == FILE_APPEND) {
		struct hdfs_object *ex = NULL;
		bool succ = false;

		if (f->fi_wbuf_used > 0) {
			res = _flush(client->fs_namenode, f, NULL, -1);
			if (res == -1)
				WARN("Flushing '%s' failed: %m", f->fi_path);
		}

		while (!succ) {
			succ = hdfs_complete(client->fs_namenode, f->fi_path, f->fi_client, &ex);
			if (!succ) {
				WARN("Could not complete '%s'", f->fi_path);
				usleep(400*1000);
			}
		}

		free(f->fi_wbuf);
		if (ex) {
			ERR(EIO, "complete(): %s", hdfs_exception_get_message(ex));
			hdfs_object_free(ex);
			res = -1;
			goto out;
		}
	}

out:
	free(f->fi_client);
	free(f->fi_path);
	free(f);
	return res;
}

/**
 * hdfsExists - Checks if a given path exists on the filesystem
 *
 * @param fs The configured filesystem handle.
 * @param path The path to look for
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsExists(hdfsFS fs, const char *path)
{
	struct hdfs_object *s, *ex = NULL;
	struct hdfsFS_internal *client = fs;
	int res = 0;
	char *path_abs = _makeabs(fs, path);

	s = hdfs_getFileInfo(client->fs_namenode, path_abs, &ex);
	if (ex) {
		ERR(EIO, "getFileInfo(): %s", hdfs_exception_get_message(ex));
		hdfs_object_free(ex);
		res = -1;
		goto out;
	}

	if (s->ob_type == H_NULL) {
		res = -1;
		errno = ENOENT;
	}

	hdfs_object_free(s);
out:
	if (path_abs != path)
		free(path_abs);
	return res;
}

/**
 * hdfsSeek - Seek to given offset in file.
 * This works only for files opened in read-only mode.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param desiredPos Offset into the file to seek into.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos)
{
	struct hdfsFile_internal *f = file;

	if (f->fi_mode != FILE_READ) {
		ERR(EINVAL, "can't seek file opened for writing");
		return -1;
	}

	f->fi_offset = desiredPos;
	return 0;
}

/**
 * hdfsTell - Get the current offset in the file, in bytes.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Current offset, -1 on error.
 */
tOffset
hdfsTell(hdfsFS fs, hdfsFile file)
{
	struct hdfsFile_internal *f = file;
	return f->fi_offset;
}

/**
 * hdfsRead - Read data from an open file.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return Returns the number of bytes actually read, possibly less
 *     than than length; -1 on error.
 */
tSize
hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length)
{
	struct hdfsFile_internal *f = file;
	tSize res;

	res = hdfsPread(fs, file, f->fi_offset, buffer, length);
	if (res != -1)
		f->fi_offset += length;

	return res;
}

/**
 * hdfsPread - Positional read of data from an open file.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param position Position from which to read
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return Returns the number of bytes actually read, possibly less than
 *     than length;-1 on error.
 */
tSize
hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length)
{
	tSize res = -1;
	struct hdfsFS_internal *client = fs;
	struct hdfsFile_internal *f = file;
	struct hdfs_object *bls = NULL, *ex = NULL;
	struct hdfs_datanode *dn;
	bool verifycrcs = true;

	if (f->fi_mode != FILE_READ) {
		ERR(EINVAL, "can't read from file opened for writing");
		goto out;
	}

	if (length == 0) {
		res = 0;
		goto out;
	}

	bls = hdfs_getBlockLocations(client->fs_namenode, f->fi_path, position, length, &ex);
	if (ex) {
		ERR(EIO, "getBlockLocations(): %s", hdfs_exception_get_message(ex));
		goto out;
	}
	if (bls->ob_type == H_NULL) {
		ERR(ENOENT, "getBlockLocations(): %s doesn't exist", f->fi_path);
		goto out;
	}

	// We may need to read multiple blocks to satisfy the read
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl = bls->ob_val._located_blocks._blocks[i];
		const char *err = NULL;

		int64_t blbegin = 0,
			blend = bl->ob_val._located_block._len,
			bloff = bl->ob_val._located_block._offset;

		// Skip blocks earlier than we're looking for:
		if (bloff + blend <= position)
			continue;
		// Skip blocks after the range we want:
		if (bloff >= position + length)
			break;

		dn = hdfs_datanode_new(bl, f->fi_client, HDFS_DATANODE_AP_1_0, &err);
		if (!dn) {
			ERR(EIO, "Error connecting to datanode: %s", err);
			goto out;
		}

		// For each block, read the relevant part into the buffer.
		if (bloff < position)
			blbegin = position - bloff;
		if (bloff + blend > position + length)
			blend = position + length - bloff;

		err = hdfs_datanode_read(dn, blbegin/* offset in block */,
		    blend - blbegin/* len */, buffer, verifycrcs);

		// Disable crc verification if the server doesn't support them
		if (err == HDFS_DATANODE_ERR_NO_CRCS) {
			WARN("Server doesn't support CRCs, cannot verify integrity");
			verifycrcs = false;
			err = NULL;

			// Re-connect to datanode to retry read without CRC
			// verification:
			hdfs_datanode_delete(dn);
			dn = hdfs_datanode_new(bl, f->fi_client, HDFS_DATANODE_AP_1_0, &err);
			if (!dn) {
				ERR(EIO, "Error connecting to datanode: %s", err);
				goto out;
			}

			err = hdfs_datanode_read(dn, blbegin/* offset in block */,
			    blend - blbegin/* len */, buffer, verifycrcs);
		}

		hdfs_datanode_delete(dn);

		if (err) {
			ERR(EIO, "Error during read: %s", err);
			goto out;
		}

		buffer = (char*)buffer + (blend - blbegin);
	}

	res = length;

out:
	if (bls)
		hdfs_object_free(bls);
	if (ex)
		hdfs_object_free(ex);
	return res;
}

/**
 * hdfsWrite - Write data into an open file.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The data.
 * @param length The no. of bytes to write.
 * @return Returns the number of bytes written, -1 on error.
 */
tSize
hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length)
{
	tSize res = length,
	      avail_in_block,
	      towrite;
	struct hdfsFile_internal *f = file;
	struct hdfsFS_internal *client = fs;

	if (f->fi_mode != FILE_WRITE && f->fi_mode != FILE_APPEND) {
		ERR(EINVAL, "Cannot write to a file opened for reading");
		res = -1;
		goto out;
	}

	avail_in_block = f->fi_blocksize;
	if (f->fi_lastblock)
		avail_in_block -= f->fi_lastblock->ob_val._located_block._len;

	if (avail_in_block <= 0) {
		// Last block is full, ignore it.
		hdfs_object_free(f->fi_lastblock);
		f->fi_lastblock = NULL;
		avail_in_block = f->fi_blocksize;
	}

	while (length > 0) {
		int rc = 0;

		// Subset of the caller's buf to copy or xmit:
		towrite = _imin(length, avail_in_block - f->fi_wbuf_used);

		// We can skip the buffer if the client is passing us full
		// blocks, and nothing is already buffered.
		if (towrite == avail_in_block) {
			const void *buf_orig = buffer;

			buffer += towrite;
			length -= towrite;

			rc = _flush(client->fs_namenode, f, buf_orig, towrite);
		} else {
			memcpy(f->fi_wbuf + f->fi_wbuf_used, buffer, towrite);
			f->fi_wbuf_used += towrite;

			buffer += towrite;
			length -= towrite;

			if (f->fi_wbuf_used == avail_in_block)
				rc = _flush(client->fs_namenode, f, NULL, -1);
		}

		if (rc == -1) {
			res = -1;
			goto out;
		}
	}

out:
	return res;
}

/**
 * hdfsFlush - Flush the data.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsFlush(hdfsFS fs, hdfsFile file)
{
	struct hdfsFile_internal *f = file;
	struct hdfsFS_internal *client = fs;
	int res = 0;

	if (f->fi_mode != FILE_WRITE && f->fi_mode != FILE_APPEND) {
		ERR(EINVAL, "Cannot flush a file opened for reading");
		res = -1;
		goto out;
	}

	if (f->fi_wbuf_used > 0)
		res = _flush(client->fs_namenode, f, NULL, -1);

out:
	return res;
}

/**
 * hdfsAvailable - Number of bytes that can be read from this
 * input stream without blocking.
 *
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns available bytes; -1 on error.
 */
int
hdfsAvailable(hdfsFS fs, hdfsFile file)
{
	return 0;
}

/**
 * hdfsCopy - Copy file from one filesystem to another.
 *
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file.
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
	char *block = NULL, *src_abs, *dst_abs;
	hdfsFileInfo *srcinfo = NULL;
	int res = -1;
	hdfsFile a = NULL, b = NULL;
	tOffset twritten = 0;

	src_abs = _makeabs(srcFS, src);
	dst_abs = _makeabs(dstFS, dst);

	if (hdfsExists(srcFS, src_abs) == -1) {
		ERR(ENOENT, "'%s' doesn't exist on srcFS", src_abs);
		goto out;
	}

	srcinfo = hdfsGetPathInfo(srcFS, src_abs);
	if (!srcinfo) {
		ERR(errno, "hdfsGetPathInfo failed");
		goto out;
	}

	if (srcinfo->mKind == kObjectKindDirectory) {
		ERR(ENOTSUP, "hdfsCopy can't do directories right now");
		goto out;
	}

	a = hdfsOpenFile(srcFS, src_abs, O_RDONLY, 0, 0, 0);
	if (!a) {
		ERR(errno, "hdfsOpenFile failed");
		goto out;
	}

	b = hdfsOpenFile(dstFS, dst_abs, O_WRONLY, 0, DEFAULT_REPLICATION,
	    DEFAULT_BLOCK_SIZE);
	if (!b) {
		ERR(errno, "hdfsOpenFile failed");
		goto out;
	}

	block = malloc(DEFAULT_BLOCK_SIZE);
	assert(block);

	while (twritten < srcinfo->mSize) {
		tSize toread, read, written;

		toread = _imin(DEFAULT_BLOCK_SIZE, srcinfo->mSize - twritten);

		read = hdfsRead(srcFS, a, block, toread);
		if (read == -1) {
			ERR(errno, "hdfsRead failed");
			goto out;
		}

		written = hdfsWrite(dstFS, b, block, read);
		if (written == -1) {
			ERR(errno, "hdfsWrite failed");
			goto out;
		}
		assert(written == read);

		twritten += written;
	}

	res = 0;

out:
	if (a)
		hdfsCloseFile(srcFS, a);
	if (b)
		hdfsCloseFile(dstFS, b);
	if (src_abs != src)
		free(src_abs);
	if (dst_abs != dst)
		free(dst_abs);
	if (block)
		free(block);
	if (srcinfo)
		hdfsFreeFileInfo(srcinfo, 1);
	return res;
}

/**
 * hdfsMove - Move file from one filesystem to another.
 *
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file.
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsMove(hdfsFS srcFS_, const char* src, hdfsFS dstFS, const char* dst)
{
	int res = -1;
	bool b;
	struct hdfs_object *ex = NULL;
	struct hdfsFS_internal *srcFS = srcFS_;
	char *src_abs, *dst_abs;

	src_abs = _makeabs(srcFS, src);
	dst_abs = _makeabs(dstFS, dst);

	// Yeah this comparison isn't perfect. We don't have anything better.
	if (srcFS_ == dstFS) {
		b = hdfs_rename(srcFS->fs_namenode, src_abs, dst_abs, &ex);
		if (ex) {
			ERR(EIO, "rename failed: %s", hdfs_exception_get_message(ex));
			goto out;
		}
		if (!b)
			WARN("rename of '%s' returned false", src_abs);
	} else {

		res = hdfsCopy(srcFS_, src_abs, dstFS, dst_abs);
		if (res == -1) {
			ERR(errno, "hdfsCopy failed");
			goto out;
		}

		b = hdfs_delete(srcFS->fs_namenode, src_abs, false/*recurse*/, &ex);
		if (ex) {
			ERR(EIO, "delete failed: %s", hdfs_exception_get_message(ex));
			goto out;
		}
		if (!b)
			WARN("delete of '%s' returned false", src_abs);
	}

	res = 0;
out:
	if (src_abs != src)
		free(src_abs);
	if (dst_abs != dst)
		free(dst_abs);
	if (ex)
		hdfs_object_free(ex);
	return res;
}

/**
 * hdfsDelete - Delete file.
 *
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsDelete(hdfsFS fs, const char* path)
{
	struct hdfs_object *ex = NULL;
	struct hdfsFS_internal *client = fs;
	char *path_abs = _makeabs(fs, path);
	int res = 0;

	/*bool b = */hdfs_delete(client->fs_namenode, path_abs, true/*recurse*/, &ex);
	if (ex) {
		ERR(EIO, "delete(): %s", hdfs_exception_get_message(ex));
		hdfs_object_free(ex);
		res = -1;
	}

	if (path_abs != path)
		free(path_abs);

	return res;
}

/**
 * hdfsRename - Rename file.
 *
 * @param fs The configured filesystem handle.
 * @param oldPath The path of the source file.
 * @param newPath The path of the destination file.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath)
{
	struct hdfs_object *ex = NULL;
	bool b;
	struct hdfsFS_internal *client = fs;
	char *oldPath_abs, *newPath_abs;
	int res = 0;

	oldPath_abs = _makeabs(fs, oldPath);
	newPath_abs = _makeabs(fs, newPath);

	b = hdfs_rename(client->fs_namenode, oldPath_abs, newPath_abs, &ex);
	if (ex) {
		ERR(EIO, "rename(): %s", hdfs_exception_get_message(ex));
		hdfs_object_free(ex);
		res = -1;
		goto out;
	}
	
	if (!b) {
		ERR(EINVAL, "rename() failed (on '%s' -> '%s')", oldPath_abs, newPath_abs);
		res = -1;
		goto out;
	}

out:
	if (oldPath_abs != oldPath)
		free(oldPath_abs);
	if (newPath_abs != newPath)
		free(newPath_abs);
	return res;
}

/**
 * hdfsGetWorkingDirectory - Get the current working directory for
 * the given filesystem.
 *
 * @param fs The configured filesystem handle.
 * @param buffer The user-buffer to copy path of cwd into.
 * @param bufferSize The length of user-buffer.
 * @return Returns buffer, NULL on error.
 */
char*
hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize)
{
	struct hdfsFS_internal *client = fs;

	if (bufferSize >= strlen(client->fs_cwd) + 1) {
		strcpy(buffer, client->fs_cwd);
		return buffer;
	}

	return NULL;
}

/**
 * hdfsSetWorkingDirectory - Set the working directory. All relative
 * paths will be resolved relative to it.
 * @param fs The configured filesystem handle.
 * @param path The path of the new 'cwd'.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsSetWorkingDirectory(hdfsFS fs, const char* path)
{
	char *path_abs = _makeabs(fs, path);
	struct hdfsFS_internal *client = fs;
	int res;

	res = hdfsExists(fs, path_abs);
	if (res != -1) {
		free(client->fs_cwd);
		client->fs_cwd = strdup(path_abs);
		assert(client->fs_cwd);
	}

	if (path_abs != path)
		free(path_abs);
	return res;
}

/**
 * hdfsCreateDirectory - Make the given file and all non-existent
 * parents into directories.
 *
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsCreateDirectory(hdfsFS fs, const char* path)
{
	int res = 0;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL;
	char *path_abs = _makeabs(fs, path);

	bool b = hdfs_mkdirs(client->fs_namenode, path_abs, 0755, &ex);
	if (ex) {
		ERR(EIO, "mkdirs(): %s", hdfs_exception_get_message(ex));
		hdfs_object_free(ex);
		res = -1;
		goto out;
	}
	if (!b) {
		ERR(EINVAL, "CreateDirectory() failed on '%s'", path_abs);
		res = -1;
		goto out;
	}

out:
	if (path_abs != path)
		free(path_abs);
	return res;
}

/**
 * hdfsSetReplication - Set the replication of the specified
 * file to the supplied value
 *
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns 0 on success, -1 on error.
 */
int
hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication)
{
	int res = 0;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL;
	char *path_abs = _makeabs(fs, path);
	bool b;

	b = hdfs_setReplication(client->fs_namenode, path_abs, replication, &ex);
	if (ex) {
		ERR(EIO, "setReplication(): %s", hdfs_exception_get_message(ex));
		hdfs_object_free(ex);
		res = -1;
		goto out;
	}
	if (!b) {
		ERR(ENOENT, "setReplication(): No such file, or %s is a directory",
		    path_abs);
		res = -1;
		goto out;
	}

out:
	if (path_abs != path)
		free(path_abs);
	return res;
}

/**
 * hdfsListDirectory - Get list of files/directories for a given
 * directory-path. hdfsFreeFileInfo should be called to deallocate memory.
 *
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @param numEntries Set to the number of files/directories in path.
 * @return Returns a dynamically-allocated array of hdfsFileInfo
 *     objects; NULL on error.
 */
hdfsFileInfo*
hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
	hdfsFileInfo *res = NULL;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL, *dl = NULL;
	char *path_abs = _makeabs(fs, path);
	int nfiles;

	dl = hdfs_getListing(client->fs_namenode, path_abs, NULL, &ex);
	if (ex) {
		ERR(EIO, "getListing(): %s", hdfs_exception_get_message(ex));
		hdfs_object_free(ex);
		goto out;
	}
	if (dl->ob_type == H_NULL) {
		ERR(ENOENT, "getListing(): %s doesn't exist", path_abs);
		goto out;
	}
	
	nfiles = dl->ob_val._directory_listing._num_files;

	res = malloc(nfiles * sizeof *res);
	assert(res);

	for (int i = 0; i < nfiles; i++) {
		struct hdfs_object *fstatus =
		    dl->ob_val._directory_listing._files[i];

		_hadoofus_file_status_to_libhdfs(fstatus, &res[i]);
	}

	*numEntries = nfiles;

out:
	if (path_abs != path)
		free(path_abs);
	if (dl)
		hdfs_object_free(dl);
	return res;
}

/**
 * hdfsGetPathInfo - Get information about a path as a (dynamically
 * allocated) single hdfsFileInfo struct. hdfsFreeFileInfo should be
 * called when the pointer is no longer needed.
 *
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns a dynamically-allocated hdfsFileInfo object;
 *     NULL on error.
 */
hdfsFileInfo*
hdfsGetPathInfo(hdfsFS fs, const char* path)
{
	hdfsFileInfo *res = NULL;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL, *fstatus = NULL;
	char *path_abs = _makeabs(fs, path);

	fstatus = hdfs_getFileInfo(client->fs_namenode, path_abs, &ex);
	if (ex) {
		ERR(EIO, "getFileInfo(): %s", hdfs_exception_get_message(ex));
		hdfs_object_free(ex);
		goto out;
	}
	if (fstatus->ob_type == H_NULL) {
		ERR(ENOENT, "getFileInfo(): %s doesn't exist", path_abs);
		goto out;
	}

	res = malloc(sizeof *res);
	assert(res);

	_hadoofus_file_status_to_libhdfs(fstatus, res);

out:
	if (fstatus)
		hdfs_object_free(fstatus);
	if (path_abs != path)
		free(path_abs);
	return res;
}

/**
 * hdfsFreeFileInfo - Free up the hdfsFileInfo array (including fields)
 *
 * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
 *     objects.
 * @param numEntries The size of the array.
 */
void
hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
	for (int i = 0; i < numEntries; i++) {
		free(hdfsFileInfo[i].mOwner);
		free(hdfsFileInfo[i].mName);
		free(hdfsFileInfo[i].mGroup);
	}

	free(hdfsFileInfo);
}

/**
 * hdfsGetHosts - Get hostnames where a particular block (determined by
 * pos & blocksize) of a file is stored. The last element in the array
 * is NULL. Due to replication, a single block could be present on
 * multiple hosts.
 *
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @param start The start of the block.
 * @param length The length of the block.
 * @return Returns a dynamically-allocated 2-d array of blocks-hosts;
 *     NULL on error.
 */
char***
hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length)
{
	char ***res = NULL;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL, *bls = NULL;
	char *path_abs = _makeabs(fs, path);
	int nblocks, nreplicas;

	bls = hdfs_getBlockLocations(client->fs_namenode, path_abs, start, length, &ex);
	if (ex) {
		ERR(EIO, "getBlockLocations(): %s", hdfs_exception_get_message(ex));
		goto out;
	}
	if (bls->ob_type == H_NULL) {
		ERR(ENOENT, "getBlockLocations(): %s doesn't exist", path_abs);
		goto out;
	}
	
	nblocks = bls->ob_val._located_blocks._num_blocks;
	res = malloc((nblocks + 1) * sizeof *res);
	assert(res);

	for (int i = 0; i < nblocks; i++) {
		char **bres;
		struct hdfs_object *bl = bls->ob_val._located_blocks._blocks[i];
		
		nreplicas = bl->ob_val._located_block._num_locs;

		bres = malloc((nreplicas + 1) * sizeof *bres);
		assert(bres);

		for (int j = 0; j < nreplicas; j++) {
			struct hdfs_object *di = bl->ob_val._located_block._locs[j];

			bres[j] = _xstrdup(di->ob_val._datanode_info._hostname);
		}

		bres[nreplicas] = NULL;
		res[i] = bres;
	}

	res[nblocks] = NULL;

out:
	if (bls)
		hdfs_object_free(bls);
	if (ex)
		hdfs_object_free(ex);
	if (path_abs != path)
		free(path_abs);
	return res;
}

/**
 * hdfsFreeHosts - Free up the structure returned by hdfsGetHosts
 *
 * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
 *     objects.
 * @param numEntries The size of the array.
 */
void
hdfsFreeHosts(char ***blockHosts)
{
	char ***blocks = blockHosts;

	for (; *blocks; blocks++) {
		char **hosts = *blocks;
		for (; *hosts; hosts++)
			free(*hosts);
		free(*blocks);
	}
	free(blockHosts);
}

/**
 * hdfsGetDefaultBlockSize - Get the optimum blocksize.
 *
 * @param fs The configured filesystem handle.
 * @return Returns the blocksize; -1 on error.
 */
tOffset
hdfsGetDefaultBlockSize(hdfsFS fs)
{
	tOffset res;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL;

	res = hdfs_getPreferredBlockSize(client->fs_namenode, "/", &ex);
	if (ex) {
		ERR(EIO, "getPreferredBlockSize(): %s", hdfs_exception_get_message(ex));
		goto out;
	}

out:
	if (ex)
		hdfs_object_free(ex);
	return res;
}

// TODO BELOW HERE XXX XXX XXX XXX

/**
 * hdfsGetCapacity - Return the raw capacity of the filesystem.
 *
 * @param fs The configured filesystem handle.
 * @return Returns the raw-capacity; -1 on error.
 */
tOffset
hdfsGetCapacity(hdfsFS fs)
{
	tOffset res = -1;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL, *stats_arr = NULL;

	stats_arr = hdfs_getStats(client->fs_namenode, &ex);
	if (ex) {
		ERR(EIO, "getStats(): %s", hdfs_exception_get_message(ex));
		goto out;
	}
	if (stats_arr->ob_type == H_NULL) {
		ERR(EIO, "getStats(): got bogus null array");
		goto out;
	}
	if (stats_arr->ob_val._array_long._len < 1) {
		ERR(EIO, "getStats(): got short stats array");
		goto out;
	}

	res = stats_arr->ob_val._array_long._vals[0];

out:
	if (stats_arr)
		hdfs_object_free(stats_arr);
	if (ex)
		hdfs_object_free(ex);
	return res;
}

/**
 * hdfsGetUsed - Return the total raw size of all files in the filesystem.
 *
 * @param fs The configured filesystem handle.
 * @return Returns the total-size; -1 on error.
 */
tOffset
hdfsGetUsed(hdfsFS fs)
{
	tOffset res = -1;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL, *stats_arr = NULL;

	stats_arr = hdfs_getStats(client->fs_namenode, &ex);
	if (ex) {
		ERR(EIO, "getStats(): %s", hdfs_exception_get_message(ex));
		goto out;
	}
	if (stats_arr->ob_type == H_NULL) {
		ERR(EIO, "getStats(): got bogus null array");
		goto out;
	}
	if (stats_arr->ob_val._array_long._len < 2) {
		ERR(EIO, "getStats(): got short stats array");
		goto out;
	}

	res = stats_arr->ob_val._array_long._vals[1];

out:
	if (stats_arr)
		hdfs_object_free(stats_arr);
	if (ex)
		hdfs_object_free(ex);
	return res;
}

/**
 * hdfsChown
 *
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param owner this is a string in Hadoop land. Set to null or "" if only setting group
 * @param group  this is a string in Hadoop land. Set to null or "" if only setting user
 * @return 0 on success else -1
 */
int
hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group)
{
	int res = 0;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL;
	char *path_abs = _makeabs(fs, path);

	hdfs_setOwner(client->fs_namenode, path_abs, owner, group, &ex);
	if (ex) {
		ERR(EIO, "setOwner(): %s", hdfs_exception_get_message(ex));
		res = -1;
		goto out;
	}

out:
	if (ex)
		hdfs_object_free(ex);
	if (path_abs != path)
		free(path_abs);
	return res;
}

/**
 * hdfsChmod
 *
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mode the bitmask to set it to
 * @return 0 on success else -1
 */
int
hdfsChmod(hdfsFS fs, const char* path, short mode)
{
	int res = 0;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL;
	char *path_abs = _makeabs(fs, path);

	hdfs_setPermission(client->fs_namenode, path_abs, mode, &ex);
	if (ex) {
		ERR(EIO, "setPermission(): %s", hdfs_exception_get_message(ex));
		res = -1;
		goto out;
	}

out:
	if (ex)
		hdfs_object_free(ex);
	if (path_abs != path)
		free(path_abs);
	return res;
}

/**
 * hdfsUtime
 *
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mtime new modification time or 0 for only set access time in seconds
 * @param atime new access time or 0 for only set modification time in seconds
 * @return 0 on success else -1
 */
int
hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime)
{
	int res = 0;
	struct hdfsFS_internal *client = fs;
	struct hdfs_object *ex = NULL;
	char *path_abs = _makeabs(fs, path);

	hdfs_setTimes(client->fs_namenode, path_abs,
	    (int64_t)mtime*1000, (int64_t)atime*1000, &ex);
	if (ex) {
		ERR(EIO, "setTimes(): %s", hdfs_exception_get_message(ex));
		res = -1;
		goto out;
	}

out:
	if (ex)
		hdfs_object_free(ex);
	if (path_abs != path)
		free(path_abs);
	return res;
}

// Flushes internal buffer to disk. If passed buf is non-NULL, flushes that
// instead. Returns 0 on succ, -1 on err.
static int
_flush(struct hdfs_namenode *fs, struct hdfsFile_internal *f, const void *buf, size_t len)
{
	struct hdfs_object *lb = NULL, *ex = NULL, *excl = NULL, *block = NULL;
	struct hdfs_datanode *dn = NULL;
	int res = 0, tries = 3;
	const char *err = NULL, *msg_fmt;

	const char *wbuf = f->fi_wbuf;
	size_t wbuf_len = f->fi_wbuf_used;

	// Skip the extra copy if the user is feeding us full-sized blocks
	if (buf) {
		wbuf = buf;
		wbuf_len = len;
	}

	if (!wbuf_len)
		goto out;

	// For appends, we need to finish the last block
	if (f->fi_lastblock) {
		lb = f->fi_lastblock;
		f->fi_lastblock = NULL;
	}

	for (; tries > 0; tries--) {
		if (!lb) {
			lb = hdfs_addBlock(fs, f->fi_path, f->fi_client, excl, &ex);
			if (ex) {
				ERR(EIO, "addBlock failed: %s", hdfs_exception_get_message(ex));
				res = -1;
				goto out;
			}
			if (lb->ob_type == H_NULL) {
				ERR(EIO, "addBlock returned bogus null");
				res = -1;
				goto out;
			}
		}

		msg_fmt = "connect to datanode failed";
		dn = hdfs_datanode_new(lb, f->fi_client, HDFS_DATANODE_AP_1_0, &err);
		if (!dn)
			goto dn_failed;

		msg_fmt = "write failed";
		err = hdfs_datanode_write(dn, wbuf, wbuf_len, true/*crcs*/);
		if (!err)
			break;

dn_failed:
		// On failure, either warn and try again, or give up
		if (tries == 1) {
			ERR(ECONNREFUSED, "%s: %s%s", msg_fmt, err, "");
			res = -1;
			goto out;
		}

		WARN("%s: %s%s", msg_fmt, err, ", retrying");

		block = hdfs_block_from_located_block(lb);
		hdfs_abandonBlock(fs, block, f->fi_path, f->fi_client, &ex);
		if (ex) {
			ERR(EIO, "abandonBlock failed: %s", hdfs_exception_get_message(ex));
			res = -1;
			goto out;
		}

		hdfs_object_free(block);
		block = NULL;

		// Add physical location of bad block's datanode to excluded
		// list
		if (lb->ob_val._located_block._num_locs > 0) {
			if (!excl)
				excl = hdfs_array_datanode_info_new();

			hdfs_array_datanode_info_append_datanode_info(
			    excl,
			    hdfs_datanode_info_copy(
				lb->ob_val._located_block._locs[0]
				)
			    );
		}

		hdfs_object_free(lb);
		lb = NULL;
	}

	// If we weren't given a user block directly, the internal buf has been
	// flushed:
	if (!buf)
		f->fi_wbuf_used = 0;

out:
	if (ex)
		hdfs_object_free(ex);
	if (lb)
		hdfs_object_free(lb);
	if (block)
		hdfs_object_free(block);
	if (excl)
		hdfs_object_free(excl);
	if (dn)
		hdfs_datanode_delete(dn);
	return res;
}

static void
_hadoofus_file_status_to_libhdfs(struct hdfs_object *fs, hdfsFileInfo *fi_out)
{
	struct hdfs_file_status *f = &fs->ob_val._file_status;

	fi_out->mKind = f->_directory? kObjectKindDirectory : kObjectKindFile;
	fi_out->mName = _xstrdup(f->_file);
	fi_out->mLastMod = f->_mtime / 1000; /* ms -> s */
	fi_out->mSize = f->_size;
	fi_out->mReplication = f->_replication;
	fi_out->mBlockSize = f->_block_size;
	fi_out->mOwner = _xstrdup(f->_owner);
	fi_out->mGroup = _xstrdup(f->_group);
	fi_out->mPermissions = f->_permissions;
	fi_out->mLastAccess = f->_atime / 1000; /* ms -> s */
}

static char *
_makeabs(struct hdfsFS_internal *client, const char *path)
{
	char *alloc_path;

	if (*path == '/')
		return (char*)path;

	// Skip leading "./"
	if (strlen(path) >= 2 && !memcmp(path, "./", 2))
		path += 2;

	alloc_path = malloc(strlen(client->fs_cwd) + 1 + strlen(path) + 1);
	assert(alloc_path);

	memcpy(alloc_path, client->fs_cwd, strlen(client->fs_cwd));
	alloc_path[strlen(client->fs_cwd)] = '/';
	strcpy(alloc_path + strlen(client->fs_cwd) + 1, path);

	return alloc_path;
}

static char *
_makehome(const char *user)
{
	const char *prefix = "/user/";
	char *res = malloc(strlen(prefix) + strlen(user) + 1);

	assert(res);
	memcpy(res, prefix, strlen(prefix));
	strcpy(res + strlen(prefix), user);
	return res;
}

static void
_urandbytes(char *b, size_t n)
{
	int fd, r;

	fd = open("/dev/urandom", O_RDONLY);
	assert(fd != -1);

	r = read(fd, b, n);
	assert(r == n);

	close(fd);
}

static char *
_xstrdup(const char *s)
{
	char *res;

	res = strdup(s);
	assert(res);

	return res;
}
