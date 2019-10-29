#ifndef _HADOOFUS_CRC32C_H
#define _HADOOFUS_CRC32C_H

#include <stdint.h>

// Update a running CRC32-C with the bytes in the given buffer and return the
// updated CRC32-C. The initial value for crc should be 0. Pre- and
// post-conditioning (bitwise inversion) is handled by this function and should
// not be be performed by the caller. This function chooses the implementation
// based on the system in use: an implementation using the Intel SSE4.2 CRC32
// instructions, an implementation using ARMv8's optional CRC32C instructions,
// or a table-driven software implementation.
uint32_t	crc32c(uint32_t crc, const void *buf, unsigned len);

#if defined(__amd64__) || defined(__i386__)
// Should only be called if the processor supports SSE4.2
uint32_t	sse42_crc32c(uint32_t crc, const void *buf, unsigned len);
#elif defined(__aarch64__)
// Should only be called if the processor supports ARMv8's optional CRC32-C
// instructions.
uint32_t	armv8_crc32c(uint32_t crc, const void *buf, unsigned len);
#endif

uint32_t	sw_crc32c(uint32_t crc, const void *buf, unsigned len);

#endif // _HADOOFUS_CRC32C_H
