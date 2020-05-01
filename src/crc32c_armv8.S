/*
 * Modified from original FreeBSD source:
 *   - Remove FreeBSD-specific code and includes
 *   - Pull in the #defines from FreeBSD's arm64 asm.h
 *   - Add pre- and post-conditioning (bit inversion) to the crc
 *   - Wrap code in preprocessor architecture condition
 *   - Conditionally don't use the .arch directive on clang versions < 4.0
 *   - Update extern function to be within Hadoofus's namespace
 */

/*-
 * Copyright (c) 2017 Michael Tuexen
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 */

#if defined(__aarch64__)

#define	ENTRY(sym)						\
	.text; .globl sym; .align 2; .type sym,#function; sym:
#define	END(sym) .size sym, . - sym

// The .arch directive doesn't exist on clang 3.8, but exists in
// clang 4.0 (unclear if it exists in 3.9, but let's assume not).
// There doesn't seem to be a __has_XXX() macro for assembler directives
#if !defined(__clang__) || __clang_major__ >= 4
.arch armv8-a+crc
#endif

/*
 * uint32_t
 * _hdfs_armv8_crc32c(uint32_t crc, const unsigned char *buf, unsigned int len)
 */

ENTRY(_hdfs_armv8_crc32c)
	mvn	w0, w0 // pre-condition the crc
	cbz	w2, end
	tbz	x1, #0x0, half_word_aligned
	sub	w2, w2, 0x1
	ldr	w10, [x1], #0x1
	crc32cb	w0, w0, w10
half_word_aligned:
	cmp	w2, #0x2
	b.lo	last_byte
	tbz	x1, #0x1, word_aligned
	sub	w2, w2, 0x2
	ldr	w10, [x1], #0x2
	crc32ch	w0, w0, w10
word_aligned:
	cmp	w2, #0x4
	b.lo	last_half_word
	tbz	x1, #0x2, double_word_aligned
	sub	w2, w2, 0x4
	ldr	w10, [x1], #0x4
	crc32cw	w0, w0, w10
double_word_aligned:
	lsr	w9, w2, #0x3
	cbz	w9, last_word
loop:
	ldr	x10, [x1], #0x8
	crc32cx	w0, w0, x10
	subs	w9, w9, #1
	b.ne	loop
last_word:
	tbz	w2, #0x2, last_half_word
	ldr	w10, [x1], #0x4
	crc32cw	w0, w0, w10
last_half_word:
	tbz	w2, #0x1, last_byte
	ldr	w10, [x1], #0x2
	crc32ch	w0, w0, w10
last_byte:
	tbz	w2, #0x0, end 
	ldr	w10, [x1], #0x1
	crc32cb	w0, w0, w10
end:
	mvn	w0, w0 // return a post-conditioned crc
	ret
END(_hdfs_armv8_crc32c)

#endif /* defined(__aarch64__) */
