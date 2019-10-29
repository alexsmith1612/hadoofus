#include <stdbool.h>
#include <stdint.h>

#include "crc32c.h"

#ifdef __has_attribute
# if !defined(NO_IFUNC) && __has_attribute(ifunc) // must be in separate condition from ifdef __has_attribute
#  define HAS_IFUNC
# endif
#endif

#if defined(__amd64__) || defined(__i386__)
// Adapted from Mark Adler's crc32c.c version 1.3. See crc32c_sw.c for license.

// Check for SSE 4.2.  SSE 4.2 was first supported in Nehalem processors
// introduced in November, 2008.  This does not check for the existence of the
// cpuid instruction itself, which was introduced on the 486SL in 1992, so this
// will fail on earlier x86 processors.  cpuid works on all Pentium and later
// processors.
static inline bool
has_sse42(void)
{
	uint32_t eax, ecx;

	eax = 1;
	__asm__("cpuid" : "=c"(ecx) : "a"(eax) : "%ebx", "%edx");
	return (ecx >> 20) & 1;
}
#elif defined(__aarch64__)
// Adapted from FreeBSD's armreg.h and gsb_crc32.c

#define	ID_AA64ISAR0_CRC32_SHIFT	16
#define	ID_AA64ISAR0_CRC32_MASK		(0xfUL << ID_AA64ISAR0_CRC32_SHIFT)
#define	ID_AA64ISAR0_CRC32(x)		((x) & ID_AA64ISAR0_CRC32_MASK)
#define	 ID_AA64ISAR0_CRC32_NONE	(0x0UL << ID_AA64ISAR0_CRC32_SHIFT)
#define	 ID_AA64ISAR0_CRC32_BASE	(0x1UL << ID_AA64ISAR0_CRC32_SHIFT)

// We only test for CRC32 support on the CPU with index 0 assuming that this
// applies to all CPUs.
static inline bool
has_armv8_crc32c(void)
{
	uint64_t reg;

	__asm __volatile("mrs	%0, id_aa64isar0_el1" : "=&r" (reg));
	return ID_AA64ISAR0_CRC32(reg) != ID_AA64ISAR0_CRC32_NONE;
}
#endif // architecture

#ifdef HAS_IFUNC

#ifdef __clang__
__attribute__((unused)) // Suppress bogus "unused function" warning on clang
#endif
static uint32_t
(*resolve_crc32c (void))(uint32_t, const void *, unsigned)
{
#if defined(__amd64__) || defined(__i386__)
	if (has_sse42())
		return sse42_crc32c;
	else
#elif defined(__aarch64__)
	if (has_armv8_crc32c())
		return armv8_crc32c;
	else
#else
	if (true)
#endif
		return sw_crc32c;
}

uint32_t
crc32c(uint32_t crc, const void *buf, unsigned len) __attribute__((ifunc("resolve_crc32c")));

#else // !defined(HAS_IFUNC)

#if defined(__amd64__) || defined(__i386__)
static bool glob_has_sse42;
static void
__attribute__((constructor))
init_glob_has_sse42(void)
{
	glob_has_sse42 = has_sse42();
}
#elif defined(__aarch64__)
static bool glob_has_armv8_crc32c;
static void
__attribute__((constructor))
init_glob_has_armv8_crc32c(void)
{
	glob_has_armv8_crc32c = has_armv8_crc32c();
}
#endif

uint32_t
crc32c(uint32_t crc, const void *buf, unsigned len)
{
#if defined(__amd64__) || defined(__i386__)
	if (glob_has_sse42)
		return sse42_crc32c(crc, buf, len);
	else
#elif defined(__aarch64__)
	if (glob_has_armv8_crc32c)
		return armv8_crc32c(crc, buf, len);
	else
#else
	if (true)
#endif
		return sw_crc32c(crc, buf, len);
}

#endif // !defined(HAS_IFUNC)
