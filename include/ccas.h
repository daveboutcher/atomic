#ifndef __CCAS_H__
#define __CCAS_H__

#include <stdint.h>

/*
 * This data structure is the pointer/counter tuple used by the
 * 16 byte compare and swap.  These MUST be 16 byte aligned.
 */
struct counted_ptr {
	void *ptr;
	int64_t ctr;
} __attribute__((aligned(16)));

/*
 * 16 byte compare and swap.  This has the same semantics as the
 * __sync_bool_compare_and_swap, only using 16 byte values
 *
 * atomically compare old and mem, if they are the same then copy new
 * back to mem.
 *
 * The pseudo-code for the CMPXCHG instruction is:
 * if(EDX:EAX == Destination) {
 *    ZF = 1;
 *    Destination = ECX:EBX;
 * } else {
 *    ZF = 0;
 *    EDX:EAX = Destination;
 * }
 *
 * The counter is incremented by the inc value
 *
 * NOTE: This will NOT compile if the -fPIC (position independent code)
 *       gcc option is used, since EBX is used in PIC code generation.
 *       See http://www.technovelty.org/code/arch/pic-cas.html
 */
static inline int counted_compare_and_swap(struct counted_ptr *mem,
					   struct counted_ptr old,
					   void *newptr,
					   int64_t inc) {
	char result;
	struct counted_ptr new;

	/* The cmpxchg16b instruction requires 16 byte aligned memory */
	assert(((unsigned long)mem & 0x0F) == 0);
	assert(inc > 0);

	new.ptr = newptr;
	new.ctr = old.ctr+inc;

	__asm__ __volatile__("lock; cmpxchg16b %0; setz %1;"
			     : "=m"(*mem), "=q"(result)
			     : "m"(*mem), "d" (old.ctr), "a" (old.ptr),
			       "c" (new.ctr), "b" (new.ptr)
			     : "memory");
	return (int)result;
}

/* Return true of two counted pointers (including the counters) are
 * equal
 */
static inline bool counted_ptr_eq(struct counted_ptr a,
				  struct counted_ptr b)
{
	return ((a.ptr == b.ptr) &&
		(a.ctr == b.ctr));
}

#endif