#ifndef __ATOMIC_STACK_H__
#define __ATOMIC_STACK_H__

#include "util/util.h"
/*****************************************************************************
 * author: Dave Boutcher <daveboutcher@gmail.com>
 *****************************************************************************
 *
 * This header file implements a lockless stack that supports multiple
 * concurrent users.  The only requirement is that the as_head structure
 * be 16 byte aligned (cince it uses the counted_compare_and_swap interfaces)
 *
 * An example:
 *
 * struct as_head foo;
 *
 * struct my_msg {
 *      uint64_t something;
 *      uint64_t something_else;
 *      struct as_entry ase;
 * } *msg;
 * struct as_entry *rcv;
 *   ...
 * as_init(&foo);
 *   ...
 * as_push(&foo, &msg->ase);
 *   ...
 * rcv = as_pop(&foo);
 * if (rcv == NULL)
 *    printf("queue was empty\n");
 * else
 *    msg = container_of(rcv,
 *                       struct my_msg,
 *                       ase);
 * ...
 *****************************************************************************
 */

/* Entries pushed on teh stack should have one of these inside them. */
struct as_entry {
	struct as_entry *next;
};

/* The head of the stack.  Call as_init() on this */
struct as_head {
	struct counted_ptr first;
};

/* Stack initializer. */
static inline void as_init(struct as_head *s)
{
	/* The cmpxchg16b instruction requires 16 byte aligned memory */
	assert(((unsigned long)s & 0x0F) == 0);

	s->first.ptr = NULL;
	s->first.ctr = 0;
}

/* Atomically push an entry on the stack */
static inline void as_push(struct as_head *s, struct as_entry *e)
{
	struct counted_ptr oldhead;
	do {
		oldhead = s->first;
		e->next = (struct as_entry *)oldhead.ptr;
		assert(e->next != e);
	} while (!counted_compare_and_swap(&s->first,
					   oldhead,
					   e,
					   1));
}

/* Atomically pop an entry from the stack */
static inline struct as_entry *as_pop(struct as_head *s)
{
	struct counted_ptr ret;

	do {
		ret = s->first;

		if (ret.ptr == NULL)
			return ret.ptr;

	} while (!counted_compare_and_swap(&s->first,
					   ret,
					   ((struct as_entry *)(ret.ptr))->next,
					   1));
	return ret.ptr;
}

/* Return true if the stack is empty */
static inline bool as_empty(struct as_head *s)
{
	return (s->first.ptr == NULL);
}

#endif /* __UTIL_ATIMIC_STACK_H__ */
