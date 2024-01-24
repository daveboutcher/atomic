#ifndef __ATOMIC_Q_H__
#define __ATOMIC_Q_H__
#include <assert.h>
#include <stdbool.h>
#include <stddef.h>

#include "ccas.h"

/*****************************************************************************
 * author: Dave Boutcher <daveboutcher@gmail.com>    
 *****************************************************************************
 *
 * This header file implements a lockless FIFO queue that supports multiple
 * enqueuers and multiple dequeuers concurrently.  It is based heavily on
 * the implementation described in "Simple, Fast, and Practical Non-Blocking
 * and Blocking Concurrent Queue Algorithms" by Maged Michael and Micheal
 * Scott at the University of Rochester
 * (http://www.cs.rochester.edu/~scott/papers/1996_PODC_queues.pdf)
 *
 * It makes use of a variation on compare_and_swap (CAS) where each pointer
 * value always has a counter associated with it.  The counter is updated
 * on every CAS operation to detect the ABA case, described in the paper
 * as:
 *     if a process reads a value A in a shared location, computes a new
 *     value, and then attempts a compare and swap operation, the compare and
 *     swap may succeed when it should not, if between the read and the
 *     compare and swap some other process(es) change the A to a B and then
 *     back to an A again.
 *
 * The code makes use of a 16-byte atomic compare and swap.
 *
 * This implementation works fine between different processes, as long as
 * the queue structure itself (the struct atomic_q) is in shared memory,
 * and the freeer() function works with shared memory.
 *
 * The queue always has one dummy entry at the head.  The initial dummy entry
 * is passed in at initialization time and as things are dequeued from the
 * queue, the node for each dequeued element itself becomes the dummy entry
 * and we free the old dummy entry.
 *
 * There is one nuance of this implementation to be aware of.  The
 * implemenatation will sometimes READ from elements that have been dequeued
 * and freed using the freeer() routine.  It does this because it needs to
 * prefetch values before a CAS operation which may determine that the element
 * is no longer valid.  In practical terms this does not cause any problems,
 * though tools such as valgrind will detect the invalid read if the memory
 * is truly freed.  There is no problem if freed elements are kept in a pool
 * for later re-use.  The implementation never WRITES to freed elements.
 *
 * Because of the above, never call the freer you passed into
 * <aq_ini>t directly, instead call <aq_el_free>
 ****************************************************************************/

/*****************************************************************************
 ************************** EXTERNAL INTERFACES ******************************
 *****************************************************************************/

/* The atomic_q structure is the root of each queue.  aq_init()
 * should be called before it is used, and aq_free() when it is done.
 * It needs to be 16 byte aligned.
 */
struct atomic_q;

/* The field within a element being enqueued.  It needs to be 16 byte aligned.
 */
struct atomic_el;

/*
 * Initialize a queue.  The dummyel is an initial dummy element for
 * the queue that will be freed when the first element is dequeued.  The
 * "freeer()" function is called when each element can be freed. Since elements
 * become the "dummy" element at the head of the queue, this will be AFTER the
 * dequeue function returns the element.
 *
 * Elements MUST be 16 byte aligned
 *
 */
static inline void
aq_init(struct atomic_q *mb,
	struct atomic_el *dummyel,
	void (*freeer)(void *arg, struct atomic_el *),
	void *freeer_arg);


/*
 * Free a queue.  Note that no producers/consumers should
 * still be active when this is called (bad things will happen)
 * It frees all elements currently on the queue.
 */
static inline void
aq_free(struct atomic_q *mb);

/*
 * Enqueue a element.
 */
static inline long
aq_enqueue(struct atomic_q *mb, struct atomic_el *payload);

/*
 * Dequeue a element.  If the queue is empty, and block_policy is AQ_BLOCK
 * then the call will block. If the block_policy is AQ_NONBLOCK then NULL
 * will be returned. Otherwise, block_policy is a pointer to the maximum
 * amount of time to sleep--after which NULL is returned in no element
 * has arrived.
 */
static inline struct atomic_el *
aq_dequeue(struct atomic_q *mb);

/*
 * Check if a queue is empty
 */
static inline bool
aq_empty(const struct atomic_q * const mb);

/*
 * Return number of elements in the queue.  This is an upper bound (there
 * may in fact be less than this number of elements in the queue, depending
 * on race conditions.)
 */
static inline long
aq_queued(const struct atomic_q * const mb);


/*
 * Initialized the reference management information for the element.
 * This should only be called once for each element
 */
static inline void
aq_el_init(struct atomic_el *el);

/*
 * Should be called on the element when the user is done with said
 * element.
 */
static inline void
aq_el_free(struct atomic_q *mb, struct atomic_el *el);


/*****************************************************************************
 ************************** INTERNAL INTERFACES ******************************
 *
 * Everything below here is internal to the atomic queue implementation.
 *                             Here be dragons.
 *
 *****************************************************************************
 *****************************************************************************/

/*
 * The atomic element.  Users must not touch the first 16 bytes of the
 * element, even after it is dequeued.  It is in use until the "freeer"
 * function is called.
 */
struct atomic_el {
	struct counted_ptr next;
};

/*
 * The queue itself.  Pretty basic.
 *
 * separated into cache-lines, to prevent reader-, writer-, and
 * notifier-threads from invalidating the others' caches.
 */
struct atomic_q {
	void (*freeer)(void *, struct atomic_el *);
	void *freeer_arg;
	char _pad1[48];
	struct counted_ptr head;
	char _pad2[48];
	struct counted_ptr tail;
	char _pad3[48];
};

/* Convert a counted pointer to an atomic element */
static inline struct atomic_el
*aq_from_cp(const struct counted_ptr *cp)
{
	return (struct atomic_el *)cp->ptr;
}

/*
 * Initialize the queue.
 */
static inline void
aq_init(struct atomic_q *mb,
	struct atomic_el *dummyel,
	void (*freeer)(void *, struct atomic_el *),
	void *freeer_arg)
{
	/* The cmpxchg16b instruction requires 16 byte aligned memory */
	assert(((unsigned long)mb & 0x0F) == 0);
	assert(((unsigned long)dummyel & 0x0F) == 0);

	/* Allocate and set up the dummy node in the queue */
	dummyel->next.ptr = NULL;
	/* the dummy never is never returned from dequeue, so preset the
	   "refcount" to only need a single toggle */
	dummyel->next.ctr = 1L<<63;

	mb->head.ptr = dummyel;
	mb->tail.ptr = dummyel;
	mb->head.ctr = 0;
	mb->tail.ctr = 0;

	mb->freeer = freeer;
	mb->freeer_arg = freeer_arg;
}


static inline void
aq_el_init(struct atomic_el *el)
{
	el->next.ctr = 0;
}

/* Return true if the queue is empty */
static inline bool
aq_empty(const struct atomic_q * const mb)
{
	return (aq_from_cp(&mb->head)->next.ptr == NULL);
}

/* Number of elements on the queue */
static inline long
aq_queued(const struct atomic_q * const mb)
{
	/* Return the number of enqueues - number of dequeues */
	return mb->tail.ctr - mb->head.ctr;
}

static inline void
aq_free(struct atomic_q *mb)
{
	/* Dequeue and free all the elements from the queue */
	struct atomic_el *el = aq_from_cp(&mb->head);
	while (el) {
		if (counted_compare_and_swap(&mb->head,
					     mb->head,
					     el->next.ptr,
					     1))
			mb->freeer(mb->freeer_arg, el);
		el = aq_from_cp(&mb->head);
	}

	mb->head.ptr = mb->tail.ptr = NULL;
	mb->head.ctr = mb->tail.ctr = 0;
	mb->freeer = NULL;

}

static inline void
aq_el_free(struct atomic_q *mb, struct atomic_el *el)
{
	uint64_t i = __sync_fetch_and_xor((uint64_t *)&el->next.ctr, 1UL<<63);
	if ((i & 1UL<<63) != 0)
		mb->freeer(mb->freeer_arg, el);
}

/*
 * This is much like <aq_enqueue>, but it assumes that el is a NULL
 * terminated linked list.
 */
static inline long
aq_enqueue_multi(struct atomic_q *mb, struct atomic_el *el)
{
	struct counted_ptr tail, next;
	struct atomic_el *last_el = el;
	int64_t count = 1;

	/* Make sure the element is 16 byte aligned */
	assert(0 == ((unsigned long)el & 0x0F));
	assert(0 == (el->next.ctr & 1L<<63));

	/* Get the last element in the chain of elements we're adding */
	while (last_el->next.ptr != NULL) {
		assert((uint64_t)last_el != (uint64_t)last_el->next.ptr);
		count++;
		last_el = last_el->next.ptr;
	}

	for (;;) {
		tail = mb->tail;
		next = aq_from_cp(&tail)->next;
		assert(aq_from_cp(&tail) != el);

		/* Make sure the tail didn't just move.  If so, iterate.
		 */
		if (!counted_ptr_eq(tail,mb->tail))
			continue;

		/* If the next pointer is NULL, we are really
		 * at the tail and just atomically add the new
		 * element to the tail
		 */
		if (next.ptr == NULL) {
			/* We set the last element counted pointer
			 * counter value here. The pointer part is
			 * already NULL.  This just helps with some
			 * ABA problems where someone (i.e. our caller)
			 * initializes the counter value to zero.
			 * Null pointer/zero counter are too likely
			 * to occur at some later time
			 */
			last_el->next.ctr = tail.ctr;

			/* Atomically change the next pointer
			 * from NULL to our element. If someone
			 * else made a change in the meantime,
			 * next will no longer be NULL.
			 */
			if (counted_compare_and_swap(&aq_from_cp(&tail)->next,
						     next,
						     el,
						     1)) {
				break;
			}
		} else {
			/* the tail wasn't really pointing to
			 * the tail...advance it
			 */
			counted_compare_and_swap(&mb->tail,
						 tail,
						 next.ptr,
						 1);
		}
	}

	/* Move the tail pointer to the last element (if the
	 * tail hasn't moved in the mean-time)
	 */
	counted_compare_and_swap(&mb->tail,
				 tail,
				 last_el,
				 count);

	/*
	 * return number of elements on queue
	 */
	return mb->tail.ctr - mb->head.ctr;
}

static inline long
aq_enqueue(struct atomic_q *mb, struct atomic_el *el)
{
	el->next.ptr = NULL;
	return aq_enqueue_multi(mb, el);
}

static inline struct atomic_el *
aq_dequeue(struct atomic_q *mb)
{
	struct counted_ptr head, tail, next;

	for (;;) {
		head = mb->head;
		tail = mb->tail;
		next = aq_from_cp(&head)->next;

		/* If the head just moved under us, just iterate */
		if (!counted_ptr_eq(head,mb->head))
			continue;

		/* If head and tail point to the same entry, this MAY BE
		 * an empty queue.
		 */
		if (!next.ptr || (head.ptr == tail.ptr)) {
			/* If next is really NULL, nothing to return
			 */
			if (next.ptr == NULL) {
				return NULL;
			}
			/* In this case, tail wasn't really pointing
			 * to the tail.  Advance it and iterate
			 */
			counted_compare_and_swap(&mb->tail,
						 tail,
						 next.ptr,
						 1);
		} else {
			/* We're going to return next
			 */
			assert(next.ptr != NULL);

			/* Try and advance the head.  if this works,
			 * we're done
			 */
			if (counted_compare_and_swap(&mb->head,
						     head,
						     next.ptr,
						     1)) {
				break;
			}
		}
	}

	/* Free the head pointer */
	aq_el_free(mb, aq_from_cp(&head));

	return aq_from_cp(&next);
}

#endif
