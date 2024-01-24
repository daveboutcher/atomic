#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "atomic_q.h"
#include "util.h"
/*****************************************************************************
 * Unit tests for the atomic queue functions.  This sends a specified
 * number of messages from N sender threads to M receiver threads.
 *
 * to change the number of messages, edit the NMSG value
 * sender threads and receiver threads are specified in NUM_SENDERS and
 * NUM_RECEIVERS.
 *
 * In the main routine, update "repeat" if you want to run a more torturous
 * test (i.e. repeat a number of times.)
 *
 * The test validates that the correct number of messages is sent and
 * received.  Each message is given a numeric ID.  When it is sent, the
 * corresponding bit is turned on in a bit map, when it is received, we
 * validate that the bit was on and then turn it off.  This should detect
 * erroneous multiple sends or receives.
 *
 * This runs fine under valgrind.
 ****************************************************************************/

#define VERBOSE (0)

#define MAX_BIT (512)

struct mymsg {
	struct atomic_el amsg;
	long payload;
	char __pad[8];
} msgs[MAX_BIT];

#define ALLOC_BITS(map, x) unsigned long (map)[x/(8*sizeof(long))];

ALLOC_BITS(map, MAX_BIT);

static inline bool setbit(unsigned long *pmap, unsigned long bit)
{
        unsigned long idx = bit / (sizeof(long)*8);
        unsigned long x = 1LU << (bit % (sizeof(long) * 8));

        return ((__sync_fetch_and_or(pmap+idx, x) & x) != 0);
}

static inline bool clearbit(unsigned long *pmap, unsigned long bit)
{
        unsigned long idx = bit / (sizeof(long)*8);
        unsigned long x = (1LU << (bit % (sizeof(long) * 8)));
        unsigned long y = ~x;

        return ((__sync_fetch_and_and(pmap+idx, y) & x) != 0);
}

static inline bool testbit(unsigned long *pmap, unsigned long bit)
{
        unsigned long idx = bit / (sizeof(long)*8);
        unsigned long x = (1LU << (bit % (sizeof(long) * 8)));

        return ((pmap[idx] & x) != 0);
}

static struct mymsg *get_msg(void)
{
        static unsigned long cur_msg = 10;
        unsigned long ret = 0;
	struct mymsg *retmsg;

        do {
                ret = __sync_fetch_and_add(&cur_msg,1) % MAX_BIT;
        } while (setbit(map, ret));

	retmsg = msgs+ret;
	aq_el_init(&retmsg->amsg);
	return retmsg;
}

int num_free = 0;

static void free_atomic_msg(void *arg, struct atomic_el *msg)
{
	struct mymsg *m;

	__sync_fetch_and_add(&num_free, 1);
	assert((void *)0xbaddecaf == arg);

	/* if the message bit wasn't turned on, we have
	 * a bug
	 */
	m = container_of(msg, struct mymsg, amsg);
	if (!clearbit(map, (unsigned long)(m - msgs))) {
                printf("ERRROR: Received unexpected message\n");
        }
}

/* Number of messages to send/receive */
static const int NMSG      = 200000;       /* Number of messages to send/receive */
static const long SHUTDOWN = 9999999999L;
#define NUM_SENDERS (4)
#define NUM_RECEIVERS (4)
#define CAPACITY (64)

static long msgs_sent;
static long msgs_received;
static int cur_enqueued;

static void *sender(void *arg) {
        struct atomic_q *mb = (struct atomic_q *)arg;
	struct mymsg *msg;

        for (;;) {
		if (__sync_fetch_and_add(&msgs_sent, 1) >= NMSG) {
			__sync_fetch_and_sub(&msgs_sent, 1);
			return NULL;
		}

		cur_enqueued = aq_queued(mb);
		while (cur_enqueued > CAPACITY) {
                        sched_yield();
			cur_enqueued = aq_queued(mb);
		}

                msg = get_msg();
		msg->payload = msg - msgs;

                aq_enqueue(mb, &msg->amsg);
		if (VERBOSE)
			printf("S: sent %p\n",msg);
        }
}

static void *receiver(void *arg) {
        struct atomic_q *mb = (struct atomic_q *)arg;
        struct mymsg *msg;

        for (;;) {
                while ((msg = container_of(aq_dequeue(mb),
				   struct mymsg,
				   amsg)) == NULL) {
                        sched_yield();
                }
                if (msg->payload == SHUTDOWN) {
			aq_el_free(mb, &msg->amsg);
			return NULL;
                }

		if (VERBOSE)
			printf("R: received %p\n",msg);

                __sync_fetch_and_add(&msgs_received, 1);

                aq_el_free(mb, &msg->amsg);
		cur_enqueued = aq_queued(mb);
        }
}

int main(int argc, char **argv)
{
        pthread_t stid[NUM_SENDERS], rtid[NUM_RECEIVERS];
        struct atomic_q mb;
        int i, j;
        int repeat = 1; /* bump this number for more torturing */

        for (j=0; j < repeat; j++) {

		printf("atomic_q test: starting loop %d\n",j);

                /* Initialize */
                memset(map, 0x00, sizeof(map));

		aq_init(&mb,
			&get_msg()->amsg,
			free_atomic_msg,
			(void *)0xbaddecaf);

                msgs_sent = msgs_received = 0;

                assert(MAX_BIT > CAPACITY);

                for (i=0; i<NUM_SENDERS; i++) {
                        pthread_create(&stid[i],
                                       NULL,
                                       sender,
                                       &mb);
                }

                for (i=0; i<NUM_RECEIVERS; i++) {
                        pthread_create(&rtid[i],
                                       NULL,
                                       receiver,
                                       &mb);
                }

                /* Now wait for all the senders */
                for (i=0; i<NUM_SENDERS; i++) {
                        pthread_join(stid[i], NULL);
                }

                /* Send shutdown messages */
                for (i=0; i<NUM_RECEIVERS; i++) {
			struct mymsg *msg = get_msg();
			msg->payload = SHUTDOWN;
                        aq_enqueue(&mb, &msg->amsg);
                }

                /* Wait for all the receivers */
                for (i=0; i<NUM_RECEIVERS; i++) {
                        pthread_join(rtid[i], NULL);
                }

                if (!aq_empty(&mb)) {
                        printf("ERROR: Final queue not empty!\n");
                }

		aq_free(&mb);

		/* Make sure we sent/received the right number of messages */
                if (msgs_sent != msgs_received) {
                        printf("ERROR: Message counts not equal (%ld != %ld!\n",
                                msgs_sent,
                                msgs_received);
                }
                if (msgs_sent != NMSG) {
                        printf("ERROR: Message send count is wrong (%ld != %d!\n",
                                msgs_sent,
                                NMSG);
                }

		/* Make sure all the bits are turned off (i.e. we
		 * received all the messages we expect)
		 */
		for (i=0; i < MAX_BIT; i++)
                        if (testbit(map, (unsigned long)i)) {
                                printf("ERROR: message not received\n");
                        }
        }

        printf("atomic_q test: exchanged %ld messages\n", msgs_sent);

        return 0;
}
