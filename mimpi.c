/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>

/*** Constant definitions ****************************************************/

#define MIMPI_PACKET_SIZE 512
#define MIMPI_PREFIX_SIZE ((MIMPI_PACKET_SIZE) - sizeof(header_t))

#define MIMPI_GROUP_TAG -1
#define MIMPI_CLOSE_TAG -2
#define MIMPI_REQUEST_TAG -3

#define MIMPI_NOOP MIMPI_MAX
#define MIMPI_CHILDREN 2

/*** Structures **************************************************************/

typedef struct {
	size_t size;
	int tag;
} header_t;

typedef struct {
	header_t header;
	char data[MIMPI_PACKET_SIZE - sizeof(header_t)];
} prefix_t;

struct Outbox_Message {
	struct Outbox_Message *next;
	int tag;
	size_t size;
};

struct Outbox {
	struct Outbox_Message *top;
};

typedef enum {
	INBOX_MESSAGE,
	INBOX_REQUEST,
	INBOX_CLOSE,
	INBOX_GUARD,
	INBOX_DEADLOCK
} Inbox_Message_Type;

struct Inbox_Message {
	Inbox_Message_Type type;
	struct Inbox_Message *next;
	sem_t available;
	int tag;
	size_t size;
	void *data;
};

struct Inbox {
	int rank;
	struct Inbox_Message *front;
	struct Inbox_Message *back;
	size_t deadlocks;
};

/*** Global variables ********************************************************/

static bool MIMPI_deadlock_detection = false;
static pthread_t* MIMPI_receivers = NULL;
static struct Inbox* MIMPI_inboxes = NULL;
static struct Outbox* MIMPI_outboxes = NULL;

/*** Utilities ***************************************************************/

bool _MIMPI_Match(
	size_t expected_size,
	int expected_tag,
	size_t size,
	int tag
) {
	if (expected_size != size)
		return false;

	if (expected_tag == 0 || tag == 0)
		return true;

	return expected_tag == tag;
}

int readenv(int *result, const char *name) {
	char *number = getenv(name);

	if (number == NULL) {
		errno = ENOMEM;
		return -1;
	}

	*result = strtol(number, NULL, 10);

	if (result == 0 && errno == EINVAL) {
		return -1;
	}

	return 0;
}

void _MIMPI_Reduce(
	uint8_t *result,
	const uint8_t *other,
	size_t size,
	MIMPI_Op op
) {
	for (size_t i = 0; i < size; i++) {
		switch (op) {
			case MIMPI_MAX:
				if (other[i] > result[i]) result[i] = other[i];
				break;

			case MIMPI_MIN:
				if (other[i] < result[i]) result[i] = other[i];
				break;

			case MIMPI_SUM:
				result[i] += other[i];
				break;

			case MIMPI_PROD:
				result[i] *= other[i];
				break;
		}
	}
}

void _MIMPI_Get_Neighbours(
	int *parent,
	int *children,
	int rank,
	int root,
	int size
) {
	int index = (size + rank - root) % size + 1;

	if (index == 1) *parent = -1;
	else *parent = (index / MIMPI_CHILDREN + root - 1) % size;

	for (int i = 0; i < MIMPI_CHILDREN; i++) {
		int child = index * MIMPI_CHILDREN + i;

		if (child > size) {
			children[i] = -1;
			continue;
		}

		children[i] = (child + root - 1) % size;
	}
}

MIMPI_Retcode _MIMPI_Update_Retcode(
	MIMPI_Retcode retcode,
	MIMPI_Retcode other
) {
	if (
		retcode == MIMPI_ERROR_NO_SUCH_RANK ||
		other == MIMPI_ERROR_NO_SUCH_RANK
	) return MIMPI_ERROR_NO_SUCH_RANK;

	if (
		retcode == MIMPI_ERROR_ATTEMPTED_SELF_OP ||
		other == MIMPI_ERROR_ATTEMPTED_SELF_OP
	) return MIMPI_ERROR_ATTEMPTED_SELF_OP;

	if (
		retcode == MIMPI_ERROR_REMOTE_FINISHED ||
		other == MIMPI_ERROR_REMOTE_FINISHED
	) return MIMPI_ERROR_REMOTE_FINISHED;

	if (
		retcode == MIMPI_ERROR_DEADLOCK_DETECTED ||
		other == MIMPI_ERROR_DEADLOCK_DETECTED
	) return MIMPI_ERROR_DEADLOCK_DETECTED;

	return MIMPI_SUCCESS;
}

/*** Outbox ******************************************************************/

void Outbox_Init(
	struct Outbox *outbox
) {
	outbox->top = NULL;
}

void Outbox_Destroy(
	struct Outbox *outbox
) {
	struct Outbox_Message *message = outbox->top;

	while (message) {
		struct Outbox_Message *freeable = message;

		message = message->next;

		free(freeable);
	}
}

void Outbox_Push(
	struct Outbox *outbox,
	int tag,
	size_t size
) {
	struct Outbox_Message *message =
		(struct Outbox_Message*)malloc(sizeof(struct Outbox_Message));

	message->tag = tag;
	message->size = size;

	message->next = outbox->top;
	outbox->top = message;
}

bool Outbox_Pop(
	struct Outbox *outbox,
	int tag,
	size_t size
) {
	struct Outbox_Message *previous = NULL;
	struct Outbox_Message *message = outbox->top;

	while (message) {
		if (!_MIMPI_Match(size, tag, message->size, message->tag)) {
			previous = message;
			message = message->next;
			continue;
		}

		if (previous) previous->next = message->next;
		else outbox->top = message->next;
		free(message);

		return true;
	}

	return false;
}

/*** Inbox helpers ***********************************************************/

bool _Inbox_Match(
	struct Inbox_Message* message,
	int tag,
	size_t size
) {
	if (message->size != size)
		return false;

	if (message->tag == MIMPI_ANY_TAG || tag == MIMPI_ANY_TAG)
		return true;

	return message->tag == tag;
}

struct Inbox_Message *_Inbox_New(void) {
	struct Inbox_Message *result =
		(struct Inbox_Message*)malloc(sizeof(struct Inbox_Message));
	ASSERT_NOT_NULL(result);

	result->next = NULL;
	result->type = INBOX_GUARD;
	result->tag = 0;
	result->size = 0;
	result->data = NULL;
	ASSERT_SYS_OK(sem_init(&result->available, 0, 0));

	return result;
}

void _Inbox_Save(
	struct Inbox *inbox,
	Inbox_Message_Type type,
	int tag,
	size_t size,
	void *data
) {
	struct Inbox_Message *message = inbox->back;

	inbox->back = _Inbox_New();

	message->type = type;
	message->next = inbox->back;
	message->tag = tag;
	message->size = size;
	message->data = data;
	ASSERT_SYS_OK(sem_post(&message->available));
}

struct Inbox_Message *_Inbox_Delete(
	struct Inbox_Message *message
) {
	struct Inbox_Message *next = message->next;

	ASSERT_SYS_OK(sem_destroy(&message->available));
	if (message->data) free(message->data);
	free(message);

	return next;
}

/*** Inbox interface *********************************************************/

void Inbox_Init(
	struct Inbox *inbox,
	int rank
) {
	inbox->rank = rank;
	inbox->deadlocks = 0;

	inbox->front = _Inbox_New();
	ASSERT_SYS_OK(sem_post(&inbox->front->available));

	inbox->back = _Inbox_New();
	inbox->front->next = inbox->back;
}

void Inbox_Destroy(
	struct Inbox *inbox
) {
	struct Inbox_Message *message = inbox->front;
	while (message) {
		struct Inbox_Message *freeable = message;
		message = message->next;
		ASSERT_SYS_OK(sem_destroy(&freeable->available));
		if (freeable->data) free(freeable->data);
		free(freeable);
	}
}

void Inbox_Save_Message(
	struct Inbox *inbox,
	int tag,
	size_t size,
	void *data
) {
	_Inbox_Save(inbox, INBOX_MESSAGE, tag, size, data);
}

void Inbox_Save_Request(
	struct Inbox *inbox,
	int tag,
	size_t size
) {
	_Inbox_Save(inbox, INBOX_REQUEST, tag, size, NULL);
}

void Inbox_Close(
	struct Inbox *inbox
) {
	_Inbox_Save(inbox, INBOX_CLOSE, 0, 0, NULL);
}

MIMPI_Retcode Inbox_Retrieve(
	struct Inbox *inbox,
	int tag,
	size_t size,
	void *data
) {
	struct Inbox_Message* previous = NULL;
	struct Inbox_Message* message = inbox->front;

	while (true) {
		previous = message;
		message = message->next;
		ASSERT_SYS_OK(sem_wait(&message->available));
		ASSERT_SYS_OK(sem_post(&message->available));

		if (message->type == INBOX_CLOSE)
			return MIMPI_ERROR_REMOTE_FINISHED;

		if (message->type == INBOX_REQUEST) {
			if (!MIMPI_deadlock_detection) continue;

			size_t size = message->size;
			int tag = message->tag;

			previous->next = _Inbox_Delete(message);
			message = previous;

			if (Outbox_Pop(&MIMPI_outboxes[inbox->rank], size, 
				tag))
				continue;

			return MIMPI_ERROR_DEADLOCK_DETECTED;
		}

		if (message->type == INBOX_DEADLOCK) {
			if (!MIMPI_deadlock_detection) continue;

			previous->next = _Inbox_Delete(message);
			message = previous;

			continue;
		}

		if (message->type == INBOX_MESSAGE &&
			_Inbox_Match(message, tag, size)) {
			memmove(data, message->data, size);

			previous->next = _Inbox_Delete(message);

			return MIMPI_SUCCESS;
		}
	}

	return MIMPI_ERROR_REMOTE_FINISHED;
}

/*** Channel communications **************************************************/

ssize_t chrecv_all(int fd, void *buffer, size_t count) {
	ssize_t total = 0;

	while (count > 0) {
		ssize_t result = chrecv(fd, buffer, count);

		if (result <= 0) {
			return -1;
		}

		count -= result;
		total += result;
		buffer += result;
	}

	return total;
}


ssize_t chsend_all(int fd, const void *buffer, size_t count) {
	ssize_t total = count;

	while (count > 0) {
		ssize_t result = chsend(fd, buffer, count);

		if (result == -1) {
			return -1;
		}

		count -= result;
		buffer += result;
	}

	return total;
}

/*** Receiver ****************************************************************/

bool _Receiver_Receive(
	int *tag,
	size_t *size,
	void **data,
	int fd
) {
	prefix_t prefix;

	if (chrecv_all(fd, &prefix, sizeof(prefix_t)) <= 0)
		return false;

	*tag = prefix.header.tag;
	*size = prefix.header.size;

	if (!*size)
		return true;

	*data = malloc(*size);
	ASSERT_NOT_NULL(*data);

	size_t prefix_count = *size;
	if (prefix_count > MIMPI_PREFIX_SIZE) prefix_count = MIMPI_PREFIX_SIZE;
	size_t suffix_count = *size - prefix_count;

	memmove(*data, &prefix.data, prefix_count);

	if (*size <= MIMPI_PREFIX_SIZE)
		return true;

	if (chrecv_all(fd, *data + MIMPI_PREFIX_SIZE, suffix_count) <= 0) {
		free(*data);
		return false;
	}

	return true;
}

void *Receiver_Main(
	void* raw_inbox
) {
	struct Inbox* inbox = (struct Inbox*)raw_inbox;

	int fd = MIMPI_CHANNEL_READER + inbox->rank;
	int tag;
	size_t size;
	void *data;

	while (_Receiver_Receive(&tag, &size, &data, fd)) {
		if (tag == MIMPI_CLOSE_TAG) break;

		if (tag == MIMPI_REQUEST_TAG) {
			header_t *header = (header_t*)data;
			Inbox_Save_Request(inbox, header->tag, header->size);
			free(data);
			continue;
		}

		Inbox_Save_Message(inbox, tag, size, data);
	}

	ASSERT_SYS_OK(close(fd));

	Inbox_Close(inbox);

	return NULL;
}

/*** Direct communication ****************************************************/

MIMPI_Retcode _MIMPI_Send_Data(
	prefix_t const *prefix,
	void const *data,
	size_t data_count,
	int fd
) {
	if (chsend_all(fd, prefix, sizeof(prefix_t)) != sizeof(prefix_t))
		return MIMPI_ERROR_REMOTE_FINISHED;

	if (!data || !data_count)
		return MIMPI_SUCCESS;

	if (chsend_all(fd, data, data_count) != data_count)
		return MIMPI_ERROR_REMOTE_FINISHED;

	return MIMPI_SUCCESS;
}

MIMPI_Retcode _MIMPI_Send(
	void const *data,
	size_t count,
	int destination,
	int tag
) {
	if (destination == MIMPI_World_rank()) {
		return MIMPI_ERROR_ATTEMPTED_SELF_OP;
	}

	if (destination < 0 || destination >= MIMPI_World_size())
		return MIMPI_ERROR_NO_SUCH_RANK;

	prefix_t prefix;
	memset(&prefix, 0, sizeof(prefix_t));

	prefix.header.size = count;
	prefix.header.tag = tag;

	size_t prefix_count =
		count < MIMPI_PREFIX_SIZE ? count : MIMPI_PREFIX_SIZE;
	size_t suffix_count = (size_t)count - prefix_count;

	memcpy(&prefix.data, data, prefix_count);

	return _MIMPI_Send_Data(&prefix, data + prefix_count, suffix_count,
		MIMPI_CHANNEL_WRITER + destination);
}

MIMPI_Retcode _MIMPI_Recv(
	void *data,
	size_t count,
	int source,
	int tag
) {
	return Inbox_Retrieve(&MIMPI_inboxes[source], tag, count, data);
}

/*** Deadlock detection ******************************************************/

MIMPI_Retcode _MIMPI_Deadlock_Request(
	size_t count,
	int source,
	int tag
) {
	if (!MIMPI_deadlock_detection) return MIMPI_SUCCESS;

	header_t header;
	memset(&header, 0, sizeof(header_t));
	header.size = count;
	header.tag = tag;

	prefix_t prefix;
	memset(&prefix, 0, sizeof(prefix_t));
	prefix.header.size = sizeof(header_t);
	prefix.header.tag = MIMPI_REQUEST_TAG;
	memmove(&prefix.data, &header, sizeof(header_t));

	return _MIMPI_Send_Data(&prefix, NULL, 0,
		MIMPI_CHANNEL_WRITER + source);
}

/*** Group communication *****************************************************/

MIMPI_Retcode _MIMPI_Collect(
	int parent,
	int children[MIMPI_CHILDREN],
	void const *send_data,
	void *recv_data,
	size_t count,
	MIMPI_Op op
) {
	size_t size = count + sizeof(MIMPI_Retcode);

	uint8_t *data = (uint8_t*)malloc(size);
	ASSERT_NOT_NULL(data);

	MIMPI_Retcode *status = (MIMPI_Retcode*)(data + count);

	uint8_t *child_data = (uint8_t*)malloc(size);
	ASSERT_NOT_NULL(child_data);

	MIMPI_Retcode *child_status = (MIMPI_Retcode*)(child_data + count);

	if (count) memcpy(data, send_data, count);
	*status = MIMPI_SUCCESS;

	for (int i = 0; i < MIMPI_CHILDREN; i++) {
		if (children[i] == -1) continue;

		MIMPI_Retcode retcode =
			_MIMPI_Recv(child_data, size, children[i], MIMPI_GROUP_TAG);

		*status = _MIMPI_Update_Retcode(*status, retcode);

		if (retcode != MIMPI_SUCCESS) continue;

		*status = _MIMPI_Update_Retcode(*status, *child_status);
		_MIMPI_Reduce(data, child_data, count, op);
	}

	free(child_data);

	if (recv_data && count) memcpy(recv_data, data, count);

	if (parent != -1) {
		MIMPI_Retcode retcode =
			_MIMPI_Send(data, size, parent, MIMPI_GROUP_TAG);

		*status = _MIMPI_Update_Retcode(*status, retcode);
	}

	MIMPI_Retcode retcode = *status;

	free(data);

	return retcode;
}

MIMPI_Retcode _MIMPI_Distribute(
	int parent,
	int children[MIMPI_CHILDREN],
	void *recv_data,
	size_t count,
	MIMPI_Retcode initial_status
) {
	size_t size = count + sizeof(MIMPI_Retcode);

	uint8_t *data = (uint8_t*)malloc(size);
	ASSERT_NOT_NULL(data);

	MIMPI_Retcode *status = (MIMPI_Retcode*)(data + count);
	*status = initial_status;

	if (parent == -1) {
		if (count) memcpy(data, recv_data, count);
	} else {
		MIMPI_Retcode retcode =
			_MIMPI_Recv(data, size, parent, MIMPI_GROUP_TAG);

		*status = _MIMPI_Update_Retcode(*status, retcode);
	}

	for (int i = 0; i < MIMPI_CHILDREN; i++) {
		if (children[i] == -1) continue;

		MIMPI_Retcode retcode =
			_MIMPI_Send(data, size, children[i], MIMPI_GROUP_TAG);

		*status = _MIMPI_Update_Retcode(*status, retcode);
	}

	if (parent != -1 && *status == MIMPI_SUCCESS)
		memmove(recv_data, data, count);

	MIMPI_Retcode retcode = *status;

	free(data);

	return retcode;
}

/*** MIMPI interface *********************************************************/

void MIMPI_Init(bool enable_deadlock_detection) {
	channels_init();

	MIMPI_deadlock_detection = enable_deadlock_detection;

	MIMPI_inboxes =
		(struct Inbox*)malloc(MIMPI_World_size() *
		sizeof(struct Inbox));
	ASSERT_NOT_NULL(MIMPI_inboxes);

	MIMPI_receivers =
		(pthread_t*)malloc(MIMPI_World_size() * sizeof(pthread_t));
	ASSERT_NOT_NULL(MIMPI_receivers);

	if (MIMPI_deadlock_detection) {
		MIMPI_outboxes =
			(struct Outbox*)malloc(MIMPI_World_size() *
			sizeof(struct Outbox));
		ASSERT_NOT_NULL(MIMPI_outboxes);
	}

	for (int rank = 0; rank < MIMPI_World_size(); rank++) {
		if (rank == MIMPI_World_rank()) continue;

		Inbox_Init(&MIMPI_inboxes[rank], rank);

		if (MIMPI_outboxes)
			Outbox_Init(&MIMPI_outboxes[rank]);

		ASSERT_ZERO(pthread_create(&MIMPI_receivers[rank],
			NULL, Receiver_Main,
			(struct Inbox*)&MIMPI_inboxes[rank]));
	}
}

void MIMPI_Finalize() {
	channels_finalize();

	for (int rank = 0; rank < MIMPI_World_size(); rank++) {
		if (rank == MIMPI_World_rank()) continue;

		prefix_t prefix;
		memset(&prefix, 0, sizeof(prefix_t));

		prefix.header.tag = MIMPI_CLOSE_TAG;
		prefix.header.size = 0;

		_MIMPI_Send_Data(&prefix, NULL, 0, MIMPI_CHANNEL_WRITER + rank);
		ASSERT_SYS_OK(close(MIMPI_CHANNEL_WRITER + rank));
	}

	for (int rank = 0; rank < MIMPI_World_size(); rank++) {
		if (rank == MIMPI_World_rank()) continue;

		ASSERT_ZERO(pthread_join(MIMPI_receivers[rank], NULL));

		Inbox_Destroy(&MIMPI_inboxes[rank]);
		if (MIMPI_outboxes) Outbox_Destroy(&MIMPI_outboxes[rank]);
	}

	free(MIMPI_receivers);
	free(MIMPI_inboxes);
	if (MIMPI_outboxes) free(MIMPI_outboxes);
}

int MIMPI_World_size() {
	static int mimpi_size = -1;
	if (mimpi_size == -1)
		ASSERT_SYS_OK(readenv(&mimpi_size, "MIMPI_SIZE"));

	return mimpi_size;
}

int MIMPI_World_rank() {
	static int mimpi_rank = -1;
	if (mimpi_rank == -1)
		ASSERT_SYS_OK(readenv(&mimpi_rank, "MIMPI_RANK"));

	return mimpi_rank;
}

MIMPI_Retcode MIMPI_Send(
	void const *data,
	int count,
	int destination,
	int tag
) {
	MIMPI_Retcode retcode = _MIMPI_Send(data, count, destination, tag);

	if (MIMPI_outboxes && retcode == MIMPI_SUCCESS)
		Outbox_Push(&MIMPI_outboxes[destination], tag, count);

	return retcode;
}

MIMPI_Retcode MIMPI_Recv(
	void *data,
	int count,
	int source,
	int tag
) {
	if (source == MIMPI_World_rank()) {
		return MIMPI_ERROR_ATTEMPTED_SELF_OP;
	}

	if (source < 0 || source >= MIMPI_World_size()) {
		return MIMPI_ERROR_NO_SUCH_RANK;
	}

	MIMPI_Retcode retcode = _MIMPI_Deadlock_Request(count, source, tag);

	if (retcode != MIMPI_SUCCESS)
		return retcode;

	return _MIMPI_Recv(data, count, source, tag);
}

MIMPI_Retcode MIMPI_Barrier() {
	const int root = 0;
	int parent, children[MIMPI_CHILDREN];
	_MIMPI_Get_Neighbours(&parent, children, MIMPI_World_rank(), root,
		MIMPI_World_size());

	MIMPI_Retcode retcode = MIMPI_SUCCESS;
	retcode = _MIMPI_Collect(parent, children, NULL, NULL, 0, MIMPI_NOOP);
	retcode = _MIMPI_Distribute(parent, children, NULL, 0, retcode);
	return retcode;
}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root) {
	int parent, children[MIMPI_CHILDREN];
	_MIMPI_Get_Neighbours(&parent, children, MIMPI_World_rank(), root,
		MIMPI_World_size());

	MIMPI_Retcode retcode = MIMPI_SUCCESS;
	retcode = _MIMPI_Collect(parent, children, NULL, NULL, 0, MIMPI_NOOP);
	retcode = _MIMPI_Distribute(parent, children, data, count, retcode);
	return retcode;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
	int parent, children[MIMPI_CHILDREN];
	_MIMPI_Get_Neighbours(&parent, children, MIMPI_World_rank(), root,
		MIMPI_World_size());

	MIMPI_Retcode retcode = MIMPI_SUCCESS;
	retcode = MIMPI_World_rank() == root ?
		_MIMPI_Collect(parent, children, send_data, recv_data, count, op) :
		_MIMPI_Collect(parent, children, send_data, NULL, count, op);
	retcode = _MIMPI_Distribute(parent, children, NULL, 0, retcode);
	return retcode;
}
