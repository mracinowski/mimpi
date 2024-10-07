/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <errno.h>
#include <stddef.h>
#include <fcntl.h>

#define CHANNEL_TABLE ((MIMPI_CHANNEL_BASE) + 3 * (MIMPI_SIZE))

typedef enum {
	READ,
	WRITE
} direction_t;

void move_fd(int old, int new) {
	if (old == new) return;

	ASSERT_SYS_OK(dup2(old, new));
	ASSERT_SYS_OK(close(old));
}

void open_channel(int source, int destination) {
	if (source == destination) return;

	int fds[2];
	ASSERT_SYS_OK(channel(fds));

	int index = CHANNEL_TABLE + MIMPI_SIZE * source + destination;
	move_fd(fds[0], 2 * index);
	move_fd(fds[1], 2 * index + 1);
}

int get_channel(int source, int destination, direction_t direction) {
	if (source == destination) return -1;

	int result = CHANNEL_TABLE + MIMPI_SIZE * source + destination;
	result *= 2;
	result += direction;

	return result;
}

void close_channel(int source, int destination) {
	if (source == destination) return;

	int index = CHANNEL_TABLE + MIMPI_SIZE * source + destination;
	ASSERT_SYS_OK(close(2 * index));
	ASSERT_SYS_OK(close(2 * index + 1));
}

void open_channels(int size) {
	for (int source = 0; source < size; source++) {
		for (int destination = 0; destination < size; destination++) {
			open_channel(source, destination);
		}
	}
}

void prepare_channels(int rank, int size) {
	for (int i = 0; i < size; i++) {
		if (i == rank) continue;

		ASSERT_SYS_OK(dup2(get_channel(i, rank, READ),
			MIMPI_CHANNEL_READER + i));
	}

	for (int i = 0; i < size; i++) {
		if (i == rank) continue;

		ASSERT_SYS_OK(dup2(get_channel(rank, i, WRITE),
			MIMPI_CHANNEL_WRITER + i));
	}
}

void close_channels(int size) {
	for (int source = 0; source < size; source++) {
		for (int destination = 0; destination < size; destination++) {
			close_channel(source, destination);
		}
	}
}

void run_child(char *prog, char **args, int rank, int size) {
	prepare_channels(rank, size);
	close_channels(size);

	char mimpi_rank[32], mimpi_size[32];

	snprintf(mimpi_rank, 32, "MIMPI_RANK=%d", rank);
	putenv(mimpi_rank);

	snprintf(mimpi_size, 32, "MIMPI_SIZE=%d", size);
	putenv(mimpi_size);

	ASSERT_SYS_OK(execvp(prog, args));
}

int main(int argc, char **argv) {
	if (argc < 3) {
		return 1;
	}

	size_t size = strtol(argv[1], NULL, 10);

	if (size < 1 || size > MIMPI_SIZE) {
		return 1;
	}

	open_channels(size);

	pid_t pid[MIMPI_SIZE];

	for (int rank = 0; rank < size; rank++) {
		pid[rank] = fork();
		ASSERT_SYS_OK(pid[rank]);

		if (pid[rank] == 0) {
			run_child(argv[2], &argv[2], rank, size);
		}
	}

	close_channels(size);

	for (int rank = size - 1; rank >= 0; rank--) {
		wait(NULL);
	}

	return 0;
}
