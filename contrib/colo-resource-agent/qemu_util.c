/*
 * Utilites, copied from qemu
 *
 * Copyright (c) 2003-2008 Fabrice Bellard
 * Copyright (c) 2010 Red Hat, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// For O_CLOEXEC
#define _GNU_SOURCE

#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>

#include <glib-2.0/glib-unix.h>

size_t colod_write_full(int fd, const uint8_t *buf, size_t count)
{
    ssize_t ret = 0;
    size_t total = 0;

    while (count) {
        ret = write(fd, buf, count);
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        count -= ret;
        buf += ret;
        total += ret;
    }

    return total;
}

size_t colod_read_full(int fd, uint8_t *buf, size_t count)
{
    ssize_t ret = 0;
    size_t total = 0;

    while (count) {
        ret = read(fd, buf, count);
        if (ret <= 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        count -= ret;
        buf += ret;
        total += ret;
    }

    return total;
}

gboolean colod_write_pidfile(const char *path, GError **errp)
{
    int fd;
    ssize_t ret;
    char pidstr[32];

    while (1) {
        struct stat a, b;
        struct flock lock = {
            .l_type = F_WRLCK,
            .l_whence = SEEK_SET,
            .l_len = 0,
        };

        fd = open(path, O_WRONLY | O_CREAT | O_CLOEXEC, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            colod_error_set(errp, "Failed to open pid file: %s",
                            g_strerror(errno));
            return FALSE;
        }

        if (fstat(fd, &b) < 0) {
            colod_error_set(errp, "Cannot stat file: %s", g_strerror(errno));
            goto fail_close;
        }

        if (fcntl(fd, F_SETLK, &lock)) {
            colod_error_set(errp, "Cannot lock pid file: %s",
                            g_strerror(errno));
            goto fail_close;
        }

        /*
         * Now make sure the path we locked is the same one that now
         * exists on the filesystem.
         */
        if (stat(path, &a) < 0) {
            /*
             * PID file disappeared, someone else must be racing with
             * us, so try again.
             */
            close(fd);
            continue;
        }

        if (a.st_ino == b.st_ino) {
            break;
        }

        /*
         * PID file was recreated, someone else must be racing with
         * us, so try again.
         */
        close(fd);
    }

    if (ftruncate(fd, 0) < 0) {
        colod_error_set(errp, "Failed to truncate pid file: %s",
                        g_strerror(errno));
        goto fail_unlink;
    }

    snprintf(pidstr, sizeof(pidstr), "%d\n", getpid());
    ret = colod_write_full(fd, (uint8_t *)pidstr, strlen(pidstr));
    if ((size_t) ret != strlen(pidstr)) {
        colod_error_set(errp, "Failed to write pid file: %s",
                        g_strerror(errno));
        goto fail_unlink;
    }

    return TRUE;

fail_unlink:
    unlink(path);
fail_close:
    close(fd);
    return FALSE;
}

int os_daemonize(void)
{
    pid_t pid;
    int fds[2];

    if (!g_unix_open_pipe(fds, FD_CLOEXEC, NULL)) {
        exit(EXIT_FAILURE);
    }

    pid = fork();
    if (pid > 0) {
        // Topmost parent process waits for final child process to init
        uint8_t status;
        ssize_t len;

        close(fds[1]);

        len = colod_read_full(fds[0], &status, 1);

        /* only exit successfully if our child actually wrote
         * a one-byte zero to our pipe, upon successful init */
        if (len == 1 && status == 0) {
            exit(EXIT_SUCCESS);
        } else {
            exit(EXIT_FAILURE);
        }
    } else if (pid < 0) {
        exit(EXIT_FAILURE);
    }

    close(fds[0]);

    if (setsid() < 0) {
        exit(EXIT_FAILURE);
    }

    signal(SIGCHLD, SIG_IGN);
    signal(SIGHUP, SIG_IGN);

    pid = fork();
    if (pid > 0) {
        exit(EXIT_SUCCESS);
    } else if (pid < 0) {
        exit(EXIT_FAILURE);
    }

    umask(027);

    if (chdir("/") < 0) {
        exit(EXIT_FAILURE);
    }

    /* Close all open file descriptors */
    int fd;
    for (fd = sysconf(_SC_OPEN_MAX); fd>=0; fd--)
    {
        if (fd == fds[1])
            continue;

        close(fd);
    }

    signal(SIGTSTP, SIG_IGN);
    signal(SIGTTOU, SIG_IGN);
    signal(SIGTTIN, SIG_IGN);

    return fds[1];
}

int os_daemonize_post_init(int pipe, GError **errp)
{
    uint8_t status = 0;
    ssize_t len;

    len = colod_write_full(pipe, &status, 1);
    if (len != 1) {
        colod_error_set(errp, "Failed to signal success: %s",
                        g_strerror(errno));
        close(pipe);
        return -1;
    }

    close(pipe);
    return 0;
}
