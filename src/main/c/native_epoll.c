#include "native_epoll.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/utsname.h>
#include <stddef.h>
#include <limits.h>
#include <inttypes.h>
#include <link.h>
#include <time.h>
#include <sys/syscall.h>

// try epoll_create1
extern int epoll_create1(int flags) __attribute__((weak));

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEventFd(JNIEnv* env, jclass clazz) {
    return eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEventFdRead(JNIEnv* env, jclass clazz, jint fd) {
    uint64_t _;
    return eventfd_read(fd, &_);
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEventFdWrite(JNIEnv* env, jclass clazz, jint fd, jlong value) {
    jint ret;
    uint64_t _;

    for (;;) {
        ret = eventfd_write(fd, (eventfd_t) value);
        if (ret == 0) {
            return ret;
        }

        if (ret < 0 && errno != EAGAIN) {
            return ret;
        }

        ret = eventfd_read(fd, &_);
        if (ret < 0 && errno != EAGAIN) {
            return ret;
        }
    }

    return ret;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeTimerFd(JNIEnv* env, jclass clazz) {
    return timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeTimerFdRead(JNIEnv* env, jclass clazz, jint fd) {
    uint64_t _;
    return read(fd, &_, sizeof(uint64_t));
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeTimerFdSetTime(JNIEnv* env, jclass clazz, jint timerFd, jint sec, jint nanosec) {
    struct itimerspec ts = {};

    ts.it_value.tv_sec = sec;
    ts.it_value.tv_nsec = nanosec;

    return timerfd_settime(timerFd, 0, &ts, NULL);
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCreate(JNIEnv* env, jclass clazz) {
    jint efd, res;
    if (epoll_create1) {
        efd = epoll_create1(EPOLL_CLOEXEC);
    } else {
        efd = epoll_create(64); // size will be ignored
    }

    res = fcntl(efd, F_SETFD, FD_CLOEXEC);
    if (res < 0) {
        close(efd);
        return res;
    }

    return efd;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollWait(JNIEnv* env, jclass clazz, jint efd, jlong address, jint maxevents, jint timeout) {
    struct epoll_event *ev = (struct epoll_event*) (intptr_t) address;
    int result, err;

    do {
        result = epoll_wait(efd, ev, maxevents, timeout);
        if (result >= 0) {
            return result;
        }
    } while((err = errno) == EINTR);
    return -err;
}

// This method is deprecated!
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollPolling(JNIEnv* env, jclass clazz, jint efd, jlong address, jint maxevents) {
    struct epoll_event *ev = (struct epoll_event*) (intptr_t) address;
    int result, err;

    do {
        result = epoll_wait(efd, ev, maxevents, 0);
        if (result == 0) {
#if defined(__x86_64__)
            asm volatile("pause\n": : :"memory");
#endif
        }

        if (result >= 0) {
            return result;
        }
    } while((err = errno) == EINTR);

    return -err;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCtl(JNIEnv* env, jclass clazz, jint efd, jint op, jint fd, jint flags) {
    struct epoll_event ev = {
        .data.fd = fd,
        .events = flags
    };
    int res = epoll_ctl(efd, op, fd, &ev);
    if (res < 0) {
        return -errno;
    }
    return res;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollin(JNIEnv* env, jclass clazz) {
    return EPOLLIN;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollout(JNIEnv* env, jclass clazz) {
    return EPOLLOUT;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollrdhup(JNIEnv* env, jclass clazz) {
    return EPOLLRDHUP;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollet(JNIEnv* env, jclass clazz) {
    return EPOLLET;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollerr(JNIEnv* env, jclass clazz) {
    return EPOLLERR;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCtlAdd(JNIEnv* env, jclass clazz) {
    return EPOLL_CTL_ADD;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCtlMod(JNIEnv* env, jclass clazz) {
    return EPOLL_CTL_MOD;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCtlDel(JNIEnv* env, jclass clazz) {
    return EPOLL_CTL_DEL;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEPollEventSize(JNIEnv* env, jclass clazz) {
    return sizeof(struct epoll_event);
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEPollDataOffset(JNIEnv* env, jclass clazz) {
    return offsetof(struct epoll_event, data);
}

JNIEXPORT jlong JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeNewEpollEvents(JNIEnv* env, jclass clazz, jint maxevents) {
    return (jlong) malloc(sizeof(struct epoll_event) * maxevents);
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeFdFromEpollEvents(JNIEnv* env, jclass clazz, jlong address, jint id) {
    return ((struct epoll_event*) (intptr_t) address)[id].data.fd;
}

JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEventsFromEpollEvents(JNIEnv* env, jclass clazz, jlong address, jint id) {
    return ((struct epoll_event*) (intptr_t) address)[id].events;
}

JNIEXPORT jlong JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMalloc(JNIEnv* env, jclass clazz, jlong size) {
    return (jlong) malloc(size);
}

JNIEXPORT void JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeFree(JNIEnv* env, jclass clazz, jlong address) {
    free((void*) address);
}

JNIEXPORT void JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMemcopy(JNIEnv* env, jclass clazz, jlong dest, jlong src, jlong size) {
    memcpy((void*) dest, (void*) src, size);
}

JNIEXPORT void JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeClose(JNIEnv* env, jclass clazz, jint fd) {
    close(fd);
}
