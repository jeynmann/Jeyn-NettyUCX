package io.netty.channel.ucx;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URL;
import java.util.Locale;

class NativeEpollApi {
    static native int nativeEpollin();
    static native int nativeEpollout();
    static native int nativeEpollrdhup();
    static native int nativeEpollet();
    static native int nativeEpollerr();

    static native int nativeEpollCtlAdd();
    static native int nativeEpollCtlMod();
    static native int nativeEpollCtlDel();

    static native int nativeTimerFd();
    static native int nativeTimerFdRead(int fd);
    static native int nativeTimerFdSetTime(int fd, int sec, int nsec);

    static native int nativeEventFd();
    static native int nativeEventFdWrite(int fd, long value);
    static native int nativeEventFdRead(int fd);

    static native int nativeEpollCreate();
    static native int nativeEpollWait(int efd, long address, int maxevents, int timeout);
    static native int nativeEpollCtl(int efd, int op, int fd, int flags);
    static native int nativeEpollPolling(int efd, long address, int length);

    static native int nativeEPollEventSize();
    static native int nativeEPollDataOffset();

    static native long nativeNewEpollEvents(int maxevents);
    static native int nativeFdFromEpollEvents(long address, int id);
    static native int nativeEventsFromEpollEvents(long address, int id);

    static native long nativeMalloc(long size);
    static native void nativeFree(long address);
    static native void nativeClose(int fd);
    static native void nativeMemcpy(long dest, long src, long size);

    static native int nativeUcpWorkerArm(long nativeId);

    private static File extractDir = null;
    private static final String NATIVE_RESOURCE_HOME = "META-INF/native/";
    private static final String NAME = "native_epoll";
    static {
        loadNativeLibrary();
    }

    protected static void loadNativeLibrary() {
        String name = System.getProperty("os.name").toLowerCase(Locale.US).trim();
        if (!name.startsWith("linux")) {
            throw new IllegalStateException("Only supported on Linux");
        }

        String libName = System.mapLibraryName(NAME);
        String libpath = NATIVE_RESOURCE_HOME + libName;
        URL url = null;

        ClassLoader loader = NativeEpollApi.class.getClassLoader();
        if (loader == null) {
            url = ClassLoader.getSystemResource(libpath);
        } else {
            url = loader.getResource(libpath);
        }

        if (url == null) {
            // If not found in classpath, try to load from java.library.path
            System.loadLibrary(NAME);
            return;
        }

        File file;
        try {
            file = extractResource(url);
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("Failed to load native lib: " + e);
        }

        if (file != null && file.exists()) {
            System.load(file.getAbsolutePath());
            return;
        }

        throw new UnsatisfiedLinkError("Failed to load native lib: " + file.getName());
    }

    protected static File extractResource(URL libPath) throws IOException {
        if (extractDir == null) {
            Path tmp = Files.createTempDirectory(NAME);
            extractDir = tmp.toFile();
            extractDir.deleteOnExit();
        }

        InputStream is = libPath.openStream();
        if (is == null) {
            throw new IOException("Error extracting native library content");
        }

        File file = new File(extractDir, new File(libPath.getPath()).getName());
        file.deleteOnExit();
        FileOutputStream os = null;
        try {
            os = new FileOutputStream(file);
            streamcopy(is, os);
        } finally {
            if (os != null) {
                os.flush();
                os.close();
            }
            is.close();
        }
        return file;
    }

    protected static void streamcopy(InputStream is, OutputStream os)
        throws IOException {
        if (is == null || os == null) {
            return;
        }
        byte[] buffer = new byte[4096];
        int length = 0;
        while ((length = is.read(buffer)) != -1) {
            os.write(buffer, 0, length);
        }
    }
}

public class NativeEpoll extends NativeEpollApi {
    // EventLoop operations and constants
    public static final int EPOLLIN = nativeEpollin();
    public static final int EPOLLOUT = nativeEpollout();
    public static final int EPOLLRDHUP = nativeEpollrdhup();
    public static final int EPOLLET = nativeEpollet();
    public static final int EPOLLERR = nativeEpollerr();

    public static final int EPOLL_CTL_ADD = nativeEpollCtlAdd();
    public static final int EPOLL_CTL_MOD = nativeEpollCtlMod();
    public static final int EPOLL_CTL_DEL = nativeEpollCtlDel();

    public static final int EPOLL_EVENT_SIZE = nativeEPollEventSize();
    public static final int EPOLL_DATA_OFFSET = nativeEPollDataOffset();

    public static int newEventFd() throws IOException {
        int efd = nativeEventFd();
        if (efd < 0) {
            throw new IOException("eventfd: " + efd);
        }
        return efd;
    }

    public static int eventFdRead(int efd) throws IOException {
        int res = nativeEventFdRead(efd);
        if (res < 0) {
            throw new IOException("eventfd_read: " + res);
        }
        return res;
    }

    public static void eventFdWrite(int efd, long val) throws IOException {
        int res = nativeEventFdWrite(efd, val);
        if (res < 0) {
            throw new IOException("eventfd_write: " + res);
        }
    }

    public static int newTimerFd() throws IOException {
        int res = nativeTimerFd();
        if (res < 0) {
            throw new IOException("timerfd_create: " + res);
        }
        return res;
    }

    public static int timerFdRead(int fd) throws IOException {
        int res = nativeTimerFdRead(fd);
        if (res < 0) {
            throw new IOException("read: " + res);
        }
        return res;
    }

    public static int timerFdSetTime(int fd, int sec, int nsec) throws IOException {
        int res = nativeTimerFdSetTime(fd, sec, nsec);
        if (res < 0) {
            throw new IOException("timerfd_settime: " + res);
        }
        return res;
    }

    public static int newEpoll() throws IOException {
        int efd = nativeEpollCreate();
        if (efd < 0) {
            throw new IOException("epoll_create: " + efd);
        }
        return efd;
    }

    public static int epollWait(int epollFd, NativeEpollEventArray events, int timeMillis) throws IOException {
        int ready = nativeEpollWait(epollFd, events.address(), events.length(), timeMillis);
        if (ready < 0) {
            throw new IOException("epoll_wait: " + ready);
        }
        return ready;
    }

    public static int epollWait(int epollFd, NativeEpollEventArray events) throws IOException {
        int ready = nativeEpollWait(epollFd, events.address(), events.length(), -1);
        if (ready < 0) {
            throw new IOException("epoll_wait -1: " + ready);
        }
        return ready;
    }

    public static int epollNow(int epollFd, NativeEpollEventArray events) throws IOException {
        int ready = nativeEpollWait(epollFd, events.address(), events.length(), 0);
        if (ready < 0) {
            throw new IOException("epoll_wait 0: " + ready);
        }
        return ready;
    }

    public static int epollPolling(int epollFd, NativeEpollEventArray events) throws IOException {
        int ready = nativeEpollPolling(epollFd, events.address(), events.length());
        if (ready < 0) {
            throw new IOException("epoll_wait polling: " + ready);
        }
        return ready;
    }

    public static void epollCtlAdd(int efd, final int fd, final int flags) throws IOException {
        int res = nativeEpollCtl(efd, EPOLL_CTL_ADD, fd, flags);
        if (res < 0) {
            throw new IOException("epoll_ctl: " + res);
        }
    }

    public static void epollCtlMod(int efd, final int fd, final int flags) throws IOException {
        int res = nativeEpollCtl(efd, EPOLL_CTL_MOD, fd, flags);
        if (res < 0) {
            throw new IOException("epoll_ctl: " + res);
        }
    }

    public static void epollCtlDel(int efd, final int fd) throws IOException {
        int res = nativeEpollCtl(efd, EPOLL_CTL_DEL, fd, 0);
        if (res < 0) {
            throw new IOException("epoll_ctl: " + res);
        }
    }

    public static long newEpollevents(int maxevents) {
        return nativeNewEpollEvents(maxevents);
    }

    public static void destroyEpollevents(long address) {
        nativeFree(address);
    }

    public static int fdFromEpollevents(long address, int id) {
        return nativeFdFromEpollEvents(address, id);
    }

    public static int eventsFromEpollevents(long address, int id) {
        return nativeEventsFromEpollEvents(address, id);
    }

    public static void close(int fd) {
        nativeClose(fd);
    }

    public static long alloc(long size) {
        return nativeMalloc(size);
    }

    public static void free(long address) {
        nativeFree(address);
    }

    public static void memcpy(long dest, long src, long size) {
        nativeMemcpy(dest, src, size);
    }

    public static int ucpWorkerArm(long nativeId) {
        return nativeUcpWorkerArm(nativeId);
    }
}