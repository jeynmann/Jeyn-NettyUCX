package io.netty.channel.ucx;

public class NativeEpollEventArray {
    NativeEpollEventArray(int maxevents) {
        if (maxevents < 1) {
            throw new IllegalArgumentException("length must be >= 1 but was " + maxevents);
        }
        length = maxevents;
        address = create(maxevents);
    }

    public void increase() {
        int newlength = length << 1;
        long newAddress = create(newlength);

        NativeEpoll.memcopy(newAddress, address, length);

        length = newlength;
        address = newAddress;
    }

    public int length() {
        return length;
    }

    public long address() {
        return address;
    }

    public int fd(int id) {
        return NativeEpoll.fdFromEpollevents(address, id);
    }

    public int events(int id) {
        return NativeEpoll.eventsFromEpollevents(address, id);
    }

    public void close() {
        destroy(address);
    }

    protected static long create(int maxevents) {
        return NativeEpoll.newEpollevents(maxevents);
    }

    protected static void destroy(long address) {
        NativeEpoll.destroyEpollevents(address);
    }

    private long address;
    private int length;

}
