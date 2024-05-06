/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class io_netty_channel_ucx_NativeEpollApi */

#ifndef _Included_io_netty_channel_ucx_NativeEpollApi
#define _Included_io_netty_channel_ucx_NativeEpollApi
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollin
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollin
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollout
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollout
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollrdhup
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollrdhup
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollet
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollet
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollerr
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollerr
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollCtlAdd
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCtlAdd
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollCtlMod
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCtlMod
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollCtlDel
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCtlDel
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeTimerFd
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeTimerFd
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeTimerFdRead
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeTimerFdRead
  (JNIEnv *, jclass, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeTimerFdSetTime
 * Signature: (III)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeTimerFdSetTime
  (JNIEnv *, jclass, jint, jint, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEventFd
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEventFd
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEventFdWrite
 * Signature: (IJ)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEventFdWrite
  (JNIEnv *, jclass, jint, jlong);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEventFdRead
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEventFdRead
  (JNIEnv *, jclass, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollCreate
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCreate
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollWait
 * Signature: (IJII)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollWait
  (JNIEnv *, jclass, jint, jlong, jint, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollCtl
 * Signature: (IIII)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollCtl
  (JNIEnv *, jclass, jint, jint, jint, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEpollPolling
 * Signature: (IJI)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEpollPolling
  (JNIEnv *, jclass, jint, jlong, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEPollEventSize
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEPollEventSize
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEPollDataOffset
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEPollDataOffset
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeNewEpollEvents
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeNewEpollEvents
  (JNIEnv *, jclass, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeFdFromEpollEvents
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeFdFromEpollEvents
  (JNIEnv *, jclass, jlong, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeEventsFromEpollEvents
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeEventsFromEpollEvents
  (JNIEnv *, jclass, jlong, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeORdonly
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeORdonly
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeOWronly
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeOWronly
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeORdwr
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeORdwr
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeProtRead
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeProtRead
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeProtWrite
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeProtWrite
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeProtExec
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeProtExec
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMapShared
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMapShared
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMapPrivate
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMapPrivate
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMapFixed
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMapFixed
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMapPopulate
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMapPopulate
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMapFailed
 * Signature: ()I
 */
JNIEXPORT jlong JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMapFailed
  (JNIEnv *, jclass);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMalloc
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMalloc
  (JNIEnv *, jclass, jlong);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeFree
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeFree
  (JNIEnv *, jclass, jlong);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeOpen
 * Signature: (Ljava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeOpen
  (JNIEnv *, jclass, jstring, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeClose
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeClose
  (JNIEnv *, jclass, jint);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMemcpy
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMemcpy
  (JNIEnv *, jclass, jlong, jlong, jlong);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMmap
 * Signature: (JJIIIJ)J
 */
JNIEXPORT jlong JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMmap
  (JNIEnv *, jclass, jlong, jlong, jint, jint, jint, jlong);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeMunmap
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeMunmap
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     io_netty_channel_ucx_NativeEpollApi
 * Method:    nativeUcpWorkerArm
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_io_netty_channel_ucx_NativeEpollApi_nativeUcpWorkerArm
  (JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif