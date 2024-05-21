package jeyn.netty.cache

import java.util.concurrent.atomic.AtomicInteger
import java.io.IOException

class WriteWorker {
    ;
}

class ReadWorker {
    ;
}

class FilePage(val address: Long) {
    var state = new AtomicInteger()

    def rwComplete() = {
        state.set(FilePage.VALID)
    }

    def prepareWrite() = {
        state.compareAndSet(FilePage.VALID, FilePage.USED) ||
        state.compareAndSet(FilePage.INVALID, FilePage.USED)
    }

    def prepareRead() = {
        state.get() == FilePage.USED ||
        state.compareAndSet(FilePage.VALID, FilePage.USED) ||
        state.get() == FilePage.USED
    }

    def commitWrite() = {
        state.set(FilePage.VALID)
    }

    def commitRead() = {
        state.set(FilePage.VALID)
    }

    def isValid() = state.get() != FilePage.INVALID

    def length = FilePage.PAGE_SIZE
}

object FilePage {
    val PAGE_SIZE = 32 << 10

    val INVALID = 0
    val VALID = 1
    val USED = 2
}

class FileBlock {
    val pages = new Array[FilePage](FileBlock.PAGE_NUM)
}

object FileBlock {
    val BLOCK_SIZE = 16 << 20
    val PAGE_NUM = BLOCK_SIZE / FilePage.PAGE_SIZE
}

class File {
    var name: String = _

}

class FileCache {

}