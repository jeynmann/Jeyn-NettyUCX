import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

object RockEg {
    def main(args: Array[String]): Unit = {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        val options = new Options().setCreateIfMissing(true).setUseDirectIoForFlushAndCompaction(true)
        try {
            // a factory method that returns a RocksDB instance
            val db = RocksDB.open(options, "/tmp/tmp.db")
        } catch {
            // do some error handling
            case e: RocksDBException => {}
        }
    }
}