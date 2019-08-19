# recordlog

## About
Recordlog is a library for reading and writing data to record logs. Many records are stored together in a `RecordFile`.

Recordlog 是用来读写record log数据的包。 records 数据都存放在 RecordFile 中

## API Documentation
`RecordFile` is a generic interface for a collection of byte arrays to be addressed by 64 bit pointers.

`RecordFile` 是一个通用接口，可以返回被64位指针定位的byte数组

`RecordFile.Reader` provides iteration over a RecordFile and can start seeked to a position.

`RecordFile.Reader` 提供了可以从指定位置遍历RecordFile的迭代器

`RecordFile.Reader` has the following methods:

`boolean next()`
Seeks to next entry, returns true if one exists

获取下一个Entry，如果存在的话就返回true

`long getPosition()`
Returns position of current entry

返回当前Entry的位置

`E get()`
Gets value of current entry

获取当前Entry的值

`RecordFile.Writer` provides one method:

`long append(E entry)`
Appends the entry provided to the file and return the address used for accessing it later

将Entry添加到文件末尾，同时返回可以访问此Entry的地址。

There are three implementations of `RecordFile`.

RecordFile有如下3中实现：

`BasicRecordFile` stores entries sequentially as serialized entry byte length, CRC32 checksum, entry bytes.

`BasicRecordFile` 将数据以数据长度、CRC32校验和、数据，这样的格式顺序存储在文件中。

`BlockCompressedRecordFile` is a `RecordFile` implementation that compresses the byte[] entries in blocks of a given size.  The compression algorithm is pluggable.  The block size is configurable.  There are a maximum of 2^recordIndexBits records per block.  Each block is padded to a multiple of 2^padBits bytes.  Within each block there is a header containing an int for the number of entries and a list of varint encoded ints for the offsets into the block for each entry.  The address scheme uses the low recordIndexBits for the index in the record offset list of the record.  The rest of the bits are used for the address.  Since the low padBits bits are always 0 they are not stored.  There is an additional class called BlockCache which enables blocks to be cached across readers using a weak valued concurrent computing hash map.

`BlockCompressedRecordFile` 是RecordFile接口的一个实现；它将数据压缩存储在指定大小的Block中，其中压缩算法和Block大小是可配置的；每个Block最多存放有2^recordIndexBits条记录，每个Block的数据会被对齐成2的padBits次幂个字节；每个Block都包含一个Header，Header中包含记录的条数和一组变长、encode过的int数组，数组中记录了每条记录的偏移量。现在的寻址方案使用低位在数组中索引记录的偏移量；使用其余位作为地址。因为低paddits位总是0，所以不会存储它们。还有blockcache 类，它使用哈希Map在reader之间缓存Block。

`RecordLogDirectory` is a `RecordFile` implementation which stores lots of BlockCompressedRecordFiles in a directory in sequentially numbered segment files.  The writer provides the additional method roll(), which synchronizes the current file and starts a new one for future appends.  RecordLogDirectory uses the top 28 bits of the address for the segment file number, which limits each individual segment file to 4 GB (they can be slightly larger but the last addressable block is at address 2^32).  RecordLogDirectory maintains a fixed size cache of segments addressed by segment number and containing block caches.  The block caches are reference counted to make closing the file handles possible when they are evicted from the cache.

`RecordLogDirectory` 是一个RecordFile实现，它按序存储了很多BlockCompressedRecordFiles。Writer提供了roll()方法，它将当前文件同步到磁盘中，并创建一个新的用于添加记录的文件。RecordLogDirectory 使用文件地址的前28位，这也将每个文件限制为4 GB。RecordLogDirectory 维护了固定大小的cache，用于缓存segments 编号和其中包含block；Block通过引用计数法来保证当没有handler在使用它们时，它们会被从cache中移除。

## CompressedBlockRecordFile file format
```
record log: [block]*[MAX_INT (int)][metadata][metadata length (int)][total size]

block: [block size (int)][block checksum (int)][num records (int)][entry length (vint)]*[entry]*[padding]
                                               |----------------- compressed ------------------|
```

Metadata is arbitrary bytes that can be written as part of the record log. When using RecordLogAppender (see lsmtree-recordcache) this is unused and metadata is written to a separate file.

Block size is the byte length of the compressed portion of the block.

Checksum is an Adler-32 checksum of the compressed portion of the block.

Each entry length is a variable-length integer, representing the byte length of the corresponding entry.

Padding is used to align the start of block positions such that the address can be represented with fewer bits later.

## Address scheme

Addresses are represented by 64-bit values.

BlockCompressedRecordFile address scheme (not written as a RecordLogDirectory):
```
[block address: 54 bits][record index: 10 bits]
```

RecordLogDirectory address scheme:
```
[segment number: 28 bits][block address: 26 bits][record index: 10 bits]
```

In both of these schemes the block address is implicitly padBits (default 6) longer. For RecordLogDirectory record logs, this means the block address is 32 bits, but the last 6 bits must be zeroes. Thus the last block address can start at 0xffffffc0 and the max record file size is approximately 4 GB.

