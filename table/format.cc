// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

#include "util/zoad/controller.h"
#include <iostream>
namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  size_t n2 = (n + kBlockTrailerSize)/IO_SIZE;
  n2 *= IO_SIZE;
  if((n + kBlockTrailerSize) % IO_SIZE){
    n2 += IO_SIZE;
  }
  if(handle.offset()%IO_SIZE + n + kBlockTrailerSize > IO_SIZE){
    n2 += IO_SIZE - handle.offset()%IO_SIZE;
  }
  //char* buf = new char[n + kBlockTrailerSize];
  //char* buf = new char[n2 + (IO_SIZE - handle.offset()%IO_SIZE)];
  char* buf = new char[n2];

  Slice contents;
  //std::cout << "ReadBlock 1 " << handle.offset() << " " << n + kBlockTrailerSize << " " << n2 + IO_SIZE - handle.offset()%IO_SIZE << " " << n2 << " " << handle.offset()%IO_SIZE << std::endl; 
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  //std::cout << "ReadBlock 2 " << n2 << std::endl;
  //std::cout << "ReadBlock 3 " << IO_SIZE - handle.offset()%IO_SIZE << std::endl;
  
  if (!s.ok()) {
    delete[] buf;
    return s;
  }

  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }
  //std::cout << "ReadBlock 3 : " << contents.size() << " " << n2 << std::endl;

  switch (data[n]) {
    case kNoCompression:
  //std::cout << "ReadBlock 3.1" << std::endl;
      if (data != buf) {
  //std::cout << "ReadBlock 3.2" << std::endl;
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
  //std::cout << "ReadBlock 3.3" << std::endl;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
  //std::cout << "ReadBlock 3.4" << std::endl;
      } else {
  //std::cout << "ReadBlock 3.5" << std::endl;
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
  //std::cout << "ReadBlock 3.6" << std::endl;
      }

      // Ok
      break;
    case kSnappyCompression: {
  //std::cout << "ReadBlock 3.7" << std::endl;
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
  //std::cout << "ReadBlock 3.8" << std::endl;
        delete[] buf;
  //std::cout << "ReadBlock 3.9" << std::endl;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
  //std::cout << "ReadBlock 3.10" << std::endl;
        delete[] buf;
        delete[] ubuf;
  //std::cout << "ReadBlock 3.11" << std::endl;
        return Status::Corruption("corrupted compressed block contents");
      }
  //std::cout << "ReadBlock 3.12" << std::endl;
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
  //std::cout << "ReadBlock 3.13" << std::endl;
      break;
    }
    default:
  //std::cout << "ReadBlock 3.14" << std::endl;
  //std::cout << handle.offset() << " " << n + kBlockTrailerSize <<  " - data[n] : " << data[n] << std::endl;
  //std::cout << kBlockTrailerSize << std::endl;
  //exit(0);
      delete[] buf;
  //std::cout << "ReadBlock 3.15" << std::endl;
      return Status::Corruption("bad block type");
  }
//std::cout << "ReadBlock 4" << std::endl;


  return Status::OK();
}

}  // namespace leveldb
