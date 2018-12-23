// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "leveldb/iterator.h"

namespace leveldb
{
// sstable 的数据由一个个的 block 组成。当持久化数据时,多份 KV 聚合成 block 一次写入;当读取时, 也是以 block 单位做 IO
struct BlockContents;
class Comparator;

class Block
{
public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents &contents);

  ~Block();

  size_t size() const { return size_; }
  Iterator *NewIterator(const Comparator *comparator);

private:
  uint32_t NumRestarts() const;

  const char *data_;        //block数据指针
  size_t size_;             //block数据大小
  uint32_t restart_offset_; // 重启点数组在data_中的偏移 Offset in data_ of restart array
  bool owned_;              // data_是否是Block拥有的 Block owns data_[]

  // No copying allowed
  Block(const Block &);
  void operator=(const Block &);

  class Iter;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_TABLE_BLOCK_H_
