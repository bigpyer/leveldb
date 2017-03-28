// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

//(Index)Block::Iter (Data)Block::Iter
class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_; // block操作函数
  void* arg_; // block function的自定义参数
  const ReadOptions options_; // BlockFunction的read option参数
  Status status_; // 当前状态
  IteratorWrapper index_iter_; // 遍历block的迭代器
  IteratorWrapper data_iter_; // May be NULL 遍历block data的迭代器
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL) {
}

TwoLevelIterator::~TwoLevelIterator() {
}

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock(); // 根据index iter设置data iter
  if (data_iter_.iter() != NULL) data_iter_.Seek(target); // 设置data iter跳转到target
  SkipEmptyDataBlocksForward(); // 调整iter，跳过空的block
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock(); // 根据index iter设置data iter
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward(); // 调整iter，跳过空的block
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock(); // 根据index iter设置data iter
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward(); // 调整iter，跳过空的block
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

// 向前跳过空的datablock
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) { // 跳到下一个block
    // Move to next block
    if (!index_iter_.Valid()) { // 如果index iter非法，设置data iteration为NULL
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst(); // 跳转到开始
  }
}

// 向后跳过空的datablock
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) { // 跳到前一个block
    // Move to next block
    if (!index_iter_.Valid()) { // 如果index iter非法，设置data iteration为NULL
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast(); // 跳转到开始
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

// 根据index_iter来初始化data_iter，当定位到新的block时，需要更新data Iterator，指向该block中k/v对的合适位置。
void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
