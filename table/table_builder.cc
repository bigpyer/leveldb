// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options; // data blokc的选项
  Options index_block_options; // index block的选项
  WritableFile* file; // sstable文件
  uint64_t offset; // 要写入data block在sstable文件中的偏移，初始0
  Status status;
  BlockBuilder data_block; // 当前操作的data block
  BlockBuilder index_block; //sstable的index block
  std::string last_key; // 当前data block最后的k/v对的key
  int64_t num_entries; //当前data block的个数，初始0
  bool closed;          // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block; // 根据filter数据快速定位key是否在block中

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // 添加到index block的data block的信息 Handle to add to index block

  std::string compressed_output; // 压缩后的data block信息，临时存储，写入后即被清空

  //Filter block是存储的过滤器信息，它会存储{key, 对应data block在sstable的偏移值}，不一定是完全精确的，以快速定位给定key是否在data block中。
  Rep(const Options& opt, WritableFile* f)
      : options(opt), // data block选项
        index_block_options(opt), // index block选项
        file(f), // sstable文件
        offset(0), // 要写入data block在sstable文件中的偏移，初始0
        data_block(&options), // 当前操作的data block
        index_block(&index_block_options), // sstable的index block
        num_entries(0), // 当前data block的个数，初始0
        closed(false), // 调用了Finish() or Abandon()，初始false
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)), // 根据filter数据快速定位key是否在block中
        pending_index_entry(false) { // 添加到index block的data block的信息
    index_block_options.block_restart_interval = 1; // 压缩后的data block，临时存储，写入后即被清空
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

//首先保证文件没有close，也就是没有调用Finish/Abandon，以及保证当前status是ok的；如果当前有缓存的kv对，保证新加入的key是最大的。
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) { //表明遇到下一个data block的第一个kv对，根据key调整r->last_key，这是通过Comparator的FindShortestSeprator完成的
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding)); //将pending_handle加入到index block中，最后将r->pending_index_entry设置为false。
    r->pending_index_entry = false;
  }//直到遇到下一个data block的第一个key时，我们才会为上一个data block生成index entry，这样的好处是可以为index使用较短的key;比如上一个data block最后一个k/v的key是"the quick brown fox"，其后继data block的第一个key是"the who"，我们就可以用一个较短的字符串"the r"作为上一个data block的index block entry的key。

  if (r->filter_block != NULL) { //如果filter 不为空，就把key加入到filter block中
    r->filter_block->AddKey(key);
  }

  //设置r->last_key = key，将(key, value)添加到r->data_block中，并更新entry数。
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

//首先保证未关闭，且状态ok
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return; // data block是空的
  //保证pending_index_entry为false，即data block的Add已经完成
  assert(!r->pending_index_entry);
  // 写入data block，并设置其index entry信息—BlockHandle对象
  WriteBlock(&r->data_block, &r->pending_handle);
  //写入成功，则Flush文件，并设置r->pending_index_entry为true，  
  //以根据下一个data block的first key调整index entry的key—即r->last_key  
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != NULL) { ////将data block在sstable中的偏移加入到filter block中,并指明开始新的data block
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish(); // 获得data block的序列化字符串

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression: //不压缩
      block_contents = raw;
      break;

    case kSnappyCompression: { //snappy压缩格式
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        // 如果不支持Snappy，或者压缩率低于12.5%，依然当作不压缩存储
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  //将data内容写入到文件，并重置block成初始化状态，清空compressedoutput。
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset); //为index设置data block的handle信息
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents); //写入data block 内容
  if (r->status.ok()) { // 写入1byte的type和4bytes的crc32
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {// 写入成功更新offset-下一个data block的写入偏移
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

//调用Finish函数，表明调用者将所有已经添加的k/v对持久化到sstable，并关闭sstable文件。
Status TableBuilder::Finish() {
  //首先调用Flush，写入最后的一块data block，然后设置关闭标志closed=true。表明该sstable已经关闭，不能再添加k/v对。
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 写入filter block到文件中
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // 写入meta index block到文件中
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      //加入从"filter.Name"到filter data位置的映射
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  // 写入index block，如果成功Flush过data block，那么需要为最后一块data block设置index block，并加入到index block中。
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // 写入footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
