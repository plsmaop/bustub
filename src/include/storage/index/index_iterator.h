//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(Page *start_page, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator);
  IndexIterator(Page *start_page, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                int start_ind);
  ~IndexIterator();

  bool isEnd() const;

  const MappingType &operator*() const;

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const;

  bool operator!=(const IndexIterator &itr) const;

 private:
  // add your own private member variables here
  Page *cur_page_ = nullptr;
  BufferPoolManager *buffer_pool_manager_ = nullptr;
  LeafPage *cur_leaf_ = nullptr;
  KeyComparator comparator_;
  int cur_ind_ = 0;
};

}  // namespace bustub
