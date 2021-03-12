//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "common/logger.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetSize(0);
  this->SetLSN(INVALID_LSN);
  this->SetMaxSize(max_size);
  this->SetNextPageId(INVALID_PAGE_ID);
  memset(this->array, 0, max_size * sizeof(MappingType));
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  auto ind = keyIndex(key, comparator);
  if (ind < 0) {
    return -(ind + 1);
  }

  return ind;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code

  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  auto ind = keyIndex(key, comparator);
  auto sz = this->GetSize();
  if (ind >= 0) {
    // key already existed
    return sz;
  }

  ind = -(ind + 1);
  if (ind < sz) {
    memmove(&array[ind + 1], &array[ind], sizeof(MappingType) * (sz - ind));
  }

  array[ind] = {key, value};

  this->SetSize(++sz);
  return sz;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // last half
  auto sz = this->GetSize();
  auto half = sz / 2;
  recipient->CopyNFrom(&array[half], sz - half);
  memset(&array[half], 0, sizeof(MappingType) * (sz - half));
  this->SetSize(half);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  auto sz = this->GetSize();
  memmove(&array[size], &array[0], sizeof(MappingType) * sz);
  memcpy(&array[0], items, sizeof(MappingType) * size);
  this->SetSize(sz + size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  auto ind = keyIndex(key, comparator);
  if (ind < 0) {
    return false;
  }

  *value = array[ind].second;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  auto ind = keyIndex(key, comparator);
  if (ind < 0) {
    return -1;
  }

  auto sz = this->GetSize();
  if (ind < sz - 1) {
    memmove(&array[ind], &array[ind + 1], (sz - ind - 1) * sizeof(MappingType));
  }

  this->SetSize(--sz);
  return sz;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(&array[0], this->GetSize());
  recipient->SetNextPageId(this->GetPageId());

  memset(array, 0, this->GetSize() * sizeof(MappingType));
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyLastFrom(array[0]);
  auto sz = this->GetSize();
  memmove(&array[0], &array[1], --sz * sizeof(MappingType));
  this->SetSize(sz);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  auto sz = this->GetSize();
  array[sz] = item;
  this->SetSize(sz + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  auto sz = this->GetSize();
  recipient->CopyFirstFrom(array[--sz]);
  memset(&array[sz], 0, sizeof(MappingType));
  this->SetSize(sz);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  auto sz = this->GetSize();
  memmove(&array[1], &array[0], sz * sizeof(MappingType));
  array[0] = item;
  this->SetSize(++sz);
}

/*
 * Binary Search to find the index of the given key
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::keyIndex(const KeyType &key, const KeyComparator &comparator) const {
  // binary search
  int l = 0, r = this->GetSize();
  while (l < r) {
    int mid = l + (r - l) / 2;
    auto &[_key, _] = array[mid];
    int result = comparator(key, _key);
    if (result == 0) {
      // equal
      return mid;
    } else if (result == -1) {
      // less
      r = mid;
    } else {
      l = mid + 1;
    }
  }

  // not found, but should be in index l
  return -l - 1;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
