//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetSize(0);
  this->SetLSN(INVALID_LSN);
  this->SetMaxSize(max_size);
  for (int i = 0; i < max_size; ++i) {
    this->array[i] = MappingType();
  }
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code

  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  auto sz = this->GetSize();
  for (decltype(sz) i = 0; i < sz; ++i) {
    if (array[i].second == value) {
      return i;
    }
  }

  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  if (comparator(key, this->KeyAt(1)) == -1) {
    return this->ValueAt(0);
  }

  auto ind = keyIndex(key, comparator);
  if (ind < 0) {
    auto pos = -(ind + 1);
    return this->ValueAt(pos - 1);
  }

  return array[ind].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0] = {KeyType(), old_value};
  array[1] = {new_key, new_value};
  this->SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  auto valueInd = ValueIndex(old_value);
  auto sz = this->GetSize();
  if (valueInd == -1) {
    return sz;
  }

  for (int i = sz; i > valueInd + 1; --i) {
    array[i] = array[i - 1];
  }

  array[valueInd + 1] = {new_key, new_value};
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // last half
  auto sz = this->GetSize();
  auto half = sz / 2;
  recipient->CopyNFrom(&array[half], sz - half, buffer_pool_manager);
  // memset(&array[half], 0, sizeof(MappingType) * (sz - half));
  this->SetSize(half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  auto sz = this->GetSize();
  for (int i = 0; i < size; ++i) {
    array[i + sz] = items[i];
  }

  this->SetSize(sz + size);

  for (int i = 0; i < size; ++i) {
    this->updateParentPageId(items[i], buffer_pool_manager);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  auto sz = this->GetSize();

  for (int i = index; i + 1 < sz; ++i) {
    array[i] = array[i + 1];
  }

  this->SetSize(--sz);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  auto sz = this->GetSize();
  auto pageId = array[0].second;
  // memset(array, 0, sz * sizeof(MappingType));
  this->SetSize(--sz);
  return pageId;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0, middle_key);
  auto sz = this->GetSize();
  recipient->CopyNFrom(array, sz, buffer_pool_manager);
  // memset(array, 0, sizeof(MappingType) * sz);
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(array[0], buffer_pool_manager);

  auto sz = this->GetSize();
  for (int i = 0; i + 1 < sz; ++i) {
    array[i] = array[i + 1];
  }

  this->SetKeyAt(0, KeyType());
  this->SetSize(--sz);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array[this->GetSize()] = pair;
  this->IncreaseSize(1);

  this->updateParentPageId(pair, buffer_pool_manager);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->SetKeyAt(0, middle_key);

  auto sz = this->GetSize();
  this->SetKeyAt(--sz, KeyType());

  recipient->CopyFirstFrom(array[sz], buffer_pool_manager);
  // memset(&array[sz], 0, sizeof(MappingType));
  this->SetSize(sz);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto sz = this->GetSize();
  for (int i = sz; i > 0; ++i) {
    array[i] = array[i - 1];
  }

  array[0] = pair;
  this->IncreaseSize(1);

  this->updateParentPageId(pair, buffer_pool_manager);
}

/*
 * Do binary search to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::keyIndex(const KeyType &key, const KeyComparator &comparator) const {
  // binary search
  int l = 1;
  int r = this->GetSize();
  while (l < r) {
    int mid = l + (r - l) / 2;
    auto &_key = array[mid].first;
    int result = comparator(key, _key);
    if (result == 0) {
      // equal
      return mid;
    }

    if (result == -1) {
      // less
      r = mid;
    } else {
      l = mid + 1;
    }
  }

  // not found, but should be in index l
  return -l - 1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::updateParentPageId(const MappingType &pair,
                                                        BufferPoolManager *buffer_pool_manager) const {
  auto pageId = pair.second;
  auto page = buffer_pool_manager->FetchPage(pageId);
  auto treePage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  treePage->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(pageId, true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
