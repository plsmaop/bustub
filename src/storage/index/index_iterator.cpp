/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, BufferPoolManager *buffer_pool_manager,
                                  const KeyComparator &comparator, int start_ind)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), cur_page_id_(page_id), cur_ind_(start_ind) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() const { return cur_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() const {
  if (isEnd()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index Reach End");
  }

  auto page = this->buffer_pool_manager_->FetchPage(this->cur_page_id_);
  if (page == nullptr) {
    throw ExceptionType::OUT_OF_MEMORY;
  }

  page->RLatch();
  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());

  auto &item = leaf->GetItem(cur_ind_);

  this->ReleasePage(page);
  return item;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index Reach End");
  }

  auto page = this->buffer_pool_manager_->FetchPage(this->cur_page_id_);
  if (page == nullptr) {
    throw ExceptionType::OUT_OF_MEMORY;
  }

  page->RLatch();
  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());

  if (cur_ind_ + 1 < leaf->GetSize()) {
    ++cur_ind_;

    this->ReleasePage(page);
    return *this;
  }

  cur_ind_ = 0;
  cur_page_id_ = leaf->GetNextPageId();
  this->ReleasePage(page);

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if (isEnd() && itr.isEnd()) {
    return true;
  }

  if (isEnd() || itr.isEnd()) {
    return false;
  }

  auto page = this->buffer_pool_manager_->FetchPage(this->cur_page_id_);
  if (page == nullptr) {
    throw ExceptionType::OUT_OF_MEMORY;
  }

  page->RLatch();
  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());
  auto isEqual = this->comparator_(leaf->KeyAt(cur_ind_), (*itr).first) == 0;

  this->ReleasePage(page);

  return isEqual;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !(*this == itr); }

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::ReleasePage(Page *page) const {
  auto page_id = page->GetPageId();
  page->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(page_id, false);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
