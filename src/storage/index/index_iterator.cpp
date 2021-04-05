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
INDEXITERATOR_TYPE::IndexIterator(Page *start_page, BufferPoolManager *buffer_pool_manager,
                                  const KeyComparator &comparator)
    : cur_page_(start_page), buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {
  if (isEnd()) {
    return;
  }

  cur_leaf_ = reinterpret_cast<LeafPage *>(cur_page_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *start_page, BufferPoolManager *buffer_pool_manager,
                                  const KeyComparator &comparator, int start_ind)
    : cur_page_(start_page), buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), cur_ind_(start_ind) {
  if (isEnd()) {
    return;
  }

  cur_leaf_ = reinterpret_cast<LeafPage *>(cur_page_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (isEnd()) {
    return;
  }

  cur_page_->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() const { return cur_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() const {
  if (isEnd()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index Reach End");
  }

  return cur_leaf_->GetItem(cur_ind_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index Reach End");
  }

  if (cur_ind_ + 1 < cur_leaf_->GetSize()) {
    ++cur_ind_;
    return *this;
  }

  auto nextPageId = cur_leaf_->GetNextPageId();
  if (nextPageId == INVALID_PAGE_ID) {
    // reach end

    cur_page_->RUnlatch();
    this->buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);

    cur_ind_ = 0;
    cur_page_ = nullptr;
    cur_leaf_ = nullptr;

    return *this;
  }

  auto nextPage = this->buffer_pool_manager_->FetchPage(nextPageId);
  nextPage->RLatch();
  cur_leaf_ = reinterpret_cast<LeafPage *>(nextPage->GetData());

  cur_page_->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);

  cur_page_ = nextPage;
  cur_ind_ = 0;

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

  return comparator_(cur_leaf_->KeyAt(cur_ind_), (*itr).first) == 0;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
