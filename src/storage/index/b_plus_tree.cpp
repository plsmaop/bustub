//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  // root_page_id_latch_.RLock();
  return root_page_id_ == INVALID_PAGE_ID;
  // root_page_id_latch_.RUnlock();
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  root_page_id_latch_.RLock();
  if (this->IsEmpty()) {
    root_page_id_latch_.RUnlock();
    return false;
  }

  auto page = this->FindLeafPage(key, Operation::READ, transaction);
  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType v;

  auto isExisting = leaf->Lookup(key, &v, this->comparator_);
  if (isExisting) {
    result->emplace_back(v);
  }

  this->ReleasePrevRLatch(page);
  return isExisting;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LOG_DEBUG("Try Insert %ld", key.ToString());
  root_page_id_latch_.WLock();
  if (this->IsEmpty()) {
    this->StartNewTree(key, value);

    return true;
  }

  return this->InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  auto *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw ExceptionType::OUT_OF_MEMORY;
  }

  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, this->comparator_);

  root_page_id_ = page_id;
  buffer_pool_manager_->UnpinPage(page_id, true);

  UpdateRootPageId(1);
  root_page_id_latch_.WUnlock();
  LOG_DEBUG("Unlatch root page id: %d", page_id);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LOG_DEBUG("try find leaf for insertion");
  auto page = this->FindLeafPage(key, Operation::INSERT, transaction);
  LOG_DEBUG("find leaf for insertion: %d", page->GetPageId());

  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());
  auto sz = leaf->GetSize();
  auto isDuplicated = false;

  LOG_DEBUG("try insert into %d", leaf->GetPageId());
  if (sz == leaf->Insert(key, value, this->comparator_)) {
    // duplicated key
    isDuplicated = true;
  }

  auto isSplit = false;
  if (leaf->GetSize() == leaf->GetMaxSize()) {
    // split
    isSplit = true;
    auto newLeaf = this->Split<LeafPage>(leaf);

    this->InsertIntoParent(leaf, newLeaf->KeyAt(0), newLeaf, transaction);
  }

  this->ReleaseAllWLatches(transaction, isSplit);

  if (leaf->IsRootPage()) {
    root_page_id_latch_.WUnlock();
    LOG_DEBUG("Unlatch root page id: %d", leaf->GetPageId());
  }

  page->WUnlatch();
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  return !isDuplicated;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t pageId;
  auto *page = buffer_pool_manager_->NewPage(&pageId);
  if (page == nullptr) {
    throw ExceptionType::OUT_OF_MEMORY;
  }

  auto treePage = reinterpret_cast<N *>(page->GetData());
  treePage->Init(pageId, node->GetParentPageId(), node->GetMaxSize());
  if (node->IsLeafPage()) {
    auto leaf = reinterpret_cast<LeafPage *>(node);
    auto newLeaf = reinterpret_cast<LeafPage *>(treePage);
    leaf->MoveHalfTo(newLeaf);
    LOG_DEBUG("split leaf: %d -> %d", node->GetPageId(), treePage->GetPageId());

    newLeaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(newLeaf->GetPageId());
  } else {
    auto internal = reinterpret_cast<InternalPage *>(node);
    internal->MoveHalfTo(reinterpret_cast<InternalPage *>(treePage), this->buffer_pool_manager_);
    LOG_DEBUG("split internal: %d -> %d", node->GetPageId(), treePage->GetPageId());
  }

  return treePage;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  auto parentPageId = old_node->GetParentPageId();

  Page *parentPage = nullptr;
  InternalPage *parentInternalPage = nullptr;
  if (old_node->IsRootPage()) {
    parentPage = buffer_pool_manager_->NewPage(&parentPageId);
    if (parentPage == nullptr) {
      throw ExceptionType::OUT_OF_MEMORY;
    }

    parentInternalPage = reinterpret_cast<InternalPage *>(parentPage->GetData());
    parentInternalPage->Init(parentPageId, INVALID_PAGE_ID, this->internal_max_size_);
    parentInternalPage->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    root_page_id_ = parentPageId;
    old_node->SetParentPageId(parentPageId);
    new_node->SetParentPageId(parentPageId);

    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);

    UpdateRootPageId(0);

    LOG_DEBUG("Unlatch root page id: %d", parentPageId);
    this->root_page_id_latch_.WUnlock();
    return;
  }

  LOG_DEBUG("try insert into parent: %d from %d and %d", parentPageId, old_node->GetPageId(), new_node->GetPageId());
  parentPage = buffer_pool_manager_->FetchPage(parentPageId);
  parentInternalPage = reinterpret_cast<InternalPage *>(parentPage->GetData());
  parentInternalPage->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  LOG_DEBUG("finish insert into parent: %d", parentPageId);

  if (parentInternalPage->GetSize() == parentInternalPage->GetMaxSize() + 1) {
    // split internal
    LOG_DEBUG("try split internal: %d", parentPageId);
    auto newInternal = this->Split<InternalPage>(parentInternalPage);
    auto middleKey = newInternal->KeyAt(0);
    newInternal->SetKeyAt(0, KeyType());
    this->InsertIntoParent(parentInternalPage, middleKey, newInternal, transaction);
  }

  this->buffer_pool_manager_->UnpinPage(parentPageId, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  this->root_page_id_latch_.WLock();
  if (this->IsEmpty()) {
    this->root_page_id_latch_.WUnlock();
    return;
  }

  auto page = this->FindLeafPage(key, Operation::DELETE, transaction);
  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());

  LOG_DEBUG("try delete in %d", leaf->GetPageId());

  ValueType v;
  auto isExisted = leaf->Lookup(key, &v, this->comparator_);
  if (isExisted) {
    leaf->RemoveAndDeleteRecord(key, this->comparator_);
    // LOG_DEBUG("Add %d into deleted page set", v.GetPageId());
    // transaction->AddIntoDeletedPageSet(v.GetPageId());
  }

  auto isCoalescedOrRedistributed = false;
  if (leaf->GetSize() < leaf->GetMinSize()) {
    isCoalescedOrRedistributed = true;
    this->CoalesceOrRedistribute<LeafPage>(leaf, transaction);
  }

  this->ReleaseAllWLatches(transaction, isCoalescedOrRedistributed);

  if (leaf->IsRootPage()) {
    root_page_id_latch_.WUnlock();
  }

  page->WUnlatch();
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  for (const auto &deletedPageId : *transaction->GetDeletedPageSet()) {
    if (!this->buffer_pool_manager_->DeletePage(deletedPageId)) {
      LOG_INFO("Failed to delete page: %d", deletedPageId);
      throw Exception(ExceptionType::INVALID, "Failed to delete page");
    }
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    if (this->AdjustRoot(node)) {
      LOG_DEBUG("Add %d into deleted page set", node->GetPageId());
      transaction->AddIntoDeletedPageSet(node->GetPageId());
      return true;
    }

    return false;
  }

  auto parentPageId = node->GetParentPageId();
  auto pageId = node->GetPageId();

  LOG_DEBUG("try CoalesceOrRedistribute %d", pageId);
  auto parentPage = buffer_pool_manager_->FetchPage(parentPageId);
  auto parentInternalPage = reinterpret_cast<InternalPage *>(parentPage->GetData());

  auto nodeInd = parentInternalPage->ValueIndex(pageId);
  auto shouldRedistribute = false;
  auto fromLeft = false;
  decltype(parentPage) siblingPage = nullptr;
  N *siblingTreePage = nullptr;

  if (nodeInd == 0) {
    siblingPage = this->buffer_pool_manager_->FetchPage(parentInternalPage->ValueAt(1));

    siblingPage->WLatch();

    siblingTreePage = reinterpret_cast<N *>(siblingPage->GetData());
    shouldRedistribute = this->ShouldRedistribute(node, siblingTreePage);

  } else if (nodeInd == parentInternalPage->GetSize() - 1) {
    siblingPage = this->buffer_pool_manager_->FetchPage(parentInternalPage->ValueAt(nodeInd - 1));

    siblingPage->WLatch();

    siblingTreePage = reinterpret_cast<N *>(siblingPage->GetData());
    shouldRedistribute = this->ShouldRedistribute(node, siblingTreePage);
    fromLeft = true;

  } else {
    auto leftSiblingPage = this->buffer_pool_manager_->FetchPage(parentInternalPage->ValueAt(nodeInd - 1));
    auto rightSiblingPage = this->buffer_pool_manager_->FetchPage(parentInternalPage->ValueAt(nodeInd + 1));

    leftSiblingPage->WLatch();
    rightSiblingPage->WLatch();

    auto leftSiblingTreePage = reinterpret_cast<N *>(leftSiblingPage->GetData());
    auto rightSiblingTreePage = reinterpret_cast<N *>(rightSiblingPage->GetData());

    if (this->ShouldRedistribute(node, rightSiblingTreePage)) {
      shouldRedistribute = true;
      siblingPage = rightSiblingPage;
      siblingTreePage = rightSiblingTreePage;

      leftSiblingPage->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(leftSiblingPage->GetPageId(), false);

    } else if (this->ShouldRedistribute(node, leftSiblingTreePage)) {
      shouldRedistribute = true;
      fromLeft = true;
      siblingPage = leftSiblingPage;
      siblingTreePage = leftSiblingTreePage;

      rightSiblingPage->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(rightSiblingPage->GetPageId(), false);

    } else {
      siblingPage = rightSiblingPage;
      siblingTreePage = rightSiblingTreePage;

      leftSiblingPage->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(leftSiblingPage->GetPageId(), false);
    }
  }

  if (shouldRedistribute) {
    // redistribute
    this->Redistribute<N>(siblingTreePage, node, fromLeft, nodeInd, parentInternalPage);
  } else {
    // coalesce
    auto neighborNode = &node;
    auto coalescedNode = &siblingTreePage;
    auto nodeIndToBeDeleted = nodeInd + 1;
    if (fromLeft) {
      neighborNode = &siblingTreePage;
      coalescedNode = &node;
      nodeIndToBeDeleted = nodeInd;
    }

    this->Coalesce<N>(neighborNode, coalescedNode, &parentInternalPage, nodeIndToBeDeleted, transaction);
  }

  siblingPage->WUnlatch();
  this->buffer_pool_manager_->UnpinPage(siblingPage->GetPageId(), true);
  this->buffer_pool_manager_->UnpinPage(parentPageId, true);
  return !shouldRedistribute;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if ((*node)->IsLeafPage()) {
    auto leaf = reinterpret_cast<LeafPage *>(*node);
    auto leafSib = reinterpret_cast<LeafPage *>(*neighbor_node);

    leaf->MoveAllTo(leafSib);
    leafSib->SetNextPageId(leaf->GetNextPageId());
  } else {
    auto internal = reinterpret_cast<InternalPage *>(*node);
    auto internalSib = reinterpret_cast<InternalPage *>(*neighbor_node);

    internal->MoveAllTo(internalSib, (*parent)->KeyAt(index), this->buffer_pool_manager_);
  }

  (*parent)->Remove(index);
  
  transaction->AddIntoDeletedPageSet((*node)->GetPageId());
  LOG_DEBUG("Add %d into deleted page set", (*node)->GetPageId());
  if ((*parent)->GetSize() - 1 < (*parent)->GetMinSize()) {
    return this->CoalesceOrRedistribute<InternalPage>(*parent, transaction);
  }

  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, bool from_left, int node_ind, InternalPage *parent) {
  if (node->IsLeafPage()) {
    auto neighborLeaf = reinterpret_cast<LeafPage *>(neighbor_node);
    auto leaf = reinterpret_cast<LeafPage *>(node);

    if (from_left) {
      neighborLeaf->MoveLastToFrontOf(leaf);
      parent->SetKeyAt(node_ind, node->KeyAt(0));
      return;
    }

    neighborLeaf->MoveFirstToEndOf(leaf);
    parent->SetKeyAt(node_ind + 1, neighbor_node->KeyAt(0));
    return;
  }

  auto neighborInternal = reinterpret_cast<InternalPage *>(neighbor_node);
  auto internal = reinterpret_cast<InternalPage *>(node);

  if (from_left) {
    auto newKey = neighborInternal->KeyAt(neighborInternal->GetSize() - 1);
    neighborInternal->MoveLastToFrontOf(internal, parent->KeyAt(node_ind), this->buffer_pool_manager_);
    parent->SetKeyAt(node_ind, newKey);
    return;
  }

  auto newKey = neighborInternal->KeyAt(1);
  neighborInternal->MoveFirstToEndOf(internal, parent->KeyAt(node_ind + 1), this->buffer_pool_manager_);
  parent->SetKeyAt(node_ind + 1, newKey);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // auto pageId = old_root_node->GetPageId();

  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      this->root_page_id_ = INVALID_PAGE_ID;
      return true;
    }

    return false;
  }

  if (old_root_node->GetSize() == 1) {
    auto internalPage = reinterpret_cast<InternalPage *>(old_root_node);
    auto newRootPageId = internalPage->RemoveAndReturnOnlyChild();

    auto newRootPage = this->buffer_pool_manager_->FetchPage(newRootPageId);
    auto leafPage = reinterpret_cast<LeafPage *>(newRootPage->GetData());
    this->root_page_id_ = leafPage->GetPageId();
    leafPage->SetParentPageId(INVALID_PAGE_ID);

    // set to self to avoid unlatch root_page_id_
    old_root_node->SetParentPageId(old_root_node->GetPageId());

    this->buffer_pool_manager_->UnpinPage(newRootPageId, true);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");

  root_page_id_latch_.RLock();
  if (this->IsEmpty()) {
    root_page_id_latch_.RUnlock();
    return nullptr;
  }

  auto page = this->FindLeafPage(key, Operation::READ);
  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());
  if (leaf->IsRootPage()) {
    root_page_id_latch_.RUnlock();
  }

  this->ReleasePrevRLatch(page);
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation op, Transaction *transaction) {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  decltype(page) prevPage = nullptr;

  while (true) {
    if (op == Operation::READ) {
      page->RLatch();
    } else {
      page->WLatch();
    }

    auto treePage = reinterpret_cast<BPlusTreePage *>(page->GetData());
    auto sz = treePage->GetSize();

    if (treePage->IsLeafPage()) {
      if (op == Operation::READ) {
        this->ReleasePrevRLatch(prevPage);
      } else {
        auto isSafe = op == Operation::INSERT ? sz + 1 < treePage->GetMaxSize() : sz - 1 >= treePage->GetMinSize();
        if (isSafe) {
          this->ReleaseAllWLatches(transaction, false);
        }
      }

      return page;
    }

    auto internal = reinterpret_cast<InternalPage *>(treePage);
    if (op == Operation::READ) {
      this->ReleasePrevRLatch(prevPage);

      prevPage = page;
    } else {
      auto isSafe =
          op == Operation::INSERT ? sz + 1 < internal->GetMaxSize() + 1 : sz - 1 >= internal->GetMinSize() + 1;

      if (isSafe) {
        this->ReleaseAllWLatches(transaction, false);
      }

      transaction->AddIntoPageSet(page);
    }

    auto pageId = internal->Lookup(key, this->comparator_);
    auto childPage = buffer_pool_manager_->FetchPage(pageId);
    page = childPage;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllWLatches(Transaction *transaction, bool isDirty) {
  for (const auto &prevPage : *transaction->GetPageSet()) {
    if (reinterpret_cast<BPlusTreePage *>(prevPage->GetData())->IsRootPage()) {
      root_page_id_latch_.WUnlock();
      LOG_DEBUG("Unlatch root page id: %d", prevPage->GetPageId());
    }

    prevPage->WUnlatch();
    this->buffer_pool_manager_->UnpinPage(prevPage->GetPageId(), isDirty);
    LOG_DEBUG("Unlatch page id: %d", prevPage->GetPageId());
  }

  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleasePrevRLatch(Page *prevPage) {
  if (prevPage == nullptr) {
    return;
  }

  if (reinterpret_cast<BPlusTreePage *>(prevPage->GetData())->IsRootPage()) {
    root_page_id_latch_.RUnlock();
  }

  prevPage->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(prevPage->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::ShouldRedistribute(BPlusTreePage *node, BPlusTreePage *neighbor_node) const {
  if ((node->IsLeafPage() && node->GetSize() + neighbor_node->GetSize() > node->GetMaxSize()) ||
      (!node->IsLeafPage() && node->GetSize() + neighbor_node->GetSize() - 1 > node->GetMaxSize())) {
    return true;
  }

  return false;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
