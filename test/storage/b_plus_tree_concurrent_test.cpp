/**
 * b_plus_tree_test.cpp
 */

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <random>
#include <thread>                   // NOLINT
#include "b_plus_tree_test_util.h"  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

void GetHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                    int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      std::vector<RID> v;
      if (tree->GetValue(index_key, &v)) {
        LOG_DEBUG("GET: %u", v[0].GetSlotNum());
        tree->Remove(index_key, transaction);
      };
    }
  }
  delete transaction;
}

void FUCK(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                    int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  std::vector<RID> v;
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      
      tree->Insert(index_key, rid, transaction);
      tree->GetValue(index_key, &v);
      tree->Remove(index_key, transaction);
      tree->GetValue(index_key, &v);
      tree->Insert(index_key, rid, transaction);
      tree->Remove(index_key, transaction);
      tree->GetValue(index_key, &v);
      tree->Insert(index_key, rid, transaction);
      tree->Remove(index_key, transaction);
      tree->GetValue(index_key, &v);
      tree->Insert(index_key, rid, transaction);
      tree->Remove(index_key, transaction);
    }
  }

  for (const auto &rid : v) {
    LOG_DEBUG("GET: %u", rid.GetSlotNum());
  }

  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 3);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    // size = size + 1;
  }

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 1000; i++) {
    keys.push_back(i);
  }

  std::vector<int64_t> remove_keys(keys);
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  shuffle(remove_keys.begin(), remove_keys.end(), std::default_random_engine(seed));

  LaunchParallelTest(100, InsertHelperSplit, &tree, keys, 10);

  int64_t size = 0;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, 1000);

  // concurrent delete
  LaunchParallelTest(100, DeleteHelperSplit, &tree, remove_keys, 10);

  int64_t start_key = 2;
  size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  keys = {1, 2, 3, 4, 5};
  LaunchParallelTest(5, DeleteHelperSplit, &tree, keys, 5);

  size = 0;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, 0);

  size = 0;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, 0);

  LaunchParallelTest(5, DeleteHelperSplit, &tree, keys, 5);

  size = 0;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, 0);

  for (int i = 6; i <= 1000; i++) {
    keys.push_back(i);
  }

  LaunchParallelTest(100, InsertHelperSplit, &tree, keys, 10);

  size = 0;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, 1000);

  int64_t key = 42;
  // int64_t value = key & 0xFFFFFFFF;
  std::vector<RID> vs;
  GenericKey<8> ikey;
  ikey.SetFromInteger(key);
  EXPECT_EQ(tree.GetValue(ikey, &vs), true);
  EXPECT_EQ(vs[0].GetSlotNum(), key);

  auto page = tree.FindLeafPage(ikey, false);
  EXPECT_NE(page, nullptr);

  ikey.SetFromInteger(1000000);
  page = tree.FindLeafPage(ikey, false);
  // EXPECT_EQ(page, nullptr);

  std::vector<std::thread> thread_group;

  keys.clear();
  for (int i = 1001; i <= 2000; i++) {
    keys.push_back(i);
  }

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < 100; ++thread_itr) {
    thread_group.push_back(std::thread(InsertHelperSplit, &tree, keys, 10, thread_itr));
    thread_group.push_back(std::thread(DeleteHelperSplit, &tree, keys, 10, thread_itr));
    thread_group.push_back(std::thread(GetHelperSplit, &tree, keys, 10, thread_itr));
    thread_group.push_back(std::thread(DeleteHelperSplit, &tree, keys, 10, thread_itr));
    thread_group.push_back(std::thread(DeleteHelperSplit, &tree, keys, 10, thread_itr));
  }

  LaunchParallelTest(100, FUCK, &tree, keys, 10);

  // Join the threads with the main thread
  for (auto &thr : thread_group) {
    thr.join();
  }

  size = 0;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  // EXPECT_EQ(size, 2000);

  keys.clear();
  for (int i = 1; i <= 2000; i++) {
    keys.push_back(i);
  }

  LaunchParallelTest(100, DeleteHelperSplit, &tree, keys, 11);
  size = 0;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, 0);

  keys.clear();
  for (int i = 1; i <= 8000; i++) {
    keys.push_back(i);
  }

  LaunchParallelTest(500, FUCK, &tree, keys, 17);
  size = 0;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    LOG_DEBUG("%u", (*iterator).second.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, 0);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
