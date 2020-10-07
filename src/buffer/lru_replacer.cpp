//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (frame_id_list.empty()) {
    return false;
  }

  auto lru_frame_id = frame_id_list.back();
  *frame_id = lru_frame_id;
  list_iter_table.erase(lru_frame_id);
  frame_id_list.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto list_iter = list_iter_table.find(frame_id);
  if (list_iter == list_iter_table.end()) {
    return;
  }

  frame_id_list.erase(list_iter->second);
  list_iter_table.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (frame_id_list.size() >= num_pages) {
    return;
  }

  move_to_front(frame_id);
}

size_t LRUReplacer::Size() { return frame_id_list.size(); }

void LRUReplacer::move_to_front(frame_id_t frame_id) {
  auto list_iter = list_iter_table.find(frame_id);
  if (list_iter != list_iter_table.end()) {
    return;
  }

  frame_id_list.push_front(frame_id);
  list_iter_table[frame_id] = frame_id_list.begin();
}

}  // namespace bustub
