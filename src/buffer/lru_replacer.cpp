#include "buffer/lru_replacer.h"
#include <iostream>
#include "glog/logging.h"

LRUReplacer::LRUReplacer(size_t num_pages): num_victims_(0), max_num_victims_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (victim_list_.empty()) {
//    LOG(INFO) << "LRUReplacer::Victim() failed: No victim found." << std::endl;
    *frame_id = INVALID_FRAME_ID;
    return false;
  }
  *frame_id = victim_list_.front();
  victim_list_.pop_front();
  num_victims_--;
//  LOG(INFO) << "LRUReplacer::Victim() called: " << "Victimized frame: " << *frame_id << ", NumVictims: " << num_victims_
//            << ", MaxNumVictims: " << max_num_victims_ << std::endl;
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  for (std::deque<frame_id_t>::iterator i = victim_list_.begin(); i != victim_list_.end(); i++) {
    if (*i == frame_id) {
      victim_list_.erase(i);
      num_victims_--;
      pinned_list_.insert(frame_id);
      max_num_victims_--;
//      LOG(INFO) << "LRUReplacer::Pin() called: " << "Pinned frame: " << frame_id << ", NumVictims: " << num_victims_
//                << ", MaxNumVictims: " << max_num_victims_ << std::endl;
      return;
    }
  }
//  LOG(INFO) << "LRUReplacer::Pin() failed: " << "Frame " << frame_id << " not found." << std::endl;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (pinned_list_.find(frame_id) != pinned_list_.end()) {
    pinned_list_.erase(frame_id);
    victim_list_.push_back(frame_id);
    max_num_victims_++;
    num_victims_++;
//    LOG(INFO) << "LRUReplacer::Unpin() called. Target frame found in pinned list: " << "Unpinned frame: " << frame_id
//              << ", NumVictims: " << num_victims_ << ", MaxNumVictims: " << max_num_victims_ << std::endl;
    return;
  }
  for (std::deque<frame_id_t>::iterator i = victim_list_.begin(); i != victim_list_.end(); i++) {
    if (*i == frame_id) {
//      LOG(INFO) << "LRUReplacer::Unpin() called: " << "Frame " << frame_id << " is found in victim list." << std::endl;
      return;
    }
  }
  victim_list_.push_back(frame_id);
  num_victims_++;
//  LOG(INFO) << "LRUReplacer::Unpin() called. Target frame pushed into victim list: " << "Unpinned frame: " << frame_id
//            << ", NumVictims: " << num_victims_ << ", MaxNumVictims: " << max_num_victims_ << std::endl;
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return num_victims_;
}