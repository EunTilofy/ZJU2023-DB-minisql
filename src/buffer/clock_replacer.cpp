#include "buffer/clock_replacer.h"
#include <iostream>
#include "glog/logging.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages): capacity(num_pages) {
  for (int i = 0; i < num_pages; ++i) {
    clock_list.emplace_back(INVALID_FRAME_ID);
  }
  clock_pointer = clock_list.begin();
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  for (size_t i = 0; i < capacity * 2; ++i) {
    if (*clock_pointer - INVALID_FRAME_ID) {
      if (clock_status[*clock_pointer]) {
        if (clock_status[*clock_pointer] == 1)
          clock_status[*clock_pointer] = 0;
      } else {
        *frame_id = *clock_pointer;
        clock_status.erase(*clock_pointer);
        *clock_pointer = INVALID_FRAME_ID;
        LOG(INFO) << "CLOCKReplacer::Victim() succeeded: Frame " << *frame_id << " victimized." << std::endl;
        return true;
      }
    }
    clock_pointer++;
    if (clock_pointer == clock_list.end())
      clock_pointer = clock_list.begin();
  }
  *frame_id = INVALID_FRAME_ID;
  LOG(INFO) << "CLOCKReplacer::Victim() failed: No frame can be victimized!" << std::endl;
  return false;
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if (clock_status.count(frame_id)) {
    clock_status[frame_id] = 2;
    LOG(INFO) << "CLOCKReplacer::Pin() succeeded: Frame " << frame_id << " is pinned." << std::endl;
  }
  else
    LOG(INFO) << "CLOCKReplacer::Pin() failed: Frame " << frame_id << " is not found." << std::endl;
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if (clock_status.count(frame_id)) {
    clock_status[frame_id] = 1;
    LOG(INFO) << "CLOCKReplacer::Unpin() succeeded: Frame " << frame_id << "is set to unpinned." << std::endl;
  }
  else {
    for (size_t i = 0; i < capacity; ++i) {
      if (*clock_pointer == INVALID_FRAME_ID)
        break;
      clock_pointer++;
      if (clock_pointer == clock_list.end())
        clock_pointer = clock_list.begin();
      if (i == capacity - 1) {
        LOG(INFO) << "CLOCKReplacer::Unpin() failed: No frame available!" << std::endl;
        return ;
      }
    }
    if (*clock_pointer == INVALID_FRAME_ID){
      *clock_pointer = frame_id;
      clock_status[frame_id] = 1;
    }
    else {
      LOG(INFO) << "CLOCKReplacer::Unpin() failed: No frame available!" << std::endl;
    }
    clock_pointer++;
    if (clock_pointer == clock_list.end())
      clock_pointer = clock_list.begin();
    LOG(INFO) << "CLOCKReplacer::Unpin() succeeded: " << "Frame " << frame_id << " is pushed into the buffer." << std::endl;
  }
}

size_t CLOCKReplacer::Size() {
  size_t size = 0;
  for (auto i : clock_list) {
    if (i == INVALID_FRAME_ID) continue;
    if (clock_status[i] == 2) continue;
    size++;
  }
  return size;
}