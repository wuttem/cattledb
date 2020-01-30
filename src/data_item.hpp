#pragma once

#include <cstdint>
#include <array>
#include <ctime>
#include <cstdio>
#include <cstring>


struct data_item {
  int64_t ts;
  int32_t ts_offset;
  double value;

  const std::array<char, sizeof("2019-08-01T09:41:01+00:00")> iso_format() const {
      char buf[sizeof("2019-08-01T09:41:01")];
      char add[sizeof("+00:00")];
      time_t timeGMT = (time_t) (ts + ts_offset);
      int32_t hours = (int32_t) (ts_offset / 3600) % 24;
      uint32_t minutes = (uint32_t) ((ts_offset / 60) % 60);
      std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", std::gmtime(&timeGMT));
      if(hours < 0)
      {
        std::sprintf(add, "%03d:%02d", hours, minutes);
      }
      else
      {
        std::sprintf(add, "+%02d:%02d", hours, minutes);
      }
      std::array<char, sizeof("2019-08-01T09:41:01+00:00")> arr;
      std::memcpy(&arr[0], buf, sizeof(buf));
      std::memcpy(&arr[sizeof(buf)-1], add, sizeof(add));

      return arr;
  }

  const std::array<char, sizeof(int64_t) + sizeof(int32_t) + sizeof(double)> to_bytes() const {
      std::array<char, sizeof(int64_t) + sizeof(int32_t) + sizeof(double)> arr;

      std::memcpy(&arr[0], &ts, sizeof(ts));
      std::memcpy(&arr[sizeof(ts)], &ts_offset, sizeof(ts_offset));
      std::memcpy(&arr[sizeof(ts) + sizeof(ts_offset)], &value, sizeof(value));

      return arr;
  }
};
