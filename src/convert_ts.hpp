#pragma once

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <ctime>


#ifndef HAVE_TIMEGM
// timegm is a GNU extension
static time_t
timegm(struct tm *p)
{
    return p->tm_sec + p->tm_min*60 + p->tm_hour*3600 + p->tm_yday*86400 +
        (p->tm_year-70)*31536000 + ((p->tm_year-69)/4)*86400 -
        ((p->tm_year-1)/100)*86400 + ((p->tm_year+299)/400)*86400;
}
#endif


struct TimeT
{
  time_t ts;
  int32_t ts_offset;
};


struct FullTimeTuple
{
    int year;
    int month;
    int day;
    int hour;
    int minute;
    int second;
};


int daysToMonth[2][12] =
{
    { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 },
    { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335 },
};


static time_t tuple_to_time_t(const FullTimeTuple &tt) {
    bool leap = false;
    // check leap year
    if (tt.year % 4 == 0 && (tt.year % 100 != 0 || tt.year % 400 == 0)) {
        leap = true;
    }

    int day_in_year = daysToMonth[leap ? 1 : 0][tt.month-1] + (tt.day-1);
    int unix_year = tt.year - 1900;

    return tt.second + tt.minute*60 + tt.hour*3600 + day_in_year*86400 +
           (unix_year-70)*31536000 + ((unix_year-69)/4)*86400 -
           ((unix_year-1)/100)*86400 + ((unix_year+299)/400)*86400;
}



inline TimeT fromIsoString(const std::string &iso_ts)
{
    int32_t ts_offset = 0;
    FullTimeTuple tt;
    float s;
    int tzh = 0;
    int tzm = 0;

    int parsed_cnt = sscanf(iso_ts.c_str(), "%d-%d-%dT%d:%d:%f%d:%dZ", &tt.year,
        &tt.month, &tt.day, &tt.hour, &tt.minute, &s, &tzh, &tzm);
    tt.second = (int) s;
    if (parsed_cnt > 6) {
        if (tzh < 0) {
            tzm = -tzm;
        }
        ts_offset = (tzm * 60) + (tzh * 3600);
    }

    auto ts = tuple_to_time_t(tt) - ts_offset;
    return {ts, ts_offset};
}
