#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>
#include <ctime>

#include "data_item.hpp"
#include "convert_ts.hpp"

using namespace std;
namespace py = pybind11;

namespace pybind11 { namespace detail {
    template <> struct type_caster<data_item> {
    public:
        /**
         * This macro establishes the name 'data_item' in
         * function signatures and declares a local variable
         * 'value' of type data_item
         */
        PYBIND11_TYPE_CASTER(data_item, _("data_item"));

        /**
         * Conversion part 1 (Python->C++): convert a PyObject into a data_item
         * instance or return false upon failure. The second argument
         * indicates whether implicit conversions should be applied.
         */
        bool load(handle src, bool) {
            /* Extract PyObject from handle */
            PyObject *source = src.ptr();
            if (!PyTuple_Check(source))
                return false;
            /* Now try to convert into data_item */
            value.ts = PyLong_AsLong(PyTuple_GetItem(source, 0));
            value.ts_offset = PyLong_AsLong(PyTuple_GetItem(source, 1));
            value.value = PyFloat_AsDouble(PyTuple_GetItem(source, 2));
            // Py_DECREF(source);
            /* Ensure return code was OK (to avoid out-of-range errors etc) */
            return !(value.ts == -1 && !PyErr_Occurred());
        }

        /**
         * Conversion part 2 (C++ -> Python): convert an data_item instance into
         * a Python object. The second and third arguments are used to
         * indicate the return value policy and parent object (for
         * ``return_value_policy::reference_internal``) and are generally
         * ignored by implicit casters.
         */
        static handle cast(data_item src, return_value_policy /* policy */, handle /* parent */) {
            return PyTuple_Pack(3, PyLong_FromLongLong(src.ts), PyLong_FromLong(src.ts_offset), PyFloat_FromDouble(src.value));
        }
    };
}}


typedef std::deque<data_item> data_deque;
typedef std::tuple<std::string, double> iso_item;
typedef std::tuple<int64_t, int32_t, double> c_data_item;


class timeseries {
    public:
        timeseries(const std::string &key, const std::string &metric) :
            key(key), metric(metric), _min_ts(0), _max_ts(0) { }

        void setKey(const std::string &key_) { key = key_; }

        const std::string &get_key() const { return key; }

        const std::string my_repr() const { return "<timeseries '" + key + "." + metric + "'>"; }

        const size_t my_len() const { return _data.size(); }

        bool remove_ts(const int64_t &ts) {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            int64_t ts_from_it = (*idx).ts;
            if (ts_from_it == ts) {
                _data.erase(idx);
                return true;
            }
            throw pybind11::key_error("timestamp: " + std::to_string(ts));
        }

        bool remove(const size_t &i) {
            _data.erase(_data.begin() + i);
            return true;
        }

        void trim_idx(const size_t &start_idx, const size_t &end_idx) {
            if (start_idx >= _data.size())
            {
                _data.clear();
                return;
            }
            if (start_idx >= 1 && start_idx < _data.size())
                _data.erase(_data.begin(), _data.begin() + start_idx);
            if (end_idx < (_data.size() - 1))
                _data.erase(_data.begin() + end_idx + 1, _data.end());
        }

        void trim_ts(const int64_t &start_ts, const int64_t &end_ts) {
            auto idx1 = bisect_left(start_ts);
            auto idx2 = bisect_right(end_ts);
            if (idx2 > 0) {
                trim_idx(idx1, idx2-1);
            } else {
                _data.clear();
            }
        }

        const iso_item iso_at(const size_t &i) const {
            data_item d = _data.at(i);
            auto arr = d.iso_format();
            std::string str(begin(arr), end(arr)-1);
            return std::make_tuple(str, d.value);
        }

        const py::bytes bytes_at(const size_t &i) const {
            data_item d = _data.at(i);
            auto arr = d.to_bytes();
            std::string str(begin(arr), end(arr));
            return str;
        }

        const c_data_item at(const size_t &i) const {
            data_item d = _data.at(i);
            return std::make_tuple(d.ts, d.ts_offset, d.value);
        }

        const c_data_item at_ts(const int64_t &ts) {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            int64_t ts_from_it = (*idx).ts;
            if (ts_from_it == ts) {
                data_item d = *idx;
                return std::make_tuple(d.ts, d.ts_offset, d.value);
            }
            throw pybind11::key_error("timestamp: " + std::to_string(ts));
        }

        // const data_item &at(const size_t &i) const {
        //     return _data.at(i);
        // }

        // const data_item &at_ts(const int64_t &ts) {
        //     auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
        //     int64_t ts_from_it = (*idx).ts;
        //     if (ts_from_it == ts) {
        //         return *idx;
        //     }
        //     throw pybind11::key_error("timestamp: " + std::to_string(ts));
        // }

        const size_t nearest_index_of_ts(const int64_t &ts) {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            if (idx == _data.begin()) {
                return idx - _data.begin();
            }
            if (idx == _data.end()) {
                return --idx - _data.begin();
            }
            int64_t t2 = (*idx).ts;
            int64_t t1 = (*--idx).ts;
            if (abs(ts - t1) <= abs(ts - t2))
                return idx - _data.begin();
            return ++idx - _data.begin();
        }

        const size_t index_of_ts(const int64_t &ts) {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            int64_t ts_from_it = (*idx).ts;
            if (ts_from_it == ts) {
                return idx - _data.begin();
            }
            throw pybind11::key_error("timestamp: " + std::to_string(ts));
        }

        static bool data_compare_left(const data_item& obj, int64_t ts) { return obj.ts < ts; }
        static bool data_compare_right(int64_t ts, const data_item& obj) { return ts < obj.ts; }

        const size_t bisect_left(const int64_t &ts) const {
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            return idx - _data.begin();
        }

        const size_t bisect_right(const int64_t &ts) const {
            auto idx = upper_bound(_data.begin(), _data.end(), ts, data_compare_right);
            return idx - _data.begin();
        }

        bool insert_iso(const std::string &iso_ts, const double &value) {
            auto time = fromIsoString(iso_ts);
            return insert(time.ts, time.ts_offset, value);
        }

        bool insert(const int64_t &ts, const int32_t &ts_offset, const double &value) {
            data_item d = {ts, ts_offset, value};
            // Empty
            if (_data.size() == 0) {
                _data.push_back(d);
                _max_ts = ts;
                _min_ts = ts;
                return true;
            }

            // Insert Back
            if (ts > _max_ts) {
                _data.push_back(d);
                _max_ts = ts;
                return true;
            }

            // Insert Front
            if (ts < _min_ts) {
                _data.push_front(d);
                _min_ts = ts;
                return true;
            }

            // Insert Mid
            auto idx = lower_bound(_data.begin(), _data.end(), ts, data_compare_left);
            data_item &item = _data.at(idx - _data.begin());
            // Replace
            if (item.ts == ts) {
                item.ts_offset = ts_offset;
                item.value = value;
                return false;
            }

            _data.insert(idx, d);
            return true;
        }

        const int64_t &get_min_ts() const { return _min_ts; }
        const int64_t &get_max_ts() const { return _max_ts; }

    public:
        std::string key;
        std::string metric;

    private:
        data_deque _data;
        int64_t _min_ts;
        int64_t _max_ts;
};



PYBIND11_MODULE(cdb_ext_ts, m) {
    m.doc() = "CattleDB TS C Extensions";
    py::class_<timeseries>(m, "timeseries")
        .def(py::init<const std::string &, const std::string &>())
        .def_readwrite("key", &timeseries::key)
        .def_readwrite("metric", &timeseries::metric)
        .def("insert", &timeseries::insert)
        .def("insert_iso", &timeseries::insert_iso)
        .def("at", &timeseries::at)
        .def("at_ts", &timeseries::at_ts)
        .def("index_of_ts", &timeseries::index_of_ts)
        .def("nearest_index_of_ts", &timeseries::nearest_index_of_ts)
        .def("iso_at", &timeseries::iso_at)
        .def("bytes_at", &timeseries::bytes_at)
        .def("bisect_left", &timeseries::bisect_left)
        .def("bisect_right", &timeseries::bisect_right)
        .def("trim_idx", &timeseries::trim_idx)
        .def("trim_ts", &timeseries::trim_ts)
        .def("get_min_ts", &timeseries::get_min_ts)
        .def("get_max_ts", &timeseries::get_max_ts)
        .def("remove_ts", &timeseries::remove_ts)
        .def("remove", &timeseries::remove)
        .def("__len__", &timeseries::my_len)
        .def("__repr__", &timeseries::my_repr);

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "dev";
#endif
}
