#pragma once

#include <iostream>
#include <list>
#include <seastar/core/temporary_buffer.hh>
#include <unordered_map>

namespace blobstore {

template <class Key, class Value>
class LRUCache {
   public:
    typedef Key key_type;
    typedef Value value_type;
    typedef std::list<key_type> list_type;
    typedef std::unordered_map<key_type, std::pair<value_type, typename list_type::iterator>>
        map_type;

    LRUCache(size_t capacity) : capacity_(capacity) {}

    ~LRUCache() {}

    size_t Size() const { return map_.size(); }

    size_t Capacity() const { return capacity_; }

    bool Empty() const { return map_.empty(); }

    bool Contains(const key_type &key) { return map_.find(key) != map_.end(); }

    void Insert(const key_type &key, value_type &value) {
        typename map_type::iterator i = map_.find(key);
        if (i == map_.end()) {
            // insert item into the cache, but first check if it is full
            if (Size() >= capacity_) {
                // cache is full, evict the least recently used item
                evict();
            }
        } else {
            list_.erase(i->second.second);
        }
        // insert the new item
        list_.push_front(key);
        map_[key] = std::make_pair(value, list_.begin());
    }

    std::optional<value_type> Get(const key_type &key) {
        // lookup value in the cache
        typename map_type::iterator i = map_.find(key);
        if (i == map_.end()) {
            // value not in cache
            return std::optional<value_type>();
        }

        // return the value, but first update its place in the most
        // recently used list
        typename list_type::iterator j = i->second.second;
        if (j != list_.begin()) {
            // move item to the front of the most recently used list
            list_.erase(j);
            list_.push_front(key);

            // update iterator in map
            j = list_.begin();
            const value_type &value = i->second.first;
            map_[key] = std::make_pair(value, j);

            // return the value
            return value;
        } else {
            // the item is already at the front of the most recently
            // used list so just return it
            return i->second.first;
        }
    }

    void Clear() {
        map_.clear();
        list_.clear();
    }

    void Erase(const key_type &key) {
        typename map_type::iterator i = map_.find(key);
        if (i == map_.end()) {
            return;
        }
        list_.erase(i->second.second);
        map_.erase(i);
    }

   private:
    void evict() {
        // evict item from the end of most recently used list
        typename list_type::iterator i = --list_.end();
        map_.erase(*i);
        list_.erase(i);
    }

   private:
    map_type map_;
    list_type list_;
    size_t capacity_;
};

template <class Key>
class LRUCache<Key, seastar::temporary_buffer<char>> {
   public:
    typedef Key key_type;
    typedef seastar::temporary_buffer<char> value_type;
    typedef std::list<key_type> list_type;
    typedef std::unordered_map<key_type, std::pair<value_type, typename list_type::iterator>>
        map_type;

    LRUCache(size_t capacity) : capacity_(capacity) {}

    ~LRUCache() {}

    size_t Size() const { return map_.size(); }

    size_t Capacity() const { return capacity_; }

    bool Empty() const { return map_.empty(); }

    bool Contains(const key_type &key) { return map_.find(key) != map_.end(); }

    void Insert(const key_type &key, value_type &value) {
        typename map_type::iterator i = map_.find(key);
        if (i == map_.end()) {
            // insert item into the cache, but first check if it is full
            if (Size() >= capacity_) {
                // cache is full, evict the least recently used item
                evict();
            }
        } else {
            list_.erase(i->second.second);
        }
        // insert the new item
        list_.push_front(key);
        map_[key] = std::move(std::make_pair(value.share(), list_.begin()));
    }

    std::optional<value_type> Get(const key_type &key) {
        // lookup value in the cache
        typename map_type::iterator it = map_.find(key);
        if (it == map_.end()) {
            // value not in cache
            return std::optional<value_type>();
        }

        // return the value, but first update its place in the most
        // recently used list
        typename list_type::iterator j = it->second.second;
        if (j != list_.begin()) {
            // move item to the front of the most recently used list
            list_.erase(j);
            list_.push_front(key);

            // update iterator in map
            j = list_.begin();
            value_type value = std::move(it->second.first.share());
            map_[key] = std::move(std::make_pair(value.share(), j));
            // return the value
            return std::move(value);
        } else {
            // the item is already at the front of the most recently
            // used list so just return it
            return std::move(it->second.first.share());
        }
    }

    void Clear() {
        map_.clear();
        list_.clear();
    }

    void Erase(const key_type &key) {
        typename map_type::iterator i = map_.find(key);
        if (i == map_.end()) {
            return;
        }
        list_.erase(i->second.second);
        map_.erase(i);
    }

   private:
    void evict() {
        // evict item from the end of most recently used list
        typename list_type::iterator i = --list_.end();
        map_.erase(*i);
        list_.erase(i);
    }

   private:
    map_type map_;
    list_type list_;
    size_t capacity_;
};

}  // namespace blobstore
