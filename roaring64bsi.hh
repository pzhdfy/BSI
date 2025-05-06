// Roaring64BitmapSliceIndex
//来源：https://github.com/goldenbean/BSI/blob/main/roaring64bsi.hh

#ifndef INCLUDE_ROARING_64_BITMAP_SLICE_INDEX_HH_
#define INCLUDE_ROARING_64_BITMAP_SLICE_INDEX_HH_

#include <fmt/format.h>

#include <bit>
#include <iostream>
#include <sstream>
#include <memory>
#include <optional>
#include "roaring.hh"
#include <tuple>
#include <vector>

namespace roaring {

class Roaring64Bsi;
using Roaring64MapPtr = std::unique_ptr<Roaring64Map>;
using Roaring64BsiPtr = std::unique_ptr<Roaring64Bsi>;

enum BsiOperation {
  EQ = 0,   // equal
  NEQ = 1,  // not equal
  LE = 2,   // less than or equal
  LT = 3,   // less than
  GE = 4,   // greater than or equal
  GT = 5,   // greater than
  RANGE = 6, // range
  UNKNOWN = 100
};

[[nodiscard]] static std::string toUpperCase(std::string_view sv) noexcept {
  std::string result(sv);
  std::transform(result.begin(), result.end(), result.begin(), ::toupper);
  return result;
}

[[nodiscard]] static BsiOperation toBsiOperation(std::string_view input) {
  std::string sv = toUpperCase(input);
  if (sv == "EQ") return BsiOperation::EQ;
  if (sv == "NEQ") return BsiOperation::NEQ;
  if (sv == "LE") return BsiOperation::LE;
  if (sv == "LT") return BsiOperation::LT;
  if (sv == "GE") return BsiOperation::GE;
  if (sv == "GT") return BsiOperation::GT;
  if (sv == "RANGE") return BsiOperation::RANGE;

  return BsiOperation::UNKNOWN;
}

constexpr size_t maxBitDepth{64};

class Roaring64Bsi {
 public:
  Roaring64Bsi(uint64_t minValue = 0, uint64_t maxValue = 1)
      : maxValue_{maxValue}, minValue_{minValue}, runOptimized_{true} {
    size_t bitDepth = getBitDepth(maxValue);
    indexBitMapVec_.resize(bitDepth);
  }

  Roaring64Bsi(const Roaring64Bsi& other)
      : maxValue_{other.maxValue_},
        minValue_{other.minValue_},
        runOptimized_{other.runOptimized_},
        existenceBitMap_{other.existenceBitMap_} {
    indexBitMapVec_.reserve(other.indexBitMapVec_.size());
    for (auto const& e : other.indexBitMapVec_) {
      indexBitMapVec_.emplace_back(e);
    }
  }

  // Roaring64Bsi& operator=(const Roaring64Bsi& other) = delete;

  auto operator=(const Roaring64Bsi& other) -> Roaring64Bsi& {
    if (this != &other) {
      this->existenceBitMap_ = other.existenceBitMap_;

      this->indexBitMapVec_.clear();
      this->indexBitMapVec_.reserve(other.indexBitMapVec_.size());
      for (auto const& e : other.indexBitMapVec_) {
        indexBitMapVec_.emplace_back(e);
      }

      this->maxValue_ = other.maxValue_;
      this->minValue_ = other.minValue_;
      this->runOptimized_ = other.runOptimized_;
    }

    return *this;
  }

  Roaring64Bsi(Roaring64Bsi&& other) noexcept {
    if (this != &other) {
      this->existenceBitMap_ = std::move(other.existenceBitMap_);

      this->indexBitMapVec_.clear();
      this->indexBitMapVec_.reserve(other.indexBitMapVec_.size());
      for (auto& e : other.indexBitMapVec_) {
        indexBitMapVec_.emplace_back(std::move(e));
      }
      other.clear();

      this->maxValue_ = other.maxValue_;
      this->minValue_ = other.minValue_;
      this->runOptimized_ = other.runOptimized_;
    }
  }

  // Roaring64Bsi& operator=(Roaring64Bsi&& other) noexcept = delete;

  auto operator=(Roaring64Bsi&& other) noexcept -> Roaring64Bsi& {
    if (this != &other) {
      this->existenceBitMap_ = std::move(other.existenceBitMap_);

      this->indexBitMapVec_.clear();
      this->indexBitMapVec_.reserve(other.indexBitMapVec_.size());
      for (auto& e : other.indexBitMapVec_) {
        indexBitMapVec_.emplace_back(std::move(e));
      }
      other.clear();

      this->maxValue_ = other.maxValue_;
      this->minValue_ = other.minValue_;
      this->runOptimized_ = other.runOptimized_;
    }

    return *this;
  }

  ~Roaring64Bsi() {}

  /**
   * bsi_show: 将BSI按键值展开，并展示前integer个键值对。
   */
  auto show() -> std::string {
    std::stringstream ss;

    for (uint64_t columnId : existenceBitMap_) {
      auto [value, exists] = getValue(columnId);
      ss << fmt::format(" [{},{}] \n", columnId, value);
    }

    return ss.str();
  }

  auto toString() const -> std::string {
    return fmt::format(
        "Roaring64Bsi: minValue {}, maxValue {}, runOptimized {}, bit depth {}, cardinality {}",
        minValue_, maxValue_, runOptimized_, indexBitMapVec_.size(),
        existenceBitMap_.cardinality());
  }

  void setValue(uint64_t columnId, uint64_t value) {
    ensureCapacityInternal(value, value);
    setValueInternal(columnId, value);
  }

  void setValues(const std::vector<std::tuple<uint64_t, uint64_t>>& vec) {
    uint64_t maxValue = 0;
    uint64_t minValue = 0;
    for (const auto& [columnId, value] : vec) {
      minValue = std::min(minValue, value);
      maxValue = std::max(maxValue, value);
    }

    ensureCapacityInternal(minValue, maxValue);

    for (const auto& [columnId, value] : vec) {
      setValueInternal(columnId, value);
    }
  }

  [[nodiscard]] auto getValue(uint64_t columnId) const noexcept -> std::tuple<uint64_t, bool> {
    if (!valueExist(columnId)) {
      return std::make_tuple(0, false);
    }

    return std::make_tuple(valueAt(columnId), true);
  }

  /**
   * bsi_add: 将两个BSI相同ebm对应的value相加，返回新的BSI。
   */
  void add(const Roaring64Bsi& otherBsi) {
    if (otherBsi.existenceBitMap_.isEmpty()) {
      return;
    }

    existenceBitMap_ |= otherBsi.existenceBitMap_;

    if (otherBsi.bitCount() > bitCount()) {
      grow(otherBsi.bitCount());
    }

    for (size_t i = 0; i < otherBsi.bitCount(); i++) {
      addDigit(otherBsi.indexBitMapVec_[i], i);
    }

    // update min and max after adding
    minValue_ = minValue();
    maxValue_ = maxValue();
  }

  /**
   * bsi_merge: 将两个BSI合并，要求两个BSI的ebm没有交集。
   */
  void merge(const Roaring64Bsi& otherBsi) {
    if (otherBsi.existenceBitMap_.isEmpty()) {
      return;
    }

    size_t bitDepth = std::max(indexBitMapVec_.size(), otherBsi.indexBitMapVec_.size());

    indexBitMapVec_.resize(bitDepth);

    for (size_t i = 0; i < bitDepth; i++) {
      if (i < otherBsi.indexBitMapVec_.size()) {
        indexBitMapVec_[i] |= otherBsi.indexBitMapVec_[i];
      }
      if (runOptimized_ || otherBsi.runOptimized_) {
        indexBitMapVec_[i].runOptimize();
      }
    }

    existenceBitMap_ |= otherBsi.existenceBitMap_;
    runOptimized_ = runOptimized_ || otherBsi.runOptimized_;
    maxValue_ = std::max(maxValue_, otherBsi.maxValue_);
    minValue_ = std::min(minValue_, otherBsi.minValue_);
  }

  /**
   * bsi_sum: 返回BSI value之和sum以及ebm基数cardinality组成的数组。
   * 如果有第二入参roaringbitmap为非空，则先查询BSI的ebm和bytea的交集部分，再计算sum与基数。
   */
  [[nodiscard]] auto sum(const std::optional<Roaring64Map>& foundSet = std::nullopt) const
      -> std::tuple<uint64_t, uint64_t> {
    if (foundSet.has_value()) [[likely]] {
      return sumInternal(*foundSet);
    }

    return sumInternal(this->existenceBitMap_);
  }

  /**
   * bsi_filter: 查询BSI的ebm和指定 foundSet 的交集部分，返回新的BSI。
   */
  [[nodiscard]] auto filter(const Roaring64MapPtr& foundSetPtr) const -> Roaring64BsiPtr {
    auto newBsiPtr = clone();

    if (foundSetPtr == nullptr || (*foundSetPtr).isEmpty()) {
      return newBsiPtr;
    }

    newBsiPtr->existenceBitMap_ &= *foundSetPtr;
    for (size_t i = 0; i < newBsiPtr->bitCount(); i++) {
      newBsiPtr->indexBitMapVec_[i] &= *foundSetPtr;
    }

    return newBsiPtr;
  }

  /**
   * bsi_exclude: 查询BSI的ebm中剔除指定用户 foundSet，返回新的BSI。
   */
  [[nodiscard]] auto exclude(const Roaring64MapPtr& foundSetPtr) const -> Roaring64BsiPtr {
    auto newBsiPtr = clone();
    if (foundSetPtr == nullptr || (*foundSetPtr).isEmpty()) {
      return newBsiPtr;
    }

    newBsiPtr->existenceBitMap_ -= *foundSetPtr;
    for (size_t i = 0; i < newBsiPtr->bitCount(); i++) {
      newBsiPtr->indexBitMapVec_[i] -= *foundSetPtr;
    }

    return newBsiPtr;
  }

  /**
   * bsi_ebm: 查询BSI的ebm数组的roaringbitmap。
   */
  [[nodiscard]] auto getExistenceBitmap() const -> Roaring64Map { return existenceBitMap_; }

  [[nodiscard]] auto valueExist(uint64_t columnId) const noexcept -> bool {
    return existenceBitMap_.contains(columnId);
  }

  [[nodiscard]] auto bitCount() const -> size_t { return indexBitMapVec_.size(); }

  /**
   * 对BSI进行比较过滤查询。支持LT/LE/GT/GE/EQ/NEQ/RANGE。
   */
  [[nodiscard]] auto compare(BsiOperation operation, uint64_t startOrValue, uint64_t end,
                             const std::optional<Roaring64Map>& foundSet = std::nullopt) const
      -> std::optional<Roaring64Map> {
    auto compareResult = compareUsingMinMax(operation, startOrValue, end, foundSet);

    if (compareResult) {
      return compareResult;
    }

    switch (operation) {
      case EQ:
        return oNeilCompare(operation, startOrValue, foundSet);
      case NEQ:
        return oNeilCompare(operation, startOrValue, foundSet);
      case GE:
        return oNeilCompare(operation, startOrValue, foundSet);
      case GT:
        return oNeilCompare(operation, startOrValue, foundSet);
      case LT:
        return oNeilCompare(operation, startOrValue, foundSet);
      case LE:
        return oNeilCompare(operation, startOrValue, foundSet);
      case RANGE: {
        if (startOrValue < minValue_) {
          startOrValue = minValue_;
        }

        if (end > maxValue_) {
          end = maxValue_;
        }

        auto left = oNeilCompare(BsiOperation::GE, startOrValue, foundSet);
        auto right = oNeilCompare(BsiOperation::LE, end, foundSet);

        if (left && right) {
          return *left & *right;
        }
        return std::nullopt;
      }
      default:
        return std::nullopt;
    }

    return std::nullopt;
  }

  /**
   * bsi_topk: 返回BSI top k个最大value对应的ebm组成的roaringbitmap。
   * 如果有第二入参roaringbitmap类型序列化的bytea，则先查询BSI的ebm和bytea的交集部分，再计算top
   * K。
   */
  [[nodiscard]] auto topK(uint64_t k,
                          const std::optional<Roaring64Map>& foundSet = std::nullopt) const
      -> Roaring64Map {
    Roaring64Map candidates = foundSet.has_value() ? *foundSet : getExistenceBitmap();

    if (k >= candidates.cardinality()) {
      return candidates;
    }

    Roaring64Map retBitmap;

    for (size_t x = bitCount() - 1; !candidates.isEmpty() && k > 0; x--) {
      Roaring64Map andBitmap = candidates;
      andBitmap &= indexBitMapVec_[x];
      uint64_t cardinality = andBitmap.cardinality();

      if (cardinality > k) {
        candidates &= indexBitMapVec_[x];
      } else {
        retBitmap |= andBitmap;
        candidates -= indexBitMapVec_[x];
        k -= cardinality;
      }
    }
    return retBitmap;
  }

  /**
   * bsi_transpose: 返回BSI的value转置结果，即去重后组成的roaringbitmap。
   * 如果有第二入参roaringbitmap类型序列化的bytea，则先查询BSI的ebm和bytea的交集部分，再计算转置。
   */
  [[nodiscard]] auto transpose(const std::optional<Roaring64Map>& foundSet = std::nullopt) const
      -> Roaring64Map {
    Roaring64Map fixedFoundSet{existenceBitMap_};

    if (foundSet.has_value()) {
      fixedFoundSet &= foundSet.value();
    }

    Roaring64Map retBitmap;

    std::for_each(fixedFoundSet.begin(), fixedFoundSet.end(), [&retBitmap, this](auto const it) {
      auto [value, exists] = getValue(it);

      retBitmap.add(value);
    });

    return retBitmap;
  }

  /**
   * bsi_transpose_with_count: 将BSI的value转置并统计次数，返回新的BSI。
   * 如果有第二入参roaringbitmap类型序列化的bytea，则先查询BSI的ebm和bytea的交集部分，再计算转置并统计。
   */
  [[nodiscard]] auto transposeWithCount(
      const std::optional<Roaring64Map>& foundSet = std::nullopt) const -> Roaring64Bsi {
    Roaring64Map fixedFoundSet{existenceBitMap_};

    if (foundSet.has_value()) {
      fixedFoundSet &= foundSet.value();
    }
    Roaring64Bsi retBsi;

    std::for_each(fixedFoundSet.begin(), fixedFoundSet.end(), [&retBsi, this](auto const it) {
      auto [value, exists] = getValue(it);

      if (retBsi.valueExist(value)) {
        auto [reValue, reExists] = retBsi.getValue(value);
        retBsi.setValue(value, reValue + 1);
      } else {
        retBsi.setValue(value, 1);
      }
    });

    return retBsi;
  }

  auto serializeBuffer(std::unique_ptr<char[]>& buffer) const -> size_t {
    uint64_t serialize_size = this->serializedSizeInBytes();
    buffer.reset(new char[serialize_size]);
    return serialize((char*)buffer.get());
  }

  auto serializedSizeInBytes() const -> uint64_t {
    uint64_t size = 0;
    for (const auto& rb : indexBitMapVec_) {
      size += rb.getSizeInBytes();
    }

    return 8 + 8 + 1 + existenceBitMap_.getSizeInBytes() + 4 + size;
  }

  auto serialize(char* buf) const -> size_t {
    const char* orig = buf;
    uint8_t opt = runOptimized_ ? 1 : 0;
    std::memcpy(buf, &minValue_, sizeof(uint64_t));
    buf += sizeof(uint64_t);
    std::memcpy(buf, &maxValue_, sizeof(uint64_t));
    buf += sizeof(uint64_t);
    std::memcpy(buf, &opt, sizeof(uint8_t));
    buf += sizeof(uint8_t);

    // write ebM
    auto ebMSize = existenceBitMap_.getSizeInBytes();
    existenceBitMap_.write(buf);
    buf += ebMSize;

    // write bitDepth
    uint32_t bASize = indexBitMapVec_.size();
    std::memcpy(buf, &bASize, sizeof(uint32_t));
    buf += sizeof(uint32_t);

    // write bA
    for (const auto& rb : indexBitMapVec_) {
      auto rbSize = rb.getSizeInBytes();
      rb.write(buf);
      buf += rbSize;
    }

    return buf - orig;
  }

  void deserialize(char* buf) {
    clear();

    // read meta
    uint8_t opt{0};

    std::memcpy(&minValue_, buf, sizeof(uint64_t));
    buf += sizeof(uint64_t);
    std::memcpy(&maxValue_, buf, sizeof(uint64_t));
    buf += sizeof(uint64_t);
    std::memcpy(&opt, buf, sizeof(uint8_t));
    buf += sizeof(uint8_t);

    if (opt == 1) {
      runOptimized_ = true;
    }

    // read ebM
    existenceBitMap_ = std::move(Roaring64Map::read(buf));
    buf += existenceBitMap_.getSizeInBytes();

    // read bitDepth
    uint32_t bitDepth = 0;
    std::memcpy(&bitDepth, buf, sizeof(uint32_t));
    buf += sizeof(uint32_t);

    // read bA
    indexBitMapVec_.resize(bitDepth);
    for (size_t i = 0; i < bitDepth; i++) {
      indexBitMapVec_[i] = std::move(Roaring64Map::read(buf));
      size_t baSize = indexBitMapVec_[i].getSizeInBytes();
      buf += baSize;
    }
  }

  void runOptimize() {
    existenceBitMap_.runOptimize();

    for (auto& bmPtr : indexBitMapVec_) {
      bmPtr.runOptimize();
    }
    runOptimized_ = true;
  }

  auto hasRunCompression() const -> bool const { return runOptimized_; }

  [[nodiscard]] auto clone() const -> Roaring64BsiPtr {
    return std::make_unique<Roaring64Bsi>(*this);
  }

 private:
  void clear() {
    existenceBitMap_.clear();
    indexBitMapVec_.clear();

    minValue_ = 0;
    maxValue_ = 1;
    runOptimized_ = false;
  }

  void ensureCapacityInternal(uint64_t minValue, uint64_t maxValue) {
    if (existenceBitMap_.isEmpty()) {
      minValue_ = minValue;
      maxValue_ = maxValue;
      grow(getBitDepth(maxValue));
    } else if (minValue_ > minValue) {
      minValue_ = minValue;
    } else if (maxValue_ < maxValue) {
      maxValue_ = maxValue;
      grow(getBitDepth(maxValue));
    }
  }

  void setValueInternal(uint64_t columnId, uint64_t value) {
    for (size_t i = 0; i < bitCount(); i++) {
      if ((value & (1L << i)) > 0) {
        indexBitMapVec_[i].add(columnId);
      } else {
        indexBitMapVec_[i].remove(columnId);
      }
    }
    existenceBitMap_.add(columnId);
  }

  void grow(size_t newBitDepth) {
    size_t oldBitDepth = indexBitMapVec_.size();

    if (oldBitDepth >= newBitDepth) {
      return;
    }
    indexBitMapVec_.resize(newBitDepth);
    for (size_t i = newBitDepth - 1; i >= oldBitDepth; i--) {
      indexBitMapVec_[i] = Roaring64Map();
      if (runOptimized_) {
        indexBitMapVec_[i].runOptimize();
      }
    }
  }

  [[nodiscard]] auto minValue() const -> uint64_t {
    if (existenceBitMap_.isEmpty()) {
      return 0;
    }

    auto minValuesId = existenceBitMap_;
    for (int i = indexBitMapVec_.size() - 1; i >= 0; i -= 1) {
      auto tmp = minValuesId - indexBitMapVec_[i];
      if (!tmp.isEmpty()) {
        minValuesId = tmp;
      }
    }

    return valueAt(minValuesId.minimum());
  }

  [[nodiscard]] auto maxValue() const -> uint64_t {
    if (existenceBitMap_.isEmpty()) {
      return 0;
    }

    auto maxValuesId = existenceBitMap_;
    for (int i = indexBitMapVec_.size() - 1; i >= 0; i -= 1) {
      auto tmp = maxValuesId & indexBitMapVec_[i];
      if (!tmp.isEmpty()) {
        maxValuesId = tmp;
      }
    }

    return valueAt(maxValuesId.minimum());
  }

  [[nodiscard]] auto valueAt(uint64_t columnId) const -> uint64_t {
    uint64_t value = 0;
    for (size_t i = 0; i < bitCount(); i += 1) {
      if (indexBitMapVec_[i].contains(columnId)) {
        value |= (1L << i);
      }
    }

    return value;
  }

  void addDigit(const Roaring64Map& foundSet, size_t i) {
    Roaring64Map carry = indexBitMapVec_[i] & foundSet;
    indexBitMapVec_[i] ^= foundSet;
    if (!carry.isEmpty()) {
      if (i + 1 >= bitCount()) {
        grow(bitCount() + 1);
      }
      addDigit(carry, i + 1);
    }
  }

  [[nodiscard]] auto sumInternal(const Roaring64Map& foundSet) const
      -> std::tuple<uint64_t, uint64_t> {
    uint64_t count = foundSet.cardinality();
    uint64_t sum = 0;
    for (size_t i = 0; i < bitCount(); i++) {
      sum += (1L << i) * (indexBitMapVec_[i] & foundSet).cardinality();
    }
    return std::make_tuple(sum, count);
  }

  [[nodiscard]] auto compareUsingMinMax(BsiOperation operation, uint64_t startOrValue, uint64_t end,
                                        const std::optional<Roaring64Map>& foundSet =
                                            std::nullopt) const -> std::optional<Roaring64Map> {
    auto allBitmap = foundSet.has_value() ? existenceBitMap_ & foundSet.value() : existenceBitMap_;

    Roaring64Map emptyBitmap;

    switch (operation) {
      case LT:
        if (startOrValue > maxValue_) {
          return allBitmap;
        } else if (startOrValue <= minValue_) {
          return emptyBitmap;
        }
        break;
      case LE:
        if (startOrValue >= maxValue_) {
          return allBitmap;
        } else if (startOrValue < minValue_) {
          return emptyBitmap;
        }
        break;
      case GT:
        if (startOrValue < minValue_) {
          return allBitmap;
        } else if (startOrValue >= maxValue_) {
          return emptyBitmap;
        }
        break;
      case GE:
        if (startOrValue <= minValue_) {
          return allBitmap;
        } else if (startOrValue > maxValue_) {
          return emptyBitmap;
        }
        break;
      case EQ:
        if (minValue_ == maxValue_ && minValue_ == startOrValue) {
          return allBitmap;
        } else if (startOrValue < minValue_ || startOrValue > maxValue_) {
          return emptyBitmap;
        }
        break;
      case NEQ:
        if (minValue_ == maxValue_) {
          if (minValue_ == startOrValue) {
            return emptyBitmap;
          }
          return allBitmap;
        }
        break;
      case RANGE:
        if (startOrValue <= minValue_ && end >= maxValue_) {
          return allBitmap;
        } else if (startOrValue > maxValue_ || end < minValue_) {
          return emptyBitmap;
        }
        break;
      default:
        return std::nullopt;
    }

    return std::nullopt;
  }

  [[nodiscard]] auto oNeilCompare(BsiOperation operation, uint64_t predicate,
                                  const std::optional<Roaring64Map>& foundSet = std::nullopt) const
      -> std::optional<Roaring64Map> {
    auto fixedFoundSet = foundSet.has_value() ? foundSet.value() : existenceBitMap_;

    Roaring64Map gtBitMap;
    Roaring64Map ltBitMap;
    Roaring64Map eqBitMap = existenceBitMap_;

    for (int32_t i = bitCount() - 1; i >= 0; i--) {
      auto bit = (int32_t)((predicate >> i) & 1);
      if (bit == 1) {
        ltBitMap |= (eqBitMap - indexBitMapVec_[i]);
        eqBitMap &= indexBitMapVec_[i];
      } else {
        gtBitMap |= (eqBitMap & indexBitMapVec_[i]);
        eqBitMap -= indexBitMapVec_[i];
      }
    }

    eqBitMap &= fixedFoundSet;
    switch (operation) {
      case EQ:
        return eqBitMap;
      case NEQ:
        return fixedFoundSet - eqBitMap;
      case GT:
        return gtBitMap & fixedFoundSet;
      case LT:
        return ltBitMap & fixedFoundSet;
      case LE:
        return (ltBitMap | eqBitMap) & fixedFoundSet;
      case GE:
        return (gtBitMap | eqBitMap) & fixedFoundSet;
      default:
        return std::nullopt;
    }
    return std::nullopt;
  }

  static auto leadingZeroes(uint64_t value) -> size_t {
    return (value < 1) ? 1 : __builtin_clzll(value);
  }

  static auto getBitDepth(uint64_t value) -> size_t { return maxBitDepth - leadingZeroes(value); }

 private:
  uint64_t maxValue_{1};
  uint64_t minValue_{0};
  bool runOptimized_{true};

  std::vector<Roaring64Map> indexBitMapVec_;
  Roaring64Map existenceBitMap_;
};

}  // namespace roaring

#endif /*INCLUDE_ROARING_64_BITMAP_SLICE_INDEX_HH_*/
