#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <vector>

#include "roaring.hh"      // the amalgamated roaring.hh includes roaring64map.hh
#include "roaring64bsi.hh" // the amalgamated roaring.hh includes roaring64map.hh

//测试代码参考java实现：https://github.com/RoaringBitmap/RoaringBitmap/blob/master/bsi/src/test/java/org/roaringbitmap/bsi/R64BSITest.java

void setup(roaring::Roaring64Bsi& bsi) {
    std::unordered_map<uint64_t, uint64_t> testDataSet;
    for (uint64_t i = 1; i < 100; i++) {
        testDataSet.emplace(i, i);
    }

    for (const auto& [key, value] : testDataSet) {
        bsi.setValue(key, value);
    }
}
void testSetAndGet() {
    std::cout << "testSetAndGet" << std::endl;

    roaring::Roaring64Bsi bsi(1, 99);
    setup(bsi);

    for (uint64_t i = 1; i < 100; i++) {
        auto const& [value, found] = bsi.getValue(i);
        assert(found);
        assert(value == i);
    }
}

void testMerge() {
    std::cout << "testMerge" << std::endl;

    roaring::Roaring64Bsi bsiA;
    for (uint64_t i = 1; i < 100; i++) {
        bsiA.setValue(i, i);
    }
    roaring::Roaring64Bsi bsiB;
    for (uint64_t i = 100; i < 199; i++) {
        bsiB.setValue(i, i);
    }
    assert(bsiA.getExistenceBitmap().cardinality() == 99);
    assert(bsiB.getExistenceBitmap().cardinality() == 99);
    assert(!bsiA.merge(bsiA));
    assert(bsiA.merge(bsiB));
    for (uint64_t i = 1; i < 199; i++) {
        auto const& [value, found] = bsiA.getValue(i);
        assert(found);
        assert(value == i);
    }
}

void testClone() {
    std::cout << "testClone" << std::endl;

    roaring::Roaring64Bsi bsi(1, 99);

    std::unordered_map<uint64_t, uint64_t> testDataSet;
    for (uint64_t i = 1; i < 100; i++) {
        testDataSet.emplace(i, i);
    }

    std::vector<std::tuple<uint64_t, uint64_t>> collect;
    for (const auto& [key, value] : testDataSet) {
        collect.emplace_back(key, value);
    }

    bsi.setValues(collect);

    assert(bsi.getExistenceBitmap().cardinality() == 99);
    auto clone = bsi.clone();

    for (uint64_t i = 1; i < 100; i++) {
        auto const& [value, found] = clone->getValue(i);
        assert(found);
        assert(value == i);
    }
}

void testAdd() {
    std::cout << "testAdd" << std::endl;

    roaring::Roaring64Bsi bsiA;
    for (uint64_t i = 1; i < 100; i++) {
        bsiA.setValue(i, i);
    }
    roaring::Roaring64Bsi bsiB;
    for (uint64_t i = 1; i < 120; i++) {
        bsiB.setValue(i, i);
    }

    bsiA.add(bsiB);

    for (uint64_t i = 1; i < 120; i++) {
        auto const& [value, found] = bsiA.getValue(i);
        assert(found);
        if (i < 100) {
            assert(value == i * 2);
        } else {
            assert(value == i);
        }
    }
}

void testAddAndEvaluate() {
    std::cout << "testAddAndEvaluate" << std::endl;

    roaring::Roaring64Bsi bsiA;
    for (uint64_t i = 1; i < 100; i++) {
        bsiA.setValue(i, i);
    }
    roaring::Roaring64Bsi bsiB;
    for (uint64_t i = 1; i < 120; i++) {
        bsiB.setValue(120 - i, i);
    }

    bsiA.add(bsiB);

    auto result = bsiA.compare(roaring::BsiOperation::EQ, 120, 0);
    assert(result);
    assert(99 == result->cardinality());

    std::vector<uint64_t> ans(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 99; i++) {
        assert(ans[i] == (i + 1));
    }

    result = bsiA.compare(roaring::BsiOperation::RANGE, 1, 20);
    assert(result);
    assert(20 == result->cardinality());
    ans.resize(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 20; i++) {
        assert(ans[i] == (i + 100));
    }
}

void TestIO4Stream() {
    std::cout << "TestIO4Stream" << std::endl;

    roaring::Roaring64Bsi bsi(1, 99);
    for (uint64_t i = 1; i < 100; i++) {
        bsi.setValue(i, i);
    }

    std::unique_ptr<char[]> buffer;
    size_t size = bsi.serializeBuffer(buffer);

    roaring::Roaring64Bsi newBsi;

    newBsi.deserialize(buffer.get());

    assert(newBsi.getExistenceBitmap().cardinality() == 99);

    for (uint64_t i = 1; i < 100; i++) {
        auto const& [value, found] = newBsi.getValue(i);
        assert(found);
        assert(value == i);
    }
}

void testIO4Buffer() {
    std::cout << "testIO4Buffer" << std::endl;

    roaring::Roaring64Bsi bsi(1, 99);
    for (uint64_t i = 1; i < 100; i++) {
        bsi.setValue(i, i);
    }

    std::vector<char> buffer(bsi.serializedSizeInBytes());
    bsi.serialize(buffer.data());

    roaring::Roaring64Bsi newBsi;

    newBsi.deserialize(buffer.data());

    assert(newBsi.getExistenceBitmap().cardinality() == 99);

    for (uint64_t i = 1; i < 100; i++) {
        auto const& [value, found] = newBsi.getValue(i);
        assert(found);
        assert(value == i);
    }
}

void testEQ() {
    std::cout << "testEQ" << std::endl;

    roaring::Roaring64Bsi bsi(1, 99);
    for (uint64_t x = 1; x < 100; x++) {
        if (x <= 50) {
            bsi.setValue(x, 1);
        } else {
            bsi.setValue(x, x);
        }
    }

    auto bitmap = bsi.compare(roaring::BsiOperation::EQ, 1, 0);
    assert(bitmap);
    assert(50 == bitmap->cardinality());
}

void testNotEQ() {
    std::cout << "testNotEQ" << std::endl;

    {
        roaring::Roaring64Bsi bsi;
        bsi.setValue(1, 99);
        bsi.setValue(2, 1);
        bsi.setValue(3, 50);

        auto result = bsi.compare(roaring::BsiOperation::NEQ, 99, 0, nullptr);
        assert(2 == result->cardinality());
        std::vector<uint64_t> ans(result->cardinality());
        result->toUint64Array(ans.data());
        assert(ans[0] == 2);
        assert(ans[1] == 3);

        result = bsi.compare(roaring::BsiOperation::NEQ, 100, 0);
        assert(3 == result->cardinality());
        ans.resize(result->cardinality());
        result->toUint64Array(ans.data());
        assert(ans[0] == 1 && ans[1] == 2 && ans[2] == 3);
    }

    roaring::Roaring64Bsi bsi;
    bsi.setValue(1, 99);
    bsi.setValue(2, 99);
    bsi.setValue(3, 99);

    auto result = bsi.compare(roaring::BsiOperation::NEQ, 99, 0);
    assert(result->isEmpty());

    result = bsi.compare(roaring::BsiOperation::NEQ, 1, 0);
    assert(3 == result->cardinality());
    std::vector<uint64_t> ans(result->cardinality());
    result->toUint64Array(ans.data());
    assert(ans[0] == 1 && ans[1] == 2 && ans[2] == 3);
}

void testGT() {
    std::cout << "testGT" << std::endl;

    roaring::Roaring64Bsi bsi(1, 99);
    setup(bsi);

    auto result = bsi.compare(roaring::BsiOperation::GT, 50, 0);
    assert(49 == result->cardinality());
    std::vector<uint64_t> ans(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 49; i++) {
        assert(ans[i] == (i + 51));
    }

    result = bsi.compare(roaring::BsiOperation::GT, 0, 0);
    assert(99 == result->cardinality());
    ans.resize(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 99; i++) {
        assert(ans[i] == (i + 1));
    }

    result = bsi.compare(roaring::BsiOperation::GT, 99, 0);
    assert(result->isEmpty());
}

void testGE() {
    std::cout << "testGE" << std::endl;
    roaring::Roaring64Bsi bsi(1, 99);
    setup(bsi);

    auto result = bsi.compare(roaring::BsiOperation::GE, 50, 0);
    assert(50 == result->cardinality());
    std::vector<uint64_t> ans(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 50; i++) {
        assert(ans[i] == (i + 50));
    }
    result = bsi.compare(roaring::BsiOperation::GE, 1, 0);
    assert(99 == result->cardinality());
    ans.resize(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 99; i++) {
        assert(ans[i] == (i + 1));
    }

    result = bsi.compare(roaring::BsiOperation::GE, 100, 0);
    assert(result->isEmpty());
}

void testLT() {
    std::cout << "testLT" << std::endl;
    roaring::Roaring64Bsi bsi(1, 99);
    setup(bsi);

    auto result = bsi.compare(roaring::BsiOperation::LT, 50, 0);
    assert(49 == result->cardinality());
    std::vector<uint64_t> ans(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 49; i++) {
        assert(ans[i] == (i + 1));
    }

    result = bsi.compare(roaring::BsiOperation::LT, INT32_MAX, 0);
    assert(99 == result->cardinality());
    ans.resize(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 99; i++) {
        assert(ans[i] == (i + 1));
    }

    result = bsi.compare(roaring::BsiOperation::LT, 1, 0);
    assert(result->isEmpty());
}

void testLE() {
    std::cout << "testLE" << std::endl;
    roaring::Roaring64Bsi bsi(1, 99);
    setup(bsi);

    auto result = bsi.compare(roaring::BsiOperation::LE, 50, 0);
    assert(50 == result->cardinality());
    std::vector<uint64_t> ans(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 50; i++) {
        assert(ans[i] == (i + 1));
    }
    result = bsi.compare(roaring::BsiOperation::LE, INT32_MAX, 0);
    assert(99 == result->cardinality());
    ans.resize(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 99; i++) {
        assert(ans[i] == (i + 1));
    }

    result = bsi.compare(roaring::BsiOperation::LE, 0, 0);
    assert(result->isEmpty());
}

void testRANGE() {
    std::cout << "testRANGE" << std::endl;
    roaring::Roaring64Bsi bsi(1, 99);
    setup(bsi);

    auto result = bsi.compare(roaring::BsiOperation::RANGE, 10, 20);
    assert(11 == result->cardinality());
    std::vector<uint64_t> ans(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 11; i++) {
        assert(ans[i] == (i + 10));
    }
    result = bsi.compare(roaring::BsiOperation::RANGE, 1, 200);
    assert(99 == result->cardinality());
    ans.resize(result->cardinality());
    result->toUint64Array(ans.data());
    for (uint64_t i = 0; i < 99; i++) {
        assert(ans[i] == (i + 1));
    }

    result = bsi.compare(roaring::BsiOperation::RANGE, 1000, 2000);
    assert(result->isEmpty());
}

void testSum() {
    std::cout << "testSum" << std::endl;

    roaring::Roaring64Bsi bsi(1, 99);
    for (uint64_t i = 1; i < 100; i++) {
        bsi.setValue(i, i);
    }

    std::unique_ptr<roaring::Roaring64Map> foundSet = std::make_unique<roaring::Roaring64Map>();
    for (uint64_t i = 1; i < 51; ++i) {
        foundSet->add(i);
    }

    auto const& sumPair = bsi.sum(foundSet.get());

    uint64_t sum = 0;
    uint64_t count = 0;
    for (uint64_t i = 1; i < 51; ++i) {
        sum += i;
        count++;
    }

    assert(std::get<0>(sumPair) == sum && std::get<1>(sumPair) == count);
}

void testValueZero() {
    std::cout << "testValueZero" << std::endl;
    roaring::Roaring64Bsi bsi;
    bsi.setValue(0, 0);
    bsi.setValue(1, 0);
    bsi.setValue(2, 1);

    auto result = bsi.compare(roaring::BsiOperation::EQ, 0, 0);
    assert(2 == result->cardinality());
    std::vector<uint64_t> ans(result->cardinality());
    result->toUint64Array(ans.data());
    assert(ans[0] == 0);
    assert(ans[1] == 1);

    result = bsi.compare(roaring::BsiOperation::EQ, 1, 0);
    assert(1 == result->cardinality());
    ans.resize(result->cardinality());
    result->toUint64Array(ans.data());
    assert(ans[0] == 2);
}

void testTopK() {
    std::cout << "testTopK" << std::endl;
    std::unique_ptr<roaring::Roaring64Map> f =
            std::make_unique<roaring::Roaring64Map>(roaring::Roaring64Map::bitmapOfList({5, 6}));

    roaring::Roaring64Bsi bsi;
    bsi.setValue(1, 3);
    bsi.setValue(2, 6);
    bsi.setValue(3, 4);
    bsi.setValue(4, 10);
    bsi.setValue(5, 7);

    assert(*bsi.topK(2) == roaring::Roaring64Map::bitmapOfList({4, 5}));
    //test k=0
    assert(*bsi.topK(0) == roaring::Roaring64Map::bitmapOfList({}));
    assert(*bsi.topK(2, f.get()) == roaring::Roaring64Map::bitmapOfList({5}));

    //test bsi empty
    roaring::Roaring64Bsi bsi1;
    assert(*bsi1.topK(1) == roaring::Roaring64Map::bitmapOfList({}));
    assert(*bsi1.topK(1, f.get()) == roaring::Roaring64Map::bitmapOfList({}));
    assert(*bsi1.topK(3, f.get()) == roaring::Roaring64Map::bitmapOfList({}));

    //test bsi with all zero values
    roaring::Roaring64Bsi bsi2;
    bsi2.setValue(1, 3);
    bsi2.setValue(2, 2);
    bsi2.setValue(3, 2);
    bsi2.setValue(4, 1);
    bsi2.setValue(5, 0);
    bsi2.setValue(6, 0);
    assert(*bsi2.topK(5) == roaring::Roaring64Map::bitmapOfList({1, 2, 3, 4, 5}));

    roaring::Roaring64Bsi bsi3;
    bsi3.setValue(0, 7);
    bsi3.setValue(1, 6);
    bsi3.setValue(2, 1);
    bsi3.setValue(3, 7);
    bsi3.setValue(4, 0);
    bsi3.setValue(5, 9);
    bsi3.setValue(6, 9);
    bsi3.setValue(7, 8);
    bsi3.setValue(8, 9);
    bsi3.setValue(9, 8);

    assert(*bsi3.topK(4) == roaring::Roaring64Map::bitmapOfList({5, 6, 7, 8}));
    assert(*bsi3.topK(5) == roaring::Roaring64Map::bitmapOfList({5, 6, 7, 8, 9}));
    assert(*bsi3.topK(2) == roaring::Roaring64Map::bitmapOfList({5, 6}));
}

void testTranspose() {
    std::cout << "testTranspose" << std::endl;

    roaring::Roaring64Bsi bsi;
    bsi.setValue(1, 2);
    bsi.setValue(2, 4);
    bsi.setValue(3, 4);
    bsi.setValue(4, 8);
    bsi.setValue(5, 8);
    auto re = bsi.transpose();
    assert(*re == roaring::Roaring64Map::bitmapOfList({2, 4, 8}));
}

void testTransposeWithCount() {
    std::cout << "testTransposeWithCount" << std::endl;

    roaring::Roaring64Bsi bsi;
    bsi.setValue(1, 2);
    bsi.setValue(2, 4);
    bsi.setValue(3, 4);
    bsi.setValue(4, 8);
    bsi.setValue(5, 8);
    auto re = bsi.transposeWithCount();
    assert(re->getExistenceBitmap() == roaring::Roaring64Map::bitmapOfList({2, 4, 8}));
    assert(std::get<0>(re->getValue(2)) == 1);
    assert(std::get<0>(re->getValue(4)) == 2);
    assert(std::get<0>(re->getValue(8)) == 2);
}

void testIssue743() {
    std::cout << "testIssue743" << std::endl;

    roaring::Roaring64Bsi bsi;
    bsi.setValue(100L, 3L);
    bsi.setValue(1L, 392L);

    std::vector<char> buffer(bsi.serializedSizeInBytes());
    bsi.serialize(buffer.data());

    roaring::Roaring64Bsi de_bsi;
    de_bsi.deserialize(buffer.data());
    assert(de_bsi.getValue(100L) == bsi.getValue(100L));
    assert(de_bsi.getValue(1L) == bsi.getValue(1L));
}

void testIssue753() {
    std::cout << "testIssue753" << std::endl;
    roaring::Roaring64Bsi bsi;
    for (uint64_t i = 1; i < 100; i++) {
        bsi.setValue(i, i);
    }

    /*assert(bsi.compare(roaring::BsiOperation::RANGE, -4, 56)->cardinality() == 56);
    assert(bsi.compare(roaring::BsiOperation::RANGE, -4, 129)->cardinality() == 99);
    assert(bsi.compare(roaring::BsiOperation::RANGE, -4, 200)->cardinality() == 99);
    assert(bsi.compare(roaring::BsiOperation::RANGE, -4, 20000)->cardinality() == 99);
    assert(bsi.compare(roaring::BsiOperation::RANGE, -4, -129)->cardinality() == 0);
    assert(bsi.compare(roaring::BsiOperation::RANGE, -4, -2)->cardinality() == 0);*/
    assert(bsi.compare(roaring::BsiOperation::RANGE, 0, 56)->cardinality() == 56);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 0, 129)->cardinality() == 99);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 0, 200)->cardinality() == 99);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 0, 20000)->cardinality() == 99);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 0, 0)->cardinality() == 0);

    assert(bsi.compare(roaring::BsiOperation::RANGE, 4, 56)->cardinality() == 53);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 4, 129)->cardinality() == 96);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 4, 200)->cardinality() == 96);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 4, 20000)->cardinality() == 96);
    //assert(bsi.compare(roaring::BsiOperation::RANGE, 4, -129)->cardinality() == 0);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 4, 0)->cardinality() == 0);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 4, 2)->cardinality() == 0);
    //assert(bsi.compare(roaring::BsiOperation::RANGE, -129, -14)->cardinality() == 0);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 129, 14)->cardinality() == 0);
    assert(bsi.compare(roaring::BsiOperation::RANGE, 129, 2000)->cardinality() == 0);
}

void testIssue755() {
    std::cout << "testIssue755" << std::endl;
    roaring::Roaring64Bsi bsi;
    bsi.setValue(100L, 3L);
    bsi.setValue(1L, (long)INT32_MAX * 2 + 23456);
    bsi.setValue(2L, (long)INT32_MAX + 23456);
    assert(std::get<0>(bsi.getValue(100L)) == 3L);                        // {{3,true}}
    assert(std::get<0>(bsi.getValue(1L)) == (long)INT32_MAX * 2 + 23456); // {23455,true}
    assert(std::get<0>(bsi.getValue(2L)) == (long)INT32_MAX + 23456);     // {-2147460193,true}
}

int main() {
    testSetAndGet();
    testMerge();
    testClone();
    testAdd();
    testAddAndEvaluate();
    TestIO4Stream();
    testIO4Buffer();
    testEQ();
    testNotEQ();
    testGT();
    testGE();
    testLT();
    testLE();
    testRANGE();
    testSum();
    testValueZero();
    testTopK();
    testTranspose();
    testTransposeWithCount();
    testIssue743();
    testIssue753();
    testIssue755();
    std::cout << "All tests passed!" << std::endl;

    return 0;
}
