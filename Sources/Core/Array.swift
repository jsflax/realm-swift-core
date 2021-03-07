import Foundation

/// STUB
protocol ArrayParent {
    // STUB
}

protocol Node {
    var data: UnsafeMutablePointer<UInt8> { get } // Points to first byte after header
    var ref: Int { get }
    var count: Int { get }
}

/// This type plays against the Swift generic system
/// to optimize for short strings when that type is known.
/// The underlying values will still be `String`, but the
/// read/write path is optimized.
struct ShortString {
}

struct Array<Element>: Node, ArrayParent {
    struct VTable {
        var width: UInt8
    }

    private(set) var data: UnsafeMutablePointer<UInt8>
    private(set) var ref: Int
    private(set) var count: Int

    init(from ref: Int, allocator: inout Allocator) {
        let header = allocator.translate(ref: ref)
        self.data = NodeHeader.dataWithoutHeader(header)
        self.count = NodeHeader.size(fromHeader: header)
        self.ref = ref
    }

    /// Size of an element (meaning depend on type of array).
    var width: UInt8 {
        UInt8(NodeHeader.width(fromHeader: NodeHeader.dataWithHeader(data)))
    }
    /// Min number that can be stored with current `width`
    lazy var lowerBound: Int64 = {
        switch width {
        case 0: return 0
        case 1: return 0
        case 2: return 0
        case 4: return 0
        case 8: return -0x80
        case 16: return -0x8000
        case 32: return -0x80000000
        case 64: return -0x8000000000000000
        default: fatalError("Impossible width")
        }
    }()
    /// Max number that can be stored with current `width`
    lazy var upperBound: Int64 = {
        switch width {
        case 0: return 0
        case 1: return 1
        case 2: return 3
        case 4: return 15
        case 8: return 0x7F
        case 16: return 0x7FFF
        case 32: return 0x7FFFFFFF
        case 64: return 0x7FFFFFFFFFFFFFFF
        default: fatalError("Impossible width")
        }
    }()

    var vtable: VTable {
        VTable(width: width)
    }

    subscript(ndx: Int) -> Int {
        switch vtable.width {
        case 0: return 0
        case 1:
            let offset = ndx >> 3
            return Int(data[offset] >> (ndx & 7)) & 0x01
        case 2:
            let offset = ndx >> 2
            return Int(data[offset] >> ((ndx & 3) << 1)) & 0x03
        case 4:
            let offset = ndx >> 1
            return Int(data[offset] >> ((ndx & 1) << 2)) & 0x0F
        case 8:
            return Int(UnsafeRawPointer(data).load(fromByteOffset: ndx, as: Int8.self))
        case 16:
            let offset = ndx * 2
            return Int(UnsafeRawPointer(data).load(fromByteOffset: offset, as: Int16.self))
        case 32:
            let offset = ndx * 4
            return Int(UnsafeRawPointer(data).load(fromByteOffset: offset, as: Int32.self))
        case 64:
            let offset = ndx * 8
            return Int(UnsafeRawPointer(data).load(fromByteOffset: offset, as: Int64.self))
        default: return -1
        }
    }
}

extension Array where Element == ShortString {
    @inlinable subscript(ndx: Int) -> String {
        precondition(ndx < count)
        if width == 0 {
            return ""
        }
        let data = self.data + (ndx * Int(width))
        let width = Int(self.width) // Convert to signed to account for overflow
        let arraySize: Int = Int((width - 1) - Int(data[width - 1]))

        if arraySize <= 0 {
            return ""
        }
        precondition(data[Int(arraySize)] == 0) // Realm guarantees 0 terminated return strings
        let str = String(cString: data)
        return str
    }
}
