#if os(Linux)
import Glibc
#else
import Darwin.C
#endif
import Foundation

/// Maximum number of elements in an array
private let maxArraySize = 0x00ffffff
/// Maximum number of bytes that the payload of an array can be
/// Even though the encoding supports arrays with size up to max_array_payload_aligned,
/// the maximum allocation size is smaller as it must fit within a memory section
/// (a contiguous virtual address range). This limitation is enforced in SlabAlloc::do_alloc().
private let maxArrayPayloadAligned = 0x07ffffc0

struct NodeHeader {
    private init() {}

    enum Kind {
        case normal,

        /// This array is the main array of an innner node of a B+-tree as used
        /// in table columns.
        innerBptreeNode,

        /// This array may contain refs to subarrays. An element whose least
        /// significant bit is zero, is a ref pointing to a subarray. An element
        /// whose least significant bit is one, is just a value. It is the
        /// responsibility of the application to ensure that non-ref values have
        /// their least significant bit set. This will generally be done by
        /// shifting the desired vlue to the left by one bit position, and then
        /// setting the vacated bit to one.
        hasRefs
    };

    enum WidthType: Int {
        /// indicates how many bits every element occupies
        case bits = 0,
        /// indicates how many bytes every element occupies
        multiply = 1,
        /// each element is 1 byte
        ignore = 2
    };

    static let headerCount: Int = 8 // Number of bytes used by header

    static func dataWithoutHeader(_ data: UnsafeMutablePointer<UInt8>) -> UnsafeMutablePointer<UInt8> {
        return data + headerCount
    }

    static func dataWithHeader(_ data: UnsafePointer<UInt8>) -> UnsafePointer<UInt8> {
        return data - headerCount
    }

    static func widthType(fromHeader header: UnsafePointer<UInt8>) -> WidthType {
        WidthType(rawValue: (Int(header[4]) & 0x18) >> 3)!
    }

    static func width(fromHeader header: UnsafePointer<UInt8>) -> Int {
        (1 << (Int(header[4]) & 0x07)) >> 1
    }

    static func size(fromHeader header: UnsafePointer<UInt8>) -> Int {
        (Int(header[5]) << 16) + (Int(header[6]) << 8) + Int(header[7])
    }

    static func byteSize(header: UnsafePointer<UInt8>) -> Int {
        let size = self.size(fromHeader: header)
        let width = self.width(fromHeader: header)
        let wtype = self.widthType(fromHeader: header)
        let num_bytes = self.calculateByteSize(widthType: wtype, size: size, width: width)
        return num_bytes
    }

    static func calculateByteSize(widthType: WidthType, size: Int, width: Int) -> Int {
        var bytesCount = 0
        switch widthType {
        case .bits:
            // Current assumption is that size is at most 2^24 and that width is at most 64.
            // In that case the following will never overflow. (Assuming that size_t is at least 32 bits)
            assert(size < 0x1000000)
            let bitsCount = size * Int(width)
            bytesCount = (bitsCount + 7) >> 3;
        case .multiply:
            bytesCount = size * Int(width)
        case .ignore:
            bytesCount = size;
        }

        // Ensure 8-byte alignment
        bytesCount = (bytesCount + 7) & ~size_t(7);

        bytesCount += NodeHeader.headerCount

        return bytesCount
    }
}
