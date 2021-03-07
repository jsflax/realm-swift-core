import Foundation
import Atomics

#if os(Linux)
import Glibc
#else
import Darwin.C
#endif


/// The following logically belongs in the slab allocator, but is placed
/// here to optimize a critical path:
///
/// The ref translation splits the full ref-space (both below and above baseline)
/// into equal chunks.
struct RefTranslation {
    var mappingAddr: UnsafeMutablePointer<UInt8>!
    var cookie: UInt64 = 0x1234567890
    let lowestPossibleXoverOffset = ManagedAtomic<Int>(0)

    /// `xoverMappingAddr` is used for memory synchronization of the fields
    /// `xoverMappingBase` and`xoverEncryptedMapping`. It also imposes an ordering
    /// on `lowestPossibleXoverOffset` such that once a non-null value of `xoverMappingAddr`
    /// has been acquired, `lowestPossibleXoverOffset` will never change.
    var xoverMappingAddr = ManagedAtomic<UnsafeMutablePointer<UInt8>?>(nil)
    let xoverMappingBase = 0;
    let encryptedMapping: EncryptedFileMapping? = nil
    let xoverEncryptedMapping: EncryptedFileMapping? = nil
}

protocol Allocator {
    func alloc(size: Int) -> UnsafeMutableRawPointer

    func do_alloc(size: Int) -> UnsafeMutableRawPointer

    var isReadOnly: Bool { get }

    // TODO: This pointer may be changed concurrently with access, so make sure it is
    // TODO: atomic!
    var refTranslations: [RefTranslation] { get }
}

private let sectionShift = 26;

extension Allocator {
    func alloc(size: Int) -> UnsafeMutableRawPointer {
        precondition(!isReadOnly)
        return do_alloc(size: size)
    }

    func do_alloc(size: Int) -> UnsafeMutableRawPointer {
        return UnsafeMutableRawPointer.allocate(byteCount: size, alignment: MemoryLayout<UInt8>.alignment)
    }

    func sectionIndex(fromPosition position: Int) -> Int {
        return position >> sectionShift // 64Mb chunks
    }

    func sectionBase(fromIndex index: Int) -> Int {
        return index << sectionShift; // 64MB chunks
    }

    // This function is called to handle translation of a ref which is above the limit for its
    // memory mapping. This requires one of three:
    // * bumping the limit of the mapping. (if the entire array is inside the mapping)
    // * adding a cross-over mapping. (if the array crosses a mapping boundary)
    // * using an already established cross-over mapping. (ditto)
    // this can proceed concurrently with other calls to translate()
    fileprivate func translate_less_critical(ref_translation_ptr: [RefTranslation],
                                             ref: Int) -> UnsafeMutablePointer<UInt8> {
        let idx = sectionIndex(fromPosition: ref)
        let txl = ref_translation_ptr[idx]
        let offset = ref - sectionBase(fromIndex: idx)
        var addr = txl.mappingAddr + offset
        encryptionReadBarrier(addr: addr, count: NodeHeader.headerCount, mapping: txl.encryptedMapping)
        
        let size = NodeHeader.byteSize(header: addr)
        let crosses_mapping = offset + size > (1 << sectionShift)
        // Move the limit on use of the existing primary mapping.
        // Take into account that another thread may attempt to change / have changed it concurrently,
        let lowestPossibleXoverOffset = txl.lowestPossibleXoverOffset.load(ordering: .relaxed)
        let new_lowest_possible_xover_offset = offset + (crosses_mapping ? 0 : size)
        while new_lowest_possible_xover_offset > lowestPossibleXoverOffset {
            if (txl.lowestPossibleXoverOffset
                    .weakCompareExchange(expected: lowestPossibleXoverOffset,
                                         desired: new_lowest_possible_xover_offset,
                                         successOrdering: .relaxed,
                                         failureOrdering: .relaxed).exchanged) {
                break
            }
        }
        if !crosses_mapping {
            // Array fits inside primary mapping, no new mapping needed.
            encryptionReadBarrier(addr: addr, count: size, mapping: txl.encryptedMapping)
            return addr
        } else {
            // we need a cross-over mapping. If one is already established, use that.
            var xover_mapping_addr = txl.xoverMappingAddr.load(ordering: .acquiring);
            if xover_mapping_addr == nil {
                // we need to establish a xover mapping - or wait for another thread to finish
                // establishing one:
                // TODO
//                const_cast<Allocator*>(this)->get_or_add_xover_mapping(txl, idx, offset, size);
                // reload (can be relaxed since the call above synchronizes on a mutex)
                xover_mapping_addr = txl.xoverMappingAddr.load(ordering: .relaxed);
            }
            // array is now known to be inside the established xover mapping:
            addr = xover_mapping_addr! + (offset - txl.xoverMappingBase)
            encryptionReadBarrier(addr: addr, ndx: size, mapping: txl.xoverEncryptedMapping)
            return addr
        }
    }

    // performance critical part of the translation process. Less critical code is in translate_less_critical.
    private func translate_critical(ref_translation_ptr: [RefTranslation], ref: Int) -> UnsafeMutablePointer<UInt8> {
        let idx = sectionIndex(fromPosition: ref)
        let txl = ref_translation_ptr[idx]
        if txl.cookie == 0x1234567890 {
            let offset = ref - sectionBase(fromIndex: idx);

            let lowest_possible_xover_offset = txl.lowestPossibleXoverOffset.load(ordering: .relaxed)
            if offset < lowest_possible_xover_offset {
                // the lowest possible xover offset may grow concurrently, but that will not affect this code path
                let addr = txl.mappingAddr + offset
                encryptionReadBarrier(addr: addr, count: NodeHeader.headerCount, mapping: EncryptedFileMapping())
                return addr
            } else {
                // the lowest possible xover offset may grow concurrently, but that will be handled inside the call
                return translate_less_critical(ref_translation_ptr: ref_translation_ptr, ref: ref)
            }
        }
        fatalError("Invalid ref translation entry \(txl.cookie) \(0x1234567890)")
    }

    @inlinable func translate(ref: Int) -> UnsafeMutablePointer<UInt8> {
        translate_critical(ref_translation_ptr: refTranslations, ref: ref)
    }
}
