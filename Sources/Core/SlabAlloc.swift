import Foundation
import Atomics
#if os(Linux)
import Glibc
#else
import Darwin.C
#endif

/// The allocator that is used to manage the memory of a Realm
/// group, i.e., a Realm database.
///
/// Optionally, it can be attached to an pre-existing database (file
/// or memory buffer) which then becomes an immuatble part of the
/// managed memory.
///
/// To attach a slab allocator to a pre-existing database, call
/// attach_file() or attach_buffer(). To create a new database
/// in-memory, call attach_empty().
///
/// For efficiency, this allocator manages its mutable memory as a set
/// of slabs.
class SlabAlloc: Allocator {
    static let footerMagicCookie = 0x3034125237E526C8
    static let sectionShift = 26
    static var pageSize: Int {
        let size = sysconf(_SC_PAGESIZE)
        assert(size > 0 && size % 4096 == 0)
        return size
    }

    var isReadOnly: Bool = false

    var refTranslations = [RefTranslation]()

    /// Struct of setup flags for this SlabAlloc.
    private var config: Config

    /// Storage for combining setup flags for initialization to the SlabAlloc.
    struct Config {
        /// Must be true if, and only if we are called on behalf of DB.
        var isShared = false
        /// Open the file in read-only mode. This implies \a Config::no_create.
        var readOnly = false
        /// Fail if the file does not already exist.
        var noCreate = false
        /// Skip validation of file header. In a
        /// set of overlapping DBs, only the first one (the one
        /// that creates/initlializes the coordination file) may validate
        /// the header, otherwise it will result in a race condition.
        var skipValidate = false
        /// If set, the caller is the session initiator and
        /// guarantees exclusive access to the file. If attaching in
        /// read/write mode, the file is modified: files on streaming form
        /// is changed to non-streaming form, and if needed the file size
        /// is adjusted to match mmap boundaries.
        /// Must be set to false if is_shared is false.
        var sessionInitiator = false
        /// Always initialize the file as if it was a newly
        /// created file and ignore any pre-existing contents. Requires that
        /// `sessionInitiator` be true as well.
        var clearFile = false
        /// Disable syncing to disk.
        var disableSync = false
        /// 32-byte key to use to encrypt and decrypt the backing storage,
        /// or nullptr to disable encryption.
        var encryptionKey: String?
    }

    /// Realm file footer, last 16 bytes
    struct StreamingFooter {
        let topRef: UInt64
        let magicCookie: UInt64

        init(data: inout Data) {
            self = data.withUnsafeBytes { $0.load(as: StreamingFooter.self) }
        }
    }

    /// Realm file header, first 24 bytes
    struct Header {
        struct TopRef {
            let slot1, slot2: UInt64
            subscript(ndx: Int) -> UInt64 { ndx == 0 ? slot1 : slot2 }
        }
        struct Mnemonic {
            let t, dash, d, b: UInt8
        }
        struct FileFormat {
            let slot1, slot2: UInt8
            subscript(ndx: Int) -> UInt8 { ndx == 0 ? slot1 : slot2 }
        }
        var topRef = TopRef(slot1: 0, slot2: 0) // 2 * 8 bytes
        // Info-block 8-bytes
        var mnemonic = Mnemonic(t: 0, dash: 0, d: 0, b: 0) // "T-DB"
        /// See `library_file_format`
        var fileFormat = FileFormat(slot1: 0, slot2: 0)
        var reserved: UInt8 = 0
        /// Bit 0 of m_flags is used to select between the two top refs.
        var flags: UInt8 = 0

        init(data: inout Data) {
            self = data.withUnsafeBytes { $0.load(as: Header.self) }
        }
    }

    enum AttachMode {
        /// Nothing is attached
        case none,
        /// We own the buffer (m_data = nullptr for empty buffer)
        ownedBuffer,
        /// We do not own the buffer
        usersBuffer,
        /// On behalf of DB
        sharedFile,
        /// Not on behalf of DB
        unsharedFile
    }

    enum Flags: Int {
        case selectBit = 1
    }

    /// A slab is a dynamically allocated contiguous chunk of memory used to
    /// extend the amount of space available for database node
    /// storage. Inter-node references are represented as file offsets
    /// (a.k.a. "refs"), and each slab creates an apparently seamless extension
    /// of this file offset addressable space. Slabs are stored as rows in the
    /// Slabs table in order of ascending file offsets.
    struct Slab {
        var refEnd: Int
        var addr: UnsafeMutablePointer<UInt8>!
        var count: Int
    }

    struct Retry {
    }

    private let file: FileHandle
    private var data: UnsafeMutablePointer<UInt8>!
    private let attachMode: AttachMode
    private(set) var topRef: Int = 0
    private var slabs = [Slab]()
    private var translationTableCount = 0;
    /// Queue for atomicness
    private let mappingQueue = DispatchQueue(label: "mappingQueue")
    private let baseline = ManagedAtomic<Int>(0)

    // MARK: Init from file
    /// - Note: Attach this allocator to the specified file.
    ///
    /// It is an error if this function is called at a time where the specified
    /// Realm file (file system inode) is modified asynchronously.
    ///
    /// In non-shared mode (when this function is called on behalf of a
    /// free-standing Group instance), it is the responsibility of the
    /// application to ensure that the Realm file is not modified concurrently
    /// from any other thread or process.
    ///
    /// In shared mode (when this function is called on behalf of a DB
    /// instance), the caller (DB::do_open()) must take steps to ensure
    /// cross-process mutual exclusion.
    ///
    /// Except for a `filePath`, the parameters are passed in through a
    /// configuration object.
    ///
    /// Please note that attach_file can fail to attach to a file due to a
    /// collision with a writer extending the file. This can only happen if the
    /// caller is *not* the session initiator. When this happens, attach_file()
    /// throws SlabAlloc::Retry, and the caller must retry the call. The caller
    /// should check if it has become the session initiator before retrying.
    /// This can happen if the conflicting thread (or process) terminates or
    /// crashes before the next retry.
    ///
    /// - Parameters:
    ///     - filePath: The path of the Realm file
    ///     - configuration: The configuration for this Slab Allocator
    ///     - ref: The offset of the top array in the Realm file.
    init(filePath: String, configuration: Config = Config(), ref: inout Int) throws {
        self.config = configuration
        self.attachMode = configuration.isShared ? .sharedFile : .unsharedFile

        self.file = FileHandle(forUpdatingAtPath: filePath)!
        let attr = try FileManager.default.attributesOfItem(atPath: filePath)
        let physicalFileSize = attr[FileAttributeKey.size] as! UInt64
        // Note that get_size() may (will) return a different size before and after
        // the call below to set_encryption_key.
        // TODO: file.set_encryption_key(cfg.encryption_key);

        var size = 0
        // The size of a database file must not exceed what can be encoded in
        // size_t.
        if physicalFileSize > Int.max {
            fatalError("Realm file too large: \(filePath)")
        } else {
            size = Int(physicalFileSize)
        }

        let header = file.readData(ofLength: 24).withUnsafeBytes { $0.load(as: Header.self) }

        let footer_ref = size < (MemoryLayout<StreamingFooter>.size + 24) ? 0 : (size - MemoryLayout<StreamingFooter>.size)
        let footer_page_base = footer_ref & ~(SlabAlloc.pageSize - 1)
        let footer_offset = footer_ref - footer_page_base
        let actualOffset = MemoryLayout<StreamingFooter>.size + footer_offset
        file.seek(toFileOffset: UInt64(actualOffset))
        let footer = file.readData(ofLength: footer_page_base).withUnsafeBytes { $0.load(as: StreamingFooter.self)}
        topRef = SlabAlloc.validateHeader(header: header, footer: footer, size: size, path: filePath)

        // TODO: reset_free_space_tracking();
        updateReaderView(fileSize: size)
        precondition(mappings.count > 0)
        data = mappings[0].primaryMapping

        encryptionReadBarrier(addr: mappings[0].primaryMapping, ndx: 0, count: MemoryLayout<Header>.size)

        // TODO: realmFileInfo = fileInfoForFile(file)
        ref = topRef
    }

    deinit {
        try! file.close()
    }

    var committedFileFormatVersion: Int {
        if mappings.count > 0 {
            // if we have mapped a file, m_mappings will have at least one mapping and
            // the first will be to the start of the file. Don't come here, if we're
            // just attaching a buffer. They don't have mappings.
            encryptionReadBarrier(addr: mappings[0].primaryMapping, ndx: 0, count: MemoryLayout<Header>.size)
        }

        let header = UnsafeRawPointer(data).load(as: Header.self)
        let slotSelector = ((Int(header.flags) & Flags.selectBit.rawValue) != 0 ? 1 : 0)
        return Int(header.fileFormat[slotSelector])
    }

    /*
      Memory mapping

      To make ref->ptr translation fast while also avoiding to have to memory map the entire file
      contiguously (which is a problem for large files on 32-bit devices and most iOS devices), it is
      essential to map the file in even sized sections.

      These sections must be large enough to hold one or more of the largest arrays, which can be up
      to 16MB. You can only mmap file space which has been allocated to a file. If you mmap a range
      which extends beyond the last page of a file, the result is undefined, so we can't do that.
      We don't want to extend the file in increments as large as the chunk size.

      As the file grows, we grow the mapping by creating a new larger one, which replaces the
      old one in the mapping table. However, we must keep the old mapping open, because older
      read transactions will continue to use it. Hence, the replaced mappings are accumulated
      and only cleaned out once we know that no transaction can refer to them anymore.

      Interaction with encryption

      When encryption is enabled, the memory mapping is to temporary memory, not the file.
      The binding to the file is done by software. This allows us to "cheat" and allocate
      entire sections. With encryption, it doesn't matter if the mapped memory logically
      extends beyond the end of file, because it will not be accessed.

      Growing/Changing the mapping table.

      There are two mapping tables:

      * m_mappings: This is the "source of truth" about what the current mapping is.
        It is only accessed under lock.
      * m_fast_mapping: This is generated to match m_mappings, but is also accessed in a
        mostly lock-free fashion from the translate function. Because of the lock free operation this
        table can only be extended. Only selected members in each entry can be changed.
        See RefTranslation in alloc.hpp for more details.
        The fast mapping also maps the slab area used for allocations - as mappings are added,
        the slab area *moves*, corresponding to the movement of m_baseline. This movement does
        not need to trigger generation of a new m_fast_mapping table, because it is only relevant
        to memory allocation and release, which is already serialized (since write transactions are
        single threaded).

      When m_mappings is changed due to an extend operation changing a mapping, or when
      it has grown such that it cannot be reflected in m_fast_mapping, we use read-copy-update:

      * A new fast mapping table is created. The old one is not modified.
      * The old one is held in a waiting area until it is no longer relevant because no
        live transaction can refer to it any more.
     */
    @inlinable func get_upper_section_boundary(start_pos: Int) -> Int {
        return sectionBase(fromIndex: 1 + sectionIndex(fromPosition: start_pos))
    }

    @inlinable func matches_section_boundary(pos: Int) -> Bool {
        let boundary = get_lower_section_boundary(start_pos: pos)
        return pos == boundary
    }

    @inlinable func align_size_to_section_boundary(size: Int) -> Int {
        if matches_section_boundary(pos: size) {
            return size
        } else {
            return get_upper_section_boundary(start_pos: size)
        }
    }

    @inlinable func get_lower_section_boundary(start_pos: Int) -> Int {
        return sectionBase(fromIndex: sectionIndex(fromPosition: start_pos))
    }

    struct MapEntry {
        var primaryMapping: UnsafeMutablePointer<UInt8>!
        var lowest_possible_xover_offset = 0
        var xover_mapping = Data()
    }

    private var mappings = [MapEntry]()
    private var oldMappings = [OldMapping]()
    private var mappingVersion = UInt64(1)
    private let m_youngest_live_version = UInt64(1)

    /// Description of to-be-deleted memory mapping
    struct OldMapping {
        let replaced_at_version: UInt64
        let mapping: UnsafeMutablePointer<UInt8>
    }

    class FreeBlock {
        var ref: Int    // ref for this entry. Saves a reverse translate / representing links as refs
        var prev: UnsafeMutablePointer<FreeBlock>? // circular doubly linked list
        var next: UnsafeMutablePointer<FreeBlock>?
        init(ref: Int) {
            self.ref = ref
        }

        func clear_links() {
            prev = nil; next = nil
        }
    }
    /// Stores sizes and used/free status of blocks before and after.
    struct BetweenBlocks {
        /// negated if block is in use,
        var block_before_size: Int32
        /// positive if block is free - and zero at end
        var block_after_size: Int32
    }

    var blockMap = [Int: UnsafeMutablePointer<FreeBlock>]()
    private func clear_freelists() {
        blockMap.removeAll()
    }

    // TODO: None of this logic is tested on its current path and is likely wrong.
    func bb_before(_ entry: UnsafePointer<FreeBlock>) -> UnsafePointer<BetweenBlocks> {
        return entry.withMemoryRebound(to: BetweenBlocks.self, capacity: 1, { $0 - 1 })
    }
    func bb_after(_ entry: UnsafePointer<FreeBlock>) -> UnsafePointer<BetweenBlocks> {
        let bb = bb_before(entry)
        let sz = bb.pointee.block_after_size
        return (entry + Int(sz)).withMemoryRebound(to: BetweenBlocks.self, capacity: 1, { $0 })
    }
    func block_after(_ bb: UnsafePointer<BetweenBlocks>) -> UnsafePointer<FreeBlock>? {
        if bb.pointee.block_after_size <= 0 {
            return nil
        }
        return (bb + 1).withMemoryRebound(to: FreeBlock.self, capacity: 1, { $0 })
    }

    func slab_to_entry(slab: inout Slab, ref_start: Int) -> UnsafePointer<FreeBlock> {
        var bb = withUnsafeMutableBytes(of: &slab.addr, { ptr in
            ptr.bindMemory(to: BetweenBlocks.self).baseAddress!
        })
        bb.pointee.block_before_size = 0;
        let block_size = slab.refEnd - ref_start - 2 * MemoryLayout<BetweenBlocks>.size
        bb.pointee.block_after_size = Int32(block_size);
        let entry = block_after(bb)!
        entry.pointee.clear_links()
        entry.pointee.ref = ref_start + MemoryLayout<BetweenBlocks>.size
        bb = UnsafeMutablePointer(mutating: bb_after(entry))
        bb.pointee.block_before_size = Int32(block_size);
        bb.pointee.block_after_size = 0;
        return entry
    }

    func push_freelist_entry(_ entry: UnsafeMutablePointer<FreeBlock>) {
        let size = bb_before(entry).pointee.block_after_size
        var header: UnsafeMutablePointer<FreeBlock>?
        if var it = blockMap[Int(size)] {
            header = it
            it = entry
            blockMap[Int(size)] = entry // needed to imitate ref cpy
            entry.pointee.next = header
            entry.pointee.prev = header?.pointee.prev
            entry.pointee.prev?.pointee.next = it
            entry.pointee.next?.pointee.prev = it
        } else {
            blockMap[Int(size)] = entry
            entry.pointee.prev = entry
            entry.pointee.next = entry.pointee.prev
        }
    }

    private func rebuild_freelists_from_slab() {
        clear_freelists();
        var ref_start = align_size_to_section_boundary(size: baseline.load(ordering: .relaxed))
        for var e in slabs {
            let entry = slab_to_entry(slab: &e, ref_start: ref_start)
            push_freelist_entry(UnsafeMutablePointer(mutating: entry))
            ref_start = align_size_to_section_boundary(size: e.refEnd)
        }
    }

    // MARK: Update Reader View
    private func updateReaderView(fileSize: Int) {
        mappingQueue.sync {
            let old_baseline = baseline.load(ordering: .relaxed)
            if fileSize <= old_baseline {
                return;
            }
            precondition(fileSize % 8 == 0) // 8-byte alignment required
            precondition(attachMode == .sharedFile || attachMode == .unsharedFile)
            // TODO: REALM_ASSERT_DEBUG(is_free_space_clean());
            var requires_new_translation = false;

            // Extend mapping by adding sections, potentially replacing older sections
            let old_slab_base = align_size_to_section_boundary(size: old_baseline)
            var old_num_mappings = sectionIndex(fromPosition: old_slab_base)
            precondition(mappings.count == old_num_mappings)
            baseline.store(fileSize, ordering: .relaxed)
            do {
                // 0. Special case: figure out if extension is to be done entirely within a single
                // existing mapping. This is the case if the new baseline (which must be larger
                // then the old baseline) is still below the old base of the slab area.
                if fileSize < old_slab_base {
                    precondition(old_num_mappings > 0)
                    let earlier_last_index = old_num_mappings - 1;
                    var cur_entry = mappings[earlier_last_index]
                    let section_start_offset = sectionBase(fromIndex: earlier_last_index)
                    let section_size = fileSize - section_start_offset
                    requires_new_translation = true
                    // save the old mapping/keep it open
                    oldMappings.append(OldMapping(replaced_at_version: m_youngest_live_version,
                                                  mapping: cur_entry.primaryMapping))
                    // extension cannot possibly happen if we alread have a xover mapping established
                    //                precondition(!cur_entry.xover_mapping.is_attached())
                    file.seek(toFileOffset: UInt64(section_start_offset))
                    cur_entry.primaryMapping = UnsafeMutablePointer<UInt8>.allocate(capacity: section_size)
                    file.readData(ofLength: section_size)
                        .copyBytes(to: cur_entry.primaryMapping, count: section_size)
                    mappingVersion += 1
                } else { // extension stretches over multiple sections:

                    // 1. figure out if there is a partially completed mapping, that we need to extend
                    // to cover a full mapping section
                    if old_baseline < old_slab_base {
                        precondition(old_num_mappings > 0);
                        let earlier_last_index = old_num_mappings - 1;
                        var cur_entry = mappings[earlier_last_index];
                        let section_start_offset = sectionBase(fromIndex: earlier_last_index);
                        let section_size = old_slab_base - section_start_offset
                        // we could not extend the old mapping, so replace it with a full, new one
                        requires_new_translation = true;
                        let section_reservation = sectionBase(fromIndex: old_num_mappings) - section_start_offset;
                        precondition(section_size == section_reservation);
                        // save the old mapping/keep it open
                        oldMappings.append(OldMapping(replaced_at_version: m_youngest_live_version,
                                                      mapping: cur_entry.primaryMapping))
                        // A xover mapping cannot be present in this case:
                        //                    precondition(!cur_entry.xover_mapping.is_attached());
                        file.seek(toFileOffset: UInt64(section_start_offset))
                        cur_entry.primaryMapping = UnsafeMutablePointer<UInt8>.allocate(capacity: section_size)
                        file.readData(ofLength: section_size)
                            .copyBytes(to: cur_entry.primaryMapping, count: section_size)
                        mappingVersion += 1
                    }

                    // 2. add any full mappings
                    //  - figure out how many full mappings we need to match the requested size
                    let new_slab_base = align_size_to_section_boundary(size: fileSize)
                    let num_full_mappings = sectionIndex(fromPosition: fileSize)
                    let num_mappings = sectionIndex(fromPosition: new_slab_base)
                    if num_mappings > old_num_mappings {
                        mappings.append(contentsOf: (0..<num_mappings).map{ _ in MapEntry() })
                    }

                    for k in old_num_mappings ..< num_full_mappings {
                        let section_start_offset = sectionBase(fromIndex: k)
                        let section_size = 1 << SlabAlloc.sectionShift
                        file.seek(toFileOffset: UInt64(section_start_offset))
                        mappings[k].primaryMapping = UnsafeMutablePointer<UInt8>.allocate(capacity: section_size)
                        file.readData(ofLength: section_size)
                            .copyBytes(to: mappings[k].primaryMapping, count: section_size)
                    }

                    // 3. add a final partial mapping if needed
                    if fileSize < new_slab_base {
                        precondition(num_mappings == num_full_mappings + 1);
                        let section_start_offset = sectionBase(fromIndex: num_full_mappings);
                        let section_size = fileSize - section_start_offset;
                        file.seek(toFileOffset: UInt64(section_start_offset))
                        mappings[num_full_mappings].primaryMapping = UnsafeMutablePointer<UInt8>.allocate(capacity: section_size)
                        file.readData(ofLength: section_size)
                            .copyBytes(to: mappings[num_full_mappings].primaryMapping, count: section_size)
                    }
                }
            }
            let ref_start = align_size_to_section_boundary(size: fileSize);
            let ref_displacement = ref_start - old_slab_base;
            if ref_displacement > 0 {
                // Rebase slabs as m_baseline is now bigger than old_slab_base
                for var e in slabs {
                    e.refEnd += ref_displacement
                }
            }

            rebuild_freelists_from_slab();

            // Build the fast path mapping

            // The fast path mapping is an array which will is used from multiple threads
            // without locking - see translate().

            // Addition of a new mapping may require a completely new fast mapping table.
            //
            // Being used in a multithreaded scenario, the old mappings must be retained open,
            // until the realm version for which they were established has been closed/detached.
            //
            // This assumes that only write transactions call do_alloc() or do_free() or needs to
            // translate refs in the slab area, and that all these uses are serialized, whether
            // that is achieved by being single threaded, interlocked or run from a sequential
            // scheduling queue.
            //
            rebuildTranslations(requiresNewTranslation: &requires_new_translation,
                                oldSectionsCount: &old_num_mappings)
        }
    }

    private func rebuildTranslations(requiresNewTranslation: inout Bool, oldSectionsCount: inout Int) {
        let free_space_size = slabs.count
        let mappingsCount = mappings.count
        if translationTableCount < mappingsCount + free_space_size {
            requiresNewTranslation = true;
        }
        var newTranslationTable = refTranslations
        if requiresNewTranslation {
            // we need a new translation table, but must preserve old, as translations using it
            // may be in progress concurrently
            if translationTableCount > 0 {
                // TODO: _old_translations.append(OldMapping(m_youngest_live_version, m_ref_translation_ptr.load()))
            }
            translationTableCount = mappingsCount + free_space_size;
            newTranslationTable = [RefTranslation](repeating: RefTranslation(), count: translationTableCount)
            oldSectionsCount = 0;
        }
        for i in oldSectionsCount ..< mappingsCount {
            newTranslationTable[i].mappingAddr = mappings[i].primaryMapping

            // TODO: new_translation_table[i].encrypted_mapping = m_mappings[i].primary_mapping.get_encrypted_mapping()

            // We don't copy over data for the cross over mapping. If the mapping is needed,
            // copying will happen on demand (in get_or_add_xover_mapping).
            // Note: that may never be needed, because if the array that needed the original cross over
            // mapping is freed, any new array allocated at the same position will NOT need a cross
            // over mapping, but just use the primary mapping.
        }
        for k in 0..<free_space_size {
            let base = slabs[k].addr
            precondition(slabs[k].count > 0)
            newTranslationTable[mappingsCount + k].mappingAddr = base
        }
        refTranslations = newTranslationTable;
    }

    static func validateHeader(header: Header, footer: StreamingFooter, size: Int, path: String) -> Int {
        // Verify that size is sane and 8-byte aligned
        if size < 24 || size % 8 != 0 {
            fatalError("Realm file has bad size (\(size)")
        }

        // First four bytes of info block is file format id
        if !(header.mnemonic.t == UnicodeScalar("T").value
                && header.mnemonic.dash == UnicodeScalar("-").value
                && header.mnemonic.d == UnicodeScalar("D").value
                && header.mnemonic.b == UnicodeScalar("B").value) {
            fatalError("Invalid mnemonic \(header) \(path)")
        }

        // Last bit in info block indicates which top_ref block is valid
        let slot_selector = (Int(header.flags) & Flags.selectBit.rawValue) != 0 ? 1 : 0

        // Top-ref must always point within buffer
        var top_ref = header.topRef[slot_selector];
        if slot_selector == 0 && top_ref == 0xFFFFFFFFFFFFFFFF {
            if size < 24 + MemoryLayout<StreamingFooter>.size {
                fatalError("Invalid streaming format size (\(size)")
            }
            top_ref = footer.topRef
            if footer.magicCookie != SlabAlloc.footerMagicCookie {
                fatalError("Invalid streaming format cookie (\(footer.magicCookie))")
            }
        }
        if top_ref % 8 != 0 {
            fatalError("Top ref not aligned (\(top_ref))")
        }
        if top_ref >= size {
            fatalError("Top ref outside file (size = \(size))")
        }
        return Int(top_ref)
    }
}
