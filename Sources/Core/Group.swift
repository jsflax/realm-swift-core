import Foundation
#if os(Linux)
import Glibc
#else
import Darwin.C
#endif

/// A group is a collection of named tables.
class Group /*: ArrayParent*/ {
    enum OpenMode {
        /// Open in read-only mode. Fail if the file does not already exist.
        case readOnly,
        /// Open in read/write mode. Create the file if it doesn't exist.
        readWrite,
        /// Open in read/write mode. Fail if the file does not already exist.
        readWriteNoCreate
    }

    /// `top` is the root node (or top array) of the Realm, and has the
    /// following layout:
    ///
    /// <pre>
    ///
    ///                                                     Introduced in file
    ///   Slot  Value                                       format version
    ///   ---------------------------------------------------------------------
    ///    1st   m_table_names
    ///    2nd   m_tables
    ///    3rd   Logical file size
    ///    4th   GroupWriter::m_free_positions (optional)
    ///    5th   GroupWriter::m_free_lengths   (optional)
    ///    6th   GroupWriter::m_free_versions  (optional)
    ///    7th   Transaction number / version  (optional)
    ///    8th   History type         (optional)             4
    ///    9th   History ref          (optional)             4
    ///   10th   History version      (optional)             7
    ///   11th   Sync File Id         (optional)            10
    ///
    /// </pre>
    ///
    /// The 'History type' slot stores a value of type
    /// Replication::HistoryType. The 'History version' slot stores a history
    /// schema version as returned by Replication::get_history_schema_version().
    ///
    /// The first three entries are mandatory. In files created by
    /// Group::write(), none of the optional entries are present and the size of
    /// `m_top` is 3. In files updated by Group::commit(), the 4th and 5th entry
    /// are present, and the size of `m_top` is 5. In files updated by way of a
    /// transaction (Transaction::commit()), the 4th, 5th, 6th, and 7th entry
    /// are present, and the size of `m_top` is 7. In files that contain a
    /// changeset history, the 8th, 9th, and 10th entry are present. The 11th entry
    /// will be present if the file is syncked and the client has received a client
    /// file id from the server.
    ///
    /// When a group accessor is attached to a newly created file or an empty
    /// memory buffer where there is no top array yet, `m_top`, `m_tables`, and
    /// `m_table_names` will be left in the detached state until the initiation
    /// of the first write transaction. In particular, they will remain in the
    /// detached state during read transactions that precede the first write
    /// transaction.
    private let top: Array<Any>
    let tableNames: Array<ShortString>
    private var alloc: Allocator

    /// Equivalent to calling open(const std::string&, const char*, OpenMode)
    /// on an unattached group accessor.
    init(file: String, encryption_key: String? = nil, mode: OpenMode = .readOnly) throws {
        var ref = 0
        self.alloc = try SlabAlloc(filePath: file, ref: &ref)
        self.top = Array(from: ref, allocator: &alloc)
        self.tableNames = Array(from: top[0], allocator: &alloc)
    }
};
