import XCTest
@testable import Core

final class CoreTests: XCTestCase {
    func testTableNames() throws {
        guard let realmPath = Bundle.module.path(forResource: "demo-v20", ofType: "realm") else {
            fatalError("Could not find demo Realm path")
        }
        let group = try Group(file: realmPath)
        XCTAssertEqual(group.tableNames[0], "")
        XCTAssertEqual(group.tableNames[1], "metadata")
        XCTAssertEqual(group.tableNames[2], "class_RealmTestClass0")
        XCTAssertEqual(group.tableNames[3], "class_RealmTestClass1")
        XCTAssertEqual(group.tableNames[4], "class_RealmTestClass2")
    }

    static var allTests = [
        ("testExample", testTableNames),
    ]
}
