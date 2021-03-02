import XCTest
@testable import realm_swift_core

final class realm_swift_coreTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(realm_swift_core().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
