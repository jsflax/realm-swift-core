import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(realm_swift_coreTests.allTests),
    ]
}
#endif
