import Foundation

enum ProtobufSize {
    static func lengthDelimitedField(fieldNumber: Int, payloadBytes: Int) -> Int {
        tag(fieldNumber: fieldNumber, wireType: 2) + varint(payloadBytes) + payloadBytes
    }

    static func stringField(fieldNumber: Int, value: String) -> Int {
        guard value.isEmpty == false else {
            return 0
        }
        return lengthDelimitedField(fieldNumber: fieldNumber, payloadBytes: value.utf8.count)
    }

    /// Map-entry key and value fields are encoded even when they contain their
    /// scalar default, unlike ordinary proto3 string fields.
    static func requiredStringField(fieldNumber: Int, value: String) -> Int {
        lengthDelimitedField(fieldNumber: fieldNumber, payloadBytes: value.utf8.count)
    }

    static func int64Field(fieldNumber: Int, value: Int64) -> Int {
        guard value != 0 else {
            return 0
        }
        return tag(fieldNumber: fieldNumber, wireType: 0) + signedVarint(value)
    }

    static func int32Field(fieldNumber: Int, value: Int32) -> Int {
        int64Field(fieldNumber: fieldNumber, value: Int64(value))
    }

    static func varint(_ value: Int) -> Int {
        var remaining = UInt(value)
        var count = 1
        while remaining >= 0x80 {
            remaining >>= 7
            count += 1
        }
        return count
    }

    private static func signedVarint(_ value: Int64) -> Int {
        value < 0 ? 10 : varint(Int(value))
    }

    private static func tag(fieldNumber: Int, wireType: Int) -> Int {
        varint((fieldNumber << 3) | wireType)
    }
}
