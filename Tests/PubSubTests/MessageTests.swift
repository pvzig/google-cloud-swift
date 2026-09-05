import Foundation
import PubSub
import Testing

@Suite("Message")
struct MessageTests {
    @Test(
        "String payloads use UTF-8 and preserve empty strings and embedded nulls",
        arguments: zip(
            ["", "Hello", "é\0🌍"],
            [
                [UInt8](),
                [0x48, 0x65, 0x6C, 0x6C, 0x6F],
                [0xC3, 0xA9, 0x00, 0xF0, 0x9F, 0x8C, 0x8D],
            ]
        )
    )
    func stringPayloadUsesUTF8(string: String, expectedBytes: [UInt8]) {
        let message = Message(string: string)

        #expect(Array(message.data) == expectedBytes)
        #expect(message.attributes.isEmpty)
        #expect(message.orderingKey.isEmpty)
    }

    @Test("String messages preserve attributes and ordering keys")
    func stringMessagePreservesMetadata() {
        let message = Message(
            string: "Hello",
            attributes: ["event": "created"],
            orderingKey: "user-123"
        )

        #expect(message.attributes == ["event": "created"])
        #expect(message.orderingKey == "user-123")
    }

    @Test("JSON messages encode the payload with default settings")
    func jsonMessageEncodesPayload() throws {
        let message = try Message(json: ["event": "created"])
        let payload = try JSONDecoder().decode([String: String].self, from: message.data)

        #expect(payload == ["event": "created"])
        #expect(message.attributes.isEmpty)
        #expect(message.orderingKey.isEmpty)
    }

    @Test("JSON messages honor the supplied encoder and preserve metadata")
    func jsonMessageUsesConfiguredEncoder() throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .secondsSince1970
        let message = try Message(
            json: ["createdAt": Date(timeIntervalSince1970: 1234)],
            encoder: encoder,
            attributes: ["event": "created"],
            orderingKey: "user-123"
        )
        let payload = try JSONDecoder().decode([String: Double].self, from: message.data)

        #expect(payload == ["createdAt": 1234])
        #expect(message.attributes == ["event": "created"])
        #expect(message.orderingKey == "user-123")
    }

    @Test("JSON messages propagate encoding failures")
    func jsonMessagePropagatesEncodingFailure() {
        do {
            _ = try Message(json: Double.infinity)
            Issue.record("Expected an encoding error for a non-finite JSON number")
        } catch EncodingError.invalidValue(let value, _) {
            #expect(value as? Double == .infinity)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}
