import Foundation
import SwiftProtobuf

public typealias Message = Google_Pubsub_V1_PubsubMessage
public typealias ReceivedMessage = Google_Pubsub_V1_ReceivedMessage
public typealias Topic = Google_Pubsub_V1_Topic
public typealias Subscription = Google_Pubsub_V1_Subscription
public typealias Schema = Google_Pubsub_V1_Schema
public typealias Empty = SwiftProtobuf.Google_Protobuf_Empty

extension Google_Pubsub_V1_PubsubMessage {
    public init(
        data: Data = Data(),
        attributes: [String: String] = [:],
        orderingKey: String = ""
    ) {
        self.init()
        self.data = data
        self.attributes = attributes
        self.orderingKey = orderingKey
    }

    /// Creates a message with the string's UTF-8 bytes as its payload.
    public init(
        string: String,
        attributes: [String: String] = [:],
        orderingKey: String = ""
    ) {
        self.init(data: Data(string.utf8), attributes: attributes, orderingKey: orderingKey)
    }

    /// Creates a message by encoding the value as a JSON payload.
    ///
    /// The encoder's configured strategies are used without modification.
    /// Encoding errors propagate to the caller.
    public init(
        json value: some Encodable,
        encoder: JSONEncoder = JSONEncoder(),
        attributes: [String: String] = [:],
        orderingKey: String = ""
    ) throws {
        self.init(
            data: try encoder.encode(value),
            attributes: attributes,
            orderingKey: orderingKey
        )
    }

    public var estimatedByteCount: Int {
        data.count
            + attributes.reduce(0) { partialResult, element in
                partialResult + element.key.utf8.count + element.value.utf8.count
            }
            + orderingKey.utf8.count
    }

    /// Exact protobuf message size used for publisher batching. Pub/Sub's
    /// request limit applies to the serialized wire message, including field
    /// tags and length prefixes, rather than just user data and attributes. This
    /// computes the generated message's wire shape without allocating a second
    /// full copy of the payload merely to ask SwiftProtobuf for its byte count.
    var serializedByteCount: Int {
        var count = 0
        if data.isEmpty == false {
            count += ProtobufSize.lengthDelimitedField(fieldNumber: 1, payloadBytes: data.count)
        }
        for (key, value) in attributes {
            let entryBytes =
                ProtobufSize.requiredStringField(fieldNumber: 1, value: key)
                + ProtobufSize.requiredStringField(fieldNumber: 2, value: value)
            count += ProtobufSize.lengthDelimitedField(fieldNumber: 2, payloadBytes: entryBytes)
        }
        count += ProtobufSize.stringField(fieldNumber: 3, value: messageID)
        if hasPublishTime {
            let timestampBytes =
                ProtobufSize.int64Field(fieldNumber: 1, value: publishTime.seconds)
                + ProtobufSize.int32Field(fieldNumber: 2, value: publishTime.nanos)
                + publishTime.unknownFields.data.count
            count += ProtobufSize.lengthDelimitedField(fieldNumber: 4, payloadBytes: timestampBytes)
        }
        count += ProtobufSize.stringField(fieldNumber: 5, value: orderingKey)
        count += unknownFields.data.count
        return count
    }
}

extension Google_Pubsub_V1_PublishRequest {
    var serializedByteCount: Int {
        messages.reduce(ProtobufSize.stringField(fieldNumber: 1, value: topic)) {
            $0
                + ProtobufSize.lengthDelimitedField(
                    fieldNumber: 2,
                    payloadBytes: $1.serializedByteCount
                )
        } + unknownFields.data.count
    }
}
