import GRPCCore
import SwiftProtobuf

struct PayloadLimitInterceptor: ClientInterceptor {
    let maximumBytes: Int

    func intercept<Input: Sendable, Output: Sendable>(
        request: StreamingClientRequest<Input>,
        context: ClientContext,
        next: (
            StreamingClientRequest<Input>,
            ClientContext
        ) async throws -> StreamingClientResponse<Output>
    ) async throws -> StreamingClientResponse<Output> {
        let producer = request.producer
        var checkedRequest = request
        checkedRequest.producer = { writer in
            let checkedWriter = PayloadCheckingWriter(base: writer, maximumBytes: maximumBytes)
            try await producer(RPCWriter(wrapping: checkedWriter))
        }
        return try await next(checkedRequest, context)
    }

    static func validate<Input>(_ input: Input, maximumBytes: Int) throws {
        let byteCount: Int
        if let request = input as? Google_Pubsub_V1_PublishRequest {
            // Publishing is the hot path and messages may each hold 10 MB. Use the
            // allocation-free size calculation also used by batching rather than
            // materializing the complete request solely for validation.
            byteCount = request.serializedByteCount
        } else if let message = input as? any SwiftProtobuf.Message {
            byteCount = try message.serializedData().count
        } else {
            return
        }

        guard byteCount <= maximumBytes else {
            throw RPCError(
                code: .invalidArgument,
                message: "serialized request exceeds the \(maximumBytes)-byte Pub/Sub limit"
            )
        }
    }
}

private struct PayloadCheckingWriter<Element: Sendable>: RPCWriterProtocol {
    let base: RPCWriter<Element>
    let maximumBytes: Int

    func write(_ element: Element) async throws {
        try PayloadLimitInterceptor.validate(element, maximumBytes: maximumBytes)
        try await base.write(element)
    }

    func write(contentsOf elements: some Sequence<Element>) async throws {
        let checked = try elements.map { element in
            try PayloadLimitInterceptor.validate(element, maximumBytes: maximumBytes)
            return element
        }
        try await base.write(contentsOf: checked)
    }
}
