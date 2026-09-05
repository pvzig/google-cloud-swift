import AsyncHTTPClient
import Foundation
import NIOCore
import NIOHTTP1

enum AuthHTTP {
    private struct Exchange: Sendable {
        let statusCode: UInt
        let body: Data
    }

    /// Token and metadata responses are small JSON documents; this bounds how
    /// much a misbehaving or hostile endpoint can make the client buffer.
    static let maxResponseBytes = 1 << 20

    /// Credential endpoints are on the critical path of every RPC that needs a
    /// token, so a hung metadata server must not stall callers indefinitely.
    static let requestTimeout = TimeAmount.seconds(30)

    static func getJSON<Response: Decodable>(
        url: URL,
        headers: [String: String] = [:]
    ) async throws -> Response {
        var request = HTTPClientRequest(url: url.absoluteString)
        request.method = .GET
        for (key, value) in headers {
            request.headers.replaceOrAdd(name: key, value: value)
        }
        return try await send(request)
    }

    static func postJSON<RequestBody: Encodable, Response: Decodable>(
        url: URL,
        body: RequestBody,
        bearerToken: String? = nil
    ) async throws -> Response {
        var request = HTTPClientRequest(url: url.absoluteString)
        request.method = .POST
        request.headers.replaceOrAdd(name: "Content-Type", value: "application/json")
        if let bearerToken {
            request.headers.replaceOrAdd(name: "Authorization", value: "Bearer \(bearerToken)")
        }
        request.body = .bytes(try JSONEncoder().encode(body))
        return try await send(request)
    }

    static func postForm<Response: Decodable>(
        url: URL,
        parameters: [String: String]
    ) async throws -> Response {
        try await send(formRequest(url: url, parameters: parameters))
    }

    static func formRequest(url: URL, parameters: [String: String]) -> HTTPClientRequest {
        let body = parameters.keys.sorted().map { key in
            "\(formEncode(key))=\(formEncode(parameters[key] ?? ""))"
        }.joined(separator: "&")

        var request = HTTPClientRequest(url: url.absoluteString)
        request.method = .POST
        request.headers.replaceOrAdd(
            name: "Content-Type", value: "application/x-www-form-urlencoded")
        request.body = .bytes(ByteBuffer(string: body))
        return request
    }

    private static func formEncode(_ value: String) -> String {
        value.utf8.map { byte in
            switch byte {
            case 0x30...0x39, 0x41...0x5A, 0x61...0x7A, 0x2D, 0x2E, 0x5F, 0x7E:
                return String(UnicodeScalar(byte))
            case 0x20:
                return "+"
            default:
                let hexadecimal = String(byte, radix: 16, uppercase: true)
                return "%" + (hexadecimal.count == 1 ? "0\(hexadecimal)" : hexadecimal)
            }
        }.joined()
    }

    private static func send<Response: Decodable>(
        _ request: HTTPClientRequest
    ) async throws -> Response {
        let exchange: Exchange
        do {
            exchange = try await executeAndCollect(request)
        } catch let error as CredentialsError {
            throw error
        } catch {
            // Connect failures, DNS failures and timeouts are all worth another
            // attempt; the token cache decides whether to make one.
            throw CredentialsError.message(
                "auth endpoint request failed: \(error)", isTransient: true)
        }

        guard (200..<300).contains(exchange.statusCode) else {
            throw CredentialsError.httpStatus(
                Int(exchange.statusCode), String(decoding: exchange.body, as: UTF8.self))
        }

        do {
            return try JSONDecoder().decode(Response.self, from: exchange.body)
        } catch {
            throw CredentialsError.decoding(error)
        }
    }

    private static func executeAndCollect(_ request: HTTPClientRequest) async throws -> Exchange {
        try await withDeadline(.seconds(30)) {
            // The process-wide client cannot be shut down, so nothing here owns a
            // connection pool that a credential provider would have to tear down.
            let response = try await HTTPClient.shared.execute(request, timeout: requestTimeout)
            return Exchange(
                statusCode: response.status.code, body: try await collect(response.body))
        }
    }

    static func withDeadline<Value: Sendable>(
        _ timeout: Duration,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        try await withThrowingTaskGroup(of: Value.self) { group in
            group.addTask(operation: operation)
            group.addTask {
                try await Task.sleep(for: timeout)
                throw CredentialsError.message("auth endpoint request timed out", isTransient: true)
            }

            defer { group.cancelAll() }
            guard let first = try await group.next() else {
                throw CredentialsError.message("auth endpoint request failed", isTransient: true)
            }
            return first
        }
    }

    private static func collect(_ body: HTTPClientResponse.Body) async throws -> Data {
        do {
            // `readableBytesView` keeps this on NIOCore alone; ByteBuffer's Data
            // bridging lives in NIOFoundationCompat, which Darwin happens to import
            // transitively and Linux does not.
            let buffer = try await body.collect(upTo: maxResponseBytes)
            return Data(buffer.readableBytesView)
        } catch {
            throw CredentialsError.message(
                "auth endpoint response could not be read: \(error)", isTransient: true)
        }
    }
}
