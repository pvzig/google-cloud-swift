import Foundation

public actor DefaultProvider: Provider {
    public static let shared = DefaultProvider()

    private var providerTask: Task<any Provider, Error>?
    private var overrideProvider: (any Provider)?

    public func createSession(scopes: [Scope]) async throws -> Session {
        let provider = try await resolvedProvider()
        return try await provider.createSession(scopes: scopes)
    }

    public var quotaProjectID: String? {
        get async {
            guard let provider = try? await resolvedProvider() else {
                return nil
            }
            return await provider.quotaProjectID
        }
    }

    public func shutdown() async throws {
        // Clear the cached state first so a failed resolution task can't make
        // shutdown (and therefore bootstrap) unrecoverable.
        let providerTask = self.providerTask
        let overrideProvider = self.overrideProvider
        self.providerTask = nil
        self.overrideProvider = nil

        if let providerTask, let provider = try? await providerTask.value {
            try await provider.shutdown()
        }
        if let overrideProvider {
            try await overrideProvider.shutdown()
        }
    }

    func use(_ provider: any Provider) async throws {
        try await shutdown()
        overrideProvider = provider
    }

    private func resolvedProvider() async throws -> any Provider {
        if let overrideProvider {
            return overrideProvider
        }
        if let providerTask {
            do {
                return try await providerTask.value
            } catch {
                // A Task memoizes its thrown error; clearing it lets the next call
                // retry instead of caching a transient failure (e.g. a credentials
                // file that was still being written) for the process lifetime.
                if self.providerTask == providerTask {
                    self.providerTask = nil
                }
                throw error
            }
        }

        let task = Task { try Self.resolveProvider() }
        providerTask = task
        do {
            return try await task.value
        } catch {
            if providerTask == task {
                providerTask = nil
            }
            throw error
        }
    }

    private nonisolated static func resolveProvider() throws -> any Provider {
        switch try loadADC() {
        case .contents(let data):
            return try provider(from: data)
        case .fallbackToMetadataServer:
            return MDSProvider()
        }
    }

    /// Shared factory for credential JSON, used both for top-level ADC and for
    /// the `source_credentials` of impersonated credentials. Parsing performs no
    /// network calls: malformed JSON and private keys are permanent credential
    /// failures, so they must not escape as retryable UNKNOWN transport errors.
    nonisolated static func provider(from data: Data) throws -> any Provider {
        do {
            let type = try JSONDecoder().decode(CredentialType.self, from: data).type
            switch type {
            case "authorized_user":
                return try UserAccountProvider(
                    credentials: JSONDecoder().decode(UserAccountCredentials.self, from: data))
            case "service_account":
                return try ServiceAccountProvider(
                    credentials: JSONDecoder().decode(ServiceAccountKey.self, from: data))
            case "impersonated_service_account":
                return try ImpersonatedProvider(credentialsData: data)
            case "external_account":
                return ExternalAccountProvider()
            default:
                throw CredentialsError.unsupported("unknown credential type '\(type)'")
            }
        } catch let error as CredentialsError {
            throw error
        } catch {
            throw CredentialsError.decoding(error)
        }
    }

    private nonisolated static func loadADC() throws -> ADCContents {
        if let path = ProcessInfo.processInfo.environment[
            AuthConstants.googleApplicationCredentialsVariable]
        {
            let url = URL(fileURLWithPath: path)
            do {
                return .contents(try Data(contentsOf: url))
            } catch {
                throw CredentialsError.loading(
                    "\(path). This file name was found in \(AuthConstants.googleApplicationCredentialsVariable). Verify it points to a valid credentials file."
                )
            }
        }

        guard let home = ProcessInfo.processInfo.environment["HOME"] else {
            return .fallbackToMetadataServer
        }

        let url = URL(fileURLWithPath: home).appending(
            path: ".config/gcloud/application_default_credentials.json")
        guard FileManager.default.fileExists(atPath: url.path) else {
            return .fallbackToMetadataServer
        }
        return .contents(try Data(contentsOf: url))
    }
}

public enum AuthorizationSystem {
    public static func bootstrap(_ provider: any Provider) async throws {
        try await DefaultProvider.shared.use(provider)
    }
}

private enum ADCContents {
    case contents(Data)
    case fallbackToMetadataServer
}

struct CredentialType: Decodable {
    let type: String
}
