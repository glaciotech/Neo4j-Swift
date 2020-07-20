import Foundation
import Bolt
@testable import Theo

struct BoltConfig: ClientConfigurationProtocol {
    let hostname: String
    let port: Int
    let username: String
    let password: String
    let encryption: Encryption

    init(pathToFile: String) {

        do {
            let filePathURL = URL(fileURLWithPath: pathToFile)
            let jsonData = try Data(contentsOf: filePathURL)
            let JSON = try JSONSerialization.jsonObject(with: jsonData, options: [])

            let jsonConfig = JSON as! [String:Any]

            self.username  = jsonConfig["username"] as! String
            self.password  = jsonConfig["password"] as! String
            self.hostname  = jsonConfig["hostname"] as! String
            self.port      = jsonConfig["port"] as! Int
            
            if let encrypted = jsonConfig["encrypted"] as? Bool {
                self.encryption = encrypted ? .certificateIsSelfSigned : .unencrypted
            } else {
                self.encryption = .unencrypted
            }

        } catch {

            self.username   = "neo4j"
            self.password   = "neo4j"
            self.hostname   = "localhost"
            self.port       = 7687
            self.encryption = .unencrypted

            print("Using default parameters as configuration parsing failed: \(error)")
        }
    }
}
