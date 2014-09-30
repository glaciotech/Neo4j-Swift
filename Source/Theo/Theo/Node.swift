//
//  Node.swift
//  Theo
//
//  Created by Cory D. Wiles on 9/19/14.
//  Copyright (c) 2014 Theo. All rights reserved.
//

import Foundation

struct NodeMeta: Printable {
    
    let extensions: [String: AnyObject]      = [String: AnyObject]()
    let page_traverse: String                = ""
    let labels: String                       = ""
    let outgoing_relationships: String       = ""
    let traverse: String                     = ""
    let all_typed_relationships: String      = ""
    let property: String                     = ""
    let all_relationships: String            = ""
    let node_self: String                    = ""
    let outgoing_typed_relationships: String = ""
    let properties: String                   = ""
    let incoming_relationships: String       = ""
    let incoming_typed_relationships: String = ""
    let create_relationship: String          = ""
    let data: [String: AnyObject]            = [String: AnyObject]()

    func nodeID() -> String {

        let pathComponents: Array<String> = self.node_self.componentsSeparatedByString("/")

        return pathComponents.last!
    }
    
    init(dictionary: Dictionary<String, AnyObject>!) {
        
        for (key: String, value: AnyObject) in dictionary {
            
            switch key {
            case "extensions":
                self.extensions = value as Dictionary
            case "page_traverse":
                self.page_traverse = value as String
            case "labels":
                self.labels = value as String
            case "outgoing_relationships":
                self.outgoing_relationships = value as String
            case "traverse":
                self.traverse = value as String
            case "all_relationships":
                self.all_relationships = value as String
            case "property":
                self.property = value as String
            case "all_relationships":
                self.all_relationships = value as String
            case "self":
                self.node_self = value as String
            case "outgoing_typed_relationships":
                self.outgoing_typed_relationships = value as String
            case "properties":
                self.properties = value as String
            case "incoming_relationships":
                self.incoming_relationships = value as String
            case "incoming_typed_relationships":
                self.incoming_typed_relationships = value as String
            case "create_relationship":
                self.create_relationship = value as String
            case "data":
                self.data = value as Dictionary
            default:
                ""
            }
        }
    }
    
    var description: String {
        return "Extensions: \(self.extensions), page_traverse \(self.page_traverse), labels \(self.labels), outgoing_relationships \(self.outgoing_relationships), traverse \(self.traverse), all_typed_relationships \(self.all_typed_relationships), all_typed_relationships \(self.all_typed_relationships), property \(self.property), all_relationships \(self.all_relationships), self \(self.node_self), outgoing_typed_relationships \(self.outgoing_typed_relationships), properties \(self.properties), incoming_relationships \(self.incoming_relationships), incoming_typed_relationships \(self.incoming_typed_relationships), create_relationship \(self.create_relationship), data \(self.data), nodeID \(self.nodeID())"
    }
}

class Node {

    // MARK: Private Setters and Public Getters

    private (set) var nodeData: [String:AnyObject] = [String:AnyObject]()
    private (set) var labels: [String] = [String]()

    // MARK: Public Properties
    
    var meta: NodeMeta?
    
    // MARK: Constructors
    
    /// Designated Initializer
    ///
    /// :param: Dictionary<String,AnyObject>? data
    /// :returns: Node
    required init(data: Dictionary<String,AnyObject>?) {
        
        if let dictionaryData: [String:AnyObject] = data {

            self.meta = NodeMeta(dictionary: dictionaryData)
            
            if let metaForNode = self.meta {
                self.nodeData = metaForNode.data
            }
        }
    }
    
    /// Convenience initializer
    ///
    /// calls init(data:) with the param value as nil
    ///
    /// :returns: Node
    convenience init() {
        self.init(data: nil)
    }
    
    /// Gets a specified property for the Node
    ///
    /// :param: String propertyName
    /// :returns: AnyObject?
    func getProp(propertyName: String) -> AnyObject? {

        if let object: AnyObject = self.nodeData[propertyName] {
            return object
        }
        
        return nil
    }
    
    /// Sets the property for the relationship
    ///
    /// :param: String propertyName
    /// :param: String propertyValue
    /// :returns: Void
    func setProp(propertyName: String, propertyValue: String) -> Void {
        
        var objectValue: AnyObject = propertyValue as AnyObject
        
        self.nodeData[propertyName] = objectValue
    }
    
    /// Adds label to array of labels for the node
    ///
    /// :param: String label
    /// :returns: Void
    func addLabel(label:String) -> Void {
        self.labels.append(label)
    }
    
    /// Returns whether or not the nodeData is empty
    ///
    /// This is done by checking for empty keys array
    ///
    /// :returns: Bool
    func isEmpty() -> Bool {
        return self.nodeData.keys.isEmpty
    }

    /// Returns whether the current node has labels
    ///
    /// :returns: Bool
    func hasLabels() -> Bool {
        return self.labels.isEmpty
    }
}

// MARK: - Printable

extension Node: Printable {
    
    var description: String {
        
        var returnString: String = ""
            
        for (key, value) in self.nodeData {
            returnString += "\(key): \(value) "
        }
        
        if let meta: NodeMeta = self.meta {
            returnString += meta.description
        }
            
        return returnString
    }
}


