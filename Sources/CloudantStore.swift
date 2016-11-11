import Foundation
import CouchDB
import SwiftyJSON
import KituraNet
import GeneratedSwiftServer

public class CloudantStore: Store {
    struct CloudantStoreID: ModelID {
        let value: String

        init(_ id: Any) throws {
            switch id {
            case let string as String: value = string
            default: value = String(describing: id)
            }
        }

        var description: String {
            return value
        }

        func convert(to: PropertyDefinition.PropertyType) -> Any? {
            switch to {
            case .string: return value
            case .number: return Int(value) ?? Double(value)
            case .object: return value
            default: return nil
            }
        }
    }
    public static func ID(_ id: Any) throws -> ModelID {
        return try CloudantStoreID(id)
    }

    let client: CouchDBClient

    public init(_ connectionProperties: ConnectionProperties) {
        client = CouchDBClient(connectionProperties: connectionProperties)
    }

   private static func mergeDictionary(_ dest: inout [String:Any], merge: [String:Any]) {
        for (key, value) in merge {
            dest[key] = value
        }
    }
 
    public func deleteAll(type: Model.Type, callback: @escaping ErrorCallback) {
        let databaseName = String(describing: type).lowercased()
        client.deleteDB(databaseName) { error in
            if let error = error, error.code != HTTPStatusCode.notFound.rawValue {
                callback(.internalError(error.localizedDescription))
            } else {
                callback(nil)
            }
        }
    }

    private func database(_ type: Model.Type) -> Database {
        let databaseName = String(describing: type).lowercased()
        // TODO -- should probably cache some of this
        client.createDB(databaseName) { database, error in
            // TODO ignore?
        }
        return client.database(databaseName) // NOTE(tunniclm): this assumes database() is cheap to call
    }

    // throws:
    //   StoreError.idInvalid(id) - if the id provided is not compatible
    // passes to callback:
    // * resultEntity  - the matching entity containing key-value pairs for the
    //                   properties of the document, plus an additional "id" property of
    //                   type CloudantStoreID encapsulating the docid.
    //                   Will be nil if no matching document is found, or if an error occurred.
    // * error - the error that occurred, or nil if no error occurred.
    //           Error types:
    //           * StoreError.notFound(id) - if no entity with the provided id was found in the Store
    //           * StoreError.storeUnavailable(reason) - if the Store is not in a ready state to service queries
    //           * StoreError.internalError - if there is a logic error
    public func findOne(type: Model.Type, id: ModelID, callback: @escaping EntityCallback) throws {
        guard let cloudantID = id as? CloudantStoreID else {
            // TODO(tunniclm): This failure path may go away if Store
            // is made generic over ModelID
            throw StoreError.idInvalid(id)
        }
        findOne_(type: type, id: cloudantID) { result, error in
            callback(result?.0, error)
        }
    }

    private func findOne_(type: Model.Type, id: CloudantStoreID, callback: @escaping (([String:Any], String)?, StoreError?) -> Void) {
        database(type).retrieve(id.value) { maybeDocument, error in
            if let error = error {
                switch error.code {
                case HTTPStatusCode.notFound.rawValue:
                    callback(nil, .notFound(id))
                case Database.InternalError:
                    callback(nil, .storeUnavailable(error.localizedDescription))
                default:
                    callback(nil, .internalError(error.localizedDescription))
                }
                return
            }

            guard let document = maybeDocument?.object as? [String:Any] else {
                let message = (maybeDocument?.object == nil ? "No data" : "Unexpected type");
                callback(nil, .internalError("\(message) in response from Kitura-CouchDB"))
                return
            }

            guard let docid = document["_id"] as? String else {
                let message = (document["_id"] == nil ? "Missing" : "Unexpected type for")
                callback(nil, .internalError("\(message) docid in document"))
                return
            }

            guard let rev = document["_rev"] as? String else {
                let message = (document["_rev"] == nil ? "Missing" : "Unexpected type for")
                callback(nil, .internalError("\(message) rev in document"))
                return
            }

            do {
                let cloudantID = try CloudantStoreID(docid)

                var entity: [String:Any] = [:]
                entity["id"] = cloudantID
                document.forEach { if !$0.hasPrefix("_") { entity[$0] = $1 } }
                callback((entity, rev), nil)
            } catch StoreError.idInvalid {
                callback(nil, .internalError("Invalid id returned by Kitura-CouchDB"))
            } catch {
                callback(nil, .internalError(String(describing: error)))
            }
        }
    }

    // passes to callback:
    // * resultEntites - an array of entities found in the Store. Each entity contains key-value pairs
    //                   for the properties of the document, plus an additional "id" property of
    //                   type CloudantStoreID encapsulating the docid.
    //                   Will be an empty array if no matching documents are found, or if an error occurred.
    // * error - the error that occurred, or nil if no error occurred.
    //           Error types:
    //           * StoreError.storeUnavailable(reason) - if the Store is not in a ready state to service queries
    //           * StoreError.internalError - if there is a logic error
    public func findAll(type: Model.Type, callback: @escaping EntitiesCallback) {
        database(type).retrieveAll(includeDocuments: true) { maybeDocuments, error in
            if let error = error {
                if error.code == Database.InternalError {
                    callback([], .storeUnavailable(error.localizedDescription))
                } else {
                    callback([], .internalError(error.localizedDescription))
                }
                return
            }

            guard let documents = maybeDocuments?.object as? [String:Any] else {
                let message = (maybeDocuments?.object == nil ? "No data" : "Unexpected type");
                callback([], .internalError("\(message) in response from Kitura-CouchDB"))
                return
            }

            guard var results = documents["rows"] as? [[String:Any]] else {
                let message = (documents["rows"] == nil ? "Missing document container" : "Unexpected type for document container")
                callback([], .internalError("\(message) retrieving documents with Kitura-CouchDB"))
                return
            }

            do {
                results = try results.map {
                    guard let document = $0["doc"] as? [String:Any] else {
                        throw StoreError.internalError("Missing body for document")
                    }
                    guard let docid = document["_id"] as? String else {
                        let message = (document["_id"] == nil ? "Missing" : "Unexpected type for")
                        throw StoreError.internalError("\(message) docid in document")
                    }
                    let cloudantID = try CloudantStoreID(docid)
                    var result: [String:Any] = [:]
                    result["id"] = cloudantID
                    document.forEach { if !$0.hasPrefix("_") { result[$0] = $1 } }
                    return result
                }
                callback(results, nil)
            } catch let error as StoreError {
                callback([], error)
            } catch {
                callback([], StoreError.internalError(String(describing: error)))
            }
        }
    }

    // throws:
    //   StoreError.idInvalid(id) - if an id is provided and it is not compatible
    // passes to callback:
    // * resultEntity - the entity with an additional key "id" and value of type CloudantStoreID encapsulating
    //                  the docid the entity was stored against. Will be nil if, and only if, there was an error.
    // * error - the error that occurred or nil if no error occurred.
    //           If an error occurs, the document will not be created (exception: if it is an .internalError then
    //           the document may still be created).
    //           Error types:
    //           * StoreError.idConflict(id) - if an entity already exists with the provided id
    //           * StoreError.storeUnavailable(reason) - if the Store is not in a ready state to service queries
    //           * StoreError.internalError - if there is a logic error
    // TODO(tunniclm): Provide a mechanism for differentiating between .internalErrors that prevented document creation,
    //                 versus .internalErrors where the document was created but couldn't be interpreted.
    public func create(type: Model.Type, id: ModelID?, entity: [String:Any], callback: @escaping EntityCallback) throws {
        var modifiedEntity = entity
        modifiedEntity.removeValue(forKey: "id")
        if let id = id {
            guard let cloudantID = id as? CloudantStoreID else {
                // TODO(tunniclm): This failure path may go away if Store
                // is made generic over ModelID
                throw StoreError.idInvalid(id)
            }
            modifiedEntity["_id"] = cloudantID.value
        }
        database(type).create(JSON(modifiedEntity)) { _, rev, maybeDocument, error in
            if let error = error {
                switch error.code {
                case Database.InternalError:
                    callback(nil, .storeUnavailable(error.localizedDescription))
                case HTTPStatusCode.conflict.rawValue:
                    if let conflictingID = id as? CloudantStoreID {
                        callback(nil, .idConflict(conflictingID))
                    } else {
                        callback(nil, .internalError(error.localizedDescription))
                    }
                default:
                    callback(nil, .internalError(error.localizedDescription))
                }
                return
            }

            guard let document = maybeDocument?.object as? [String:Any] else {
                let message = (maybeDocument?.object == nil ? "No data" : "Unexpected type");
                callback(nil, .internalError("\(message) in response from Kitura-CouchDB"))
                return
            }

            guard let stringID = document["id"] as? String else {
                // NOTE(tunniclm): CouchDB should always provide a string id when
                // creating a document
                callback(nil, .internalError("No valid ID in response from Kitura-CouchDB"))
                return
            }

            guard let cloudantID = try? CloudantStoreID(stringID) else {
                callback(nil, .internalError("Retrieved ID (\(stringID)) is not valid"))
                return
            }

            var result = entity
            result["id"] = cloudantID
            callback(result, nil)
        }
    }

    // throws:
    //   StoreError.idInvalid(id) - if an id is provided and it is not compatible
    // passes to callback:
    // * resultEntity - the entity with an additional key "id" and value of type CloudantStoreID encapsulating
    //                  the docid the entity was stored against. Will be nil if, and only if, there was an error.
    // * error - the error that occurred or nil if no error occurred.
    //           If an error occurs, the document will not be updated (exception: if it is an .internalError then
    //           the document may still be updated).
    //           Error types:
    //           * StoreError.notFound(id) - if no entity with the provided id was found in the Store
    /// ((idConflict never thrown since we don't support changing ids,
    //    throws .internalError at the moment -- TODO fix that!
    //           * StoreError.idConflict(id) - if changing the id of the entity and an entity already exists with the provided id
    /// ))
    //           * StoreError.storeUnavailable(reason) - if the Store is not in a ready state to service queries
    //           * StoreError.internalError - if there is a logic error
    // TODO(tunniclm): Provide a mechanism for differentiating between .internalErrors that prevented document creation,
    //                 versus .internalErrors where the document was created but couldn't be interpreted.
    public func update(type: Model.Type, id: ModelID, entity: [String:Any], callback: @escaping EntityCallback) throws {
        guard let cloudantID = id as? CloudantStoreID else {
            throw StoreError.idInvalid(id)
        }
        let newId = try entity["id"].map { try CloudantStoreID($0) }

        // TODO since we only want the revision, could we just use a HEAD request?
        // I don't think Kitura-CouchDB supports this right now though
        findOne_(type: type, id: cloudantID) { result, error in
            if let error = error {
                callback(nil, error)
                return
            }

            guard let (existingEntity, existingRev) = result else {
                // NOTE(tunniclm): findOne_() returns a result or an error so we
                // should not get here.
                assert(false)
                callback(nil, .internalError("No error and no result, should have one or the other"))
                return
            }

            var modifiedEntity = existingEntity
            CloudantStore.mergeDictionary(&modifiedEntity, merge: entity)
            modifiedEntity.removeValue(forKey: "id")
            self.update_(type: type, id: cloudantID, rev: existingRev, newId: newId, entity: modifiedEntity, callback: callback)
        }
    }

    // NOTE(tunniclm): Preconditions for this function are:
    // - existing document with valid rev and id read from db
    // - partial updates already merged into the existing entity
    // - "id" is removed from entity (if exists) and passed as newId
    private func update_(type: Model.Type, id: CloudantStoreID, rev: String, newId: CloudantStoreID?, entity: [String:Any], callback: @escaping ([String:Any]?, StoreError?) -> Void) {
        if newId != nil && newId!.value != id.value {
            // NOTE(tunniclm): Need to change the id of the document
            callback(nil, .internalError("Changing id is not supported"))
            return
        }

        // NOTE(tunniclm): No id change, just update existing document
        let json = JSON(entity)
        self.database(type).update(id.value, rev: rev, document: json) { updatedRev, maybeDocument, error in
            if let error = error {
                switch error.code {
                case Database.InternalError:
                    callback(nil, .storeUnavailable(error.localizedDescription))
                case HTTPStatusCode.notFound.rawValue:
                    callback(nil, .notFound(id))
                case HTTPStatusCode.conflict.rawValue:
                    // TODO Check for stale revision
                    callback(nil, .idConflict(id))
                default:
                    callback(nil, .internalError(error.localizedDescription))
                }
                return
            }

            guard let document = maybeDocument?.object as? [String:Any] else {
                let message = (maybeDocument?.object == nil ? "No data" : "Unexpected type");
                callback(nil, .internalError("\(message) in response from Kitura-CouchDB"))
                return
            }

            guard let stringID = document["id"] as? String else {
                // NOTE(tunniclm): CouchDB should always provide a string id when
                // creating a document
                callback(nil, .internalError("No valid ID in response from Kitura-CouchDB"))
                return
            }

            guard let id = try? CloudantStoreID(stringID) else {
                callback(nil, .internalError("Retrieved ID (\(stringID)) is not valid"))
                return
            }

            // TODO(tunniclm): Should probably try to get the revision
            // we were returned instead of the latest, since the document
            // may have been deleted or updated to a new ID between the update()
            // and retrieve(). Doing this would be more robust, and reduce the
            // chance of .notFound/.internalError
            self.findOne_(type: type, id: id) { result, error in
                if case .notFound? = error {
                    callback(nil, .internalError("stale data reading updated document"))
                    return
                }
                callback(result?.0, error)
            }
        }
    }

    // throws:
    //   StoreError.idInvalid(id) - if the provided id is not compatible
    // passes to callback:
    // * resultEntity - the entity with an additional key "id" and value of type CloudantStoreID encapsulating
    //                  the docid the entity was stored against. Will be nil if, and only if, there was an error.
    // * error - the error that occurred, or nil if no error occurred. If an error occurred, the document will
    //           not be deleted.
    //           Error types:
    //           * StoreError.notFound(id) - if no entity with the provided id was found in the Store
    //           * StoreError.storeUnavailable(reason) - if the Store is not in a ready state to service queries
    //           * StoreError.internalError - if there is a logic error
    public func delete(type: Model.Type, id: ModelID, callback: @escaping EntityCallback) throws {
        // TODO(tunniclm): Generics might help with this type constraint (make parameter "id: CloudantStoreID").
        guard let cloudantID = id as? CloudantStoreID else {
            // TODO(tunniclm): This failure path may go away if Store
            // is made generic over ModelID
            throw StoreError.idInvalid(id)
        }
        // NOTE(tunniclm): Need to fetch revision identifier
        // TODO Could use a lighter-weight call (eg HEAD) if Kiture-CouchDB supported it
        findOne_(type: type, id: cloudantID) { result, error in
            if let error = error {
                callback(nil, error)
                return
            }

            guard let (entity, rev) = result else {
                // NOTE(tunniclm): findOne_() returns a result or an error so we
                // should not get here.
                assert(false)
                callback(nil, .internalError("No error and no result, should have one or the other"))
                return
            }

            self.database(type).delete(cloudantID.value, rev: rev, failOnNotFound: true) { error in
                if let error = error {
                    switch error.code {
                    case Database.InternalError:
                        callback(nil, .storeUnavailable(error.localizedDescription))
                    case HTTPStatusCode.notFound.rawValue:
                        // NOTE(tunniclm): Someone must have deleted the
                        // record since we read it from the DB
                        callback(nil, .notFound(cloudantID))
                    case HTTPStatusCode.conflict.rawValue:
                        // TODO should we retry?
                        callback(nil, .internalError(error.localizedDescription))
                    default:
                        callback(nil, .internalError(error.localizedDescription))
                    }
                    return
                }

                callback(entity, nil)
            }
        }
    }
}
