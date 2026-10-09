# Overview

# Polaris Tag Spec

Author: [EJ Wang](mailto:ej.wang@snowflake.com)

Part 0 is the main review page. It explains the model and the choices reviewers need to assess.

* Part 1: REST API  
* Part 2: Durable data model  
* Part 3: Design context and future extensions

## 1\. Overview

Polaris tags classify catalog data for discovery and governance. Each supported catalog object or top-level Iceberg column is a **target**. v1 supports five **target kinds**: catalog, namespace, table, view, and column. A **tag definition** has a catalog-wide name, allowed values, and the target kinds it may classify. A **tag assignment** puts one selected value on one target in v1.

Each target can have one direct assignment per tag definition. Assigning that tag again replaces its value.

API requests use names, but Polaris stores IDs. Renaming a tag definition or target does not move an assignment when its ID stays the same. Columns use the containing table ID together with a stable column identity. v1 uses the field ID for Iceberg tables.

A caller can read assignments stored on one target or ask for the final tags that apply after inheritance. A tag applies only to target kinds listed in its definition. An excluded intermediate kind is skipped. It does not stop inheritance to an allowed descendant. When several assignments for the same tag apply, the closest one wins.

Tag-based access decisions are outside this proposal's v1 scope. Tag operations still require authorization, as described in Part 1, Section 7\. An **authorizer** is a built-in or external component that returns allow or deny. A later authorizer could reuse the same definitions, assignments, and inheritance rules.

## 2\. Core model

### 2.1 Tag definition

A **tag definition** gives a classification a reusable name, a controlled value list, and a list of target kinds it may classify.

{  
  "id": "tag-identity-a",  
  "name": "sensitivity",  
  "description": "Data sensitivity level",  
  "values": \["public", "internal", "confidential", "restricted"\],  
  "target-types": \["CATALOG", "NAMESPACE", "TABLE", "VIEW", "COLUMN"\],  
  "version": "rev-a"  
}

The name is unique within the catalog. id is a read-only string for comparing tag identity, including across rename. Requests still address tags by name.

description is optional on creation and in the returned definition. values is required and non-empty. Omitting target-types on creation selects all five supported kinds. The response always returns the stored non-empty set.

Allowed values are plain user-defined strings. They do not have their own IDs. Replacing one string with another is a removal and an addition. Existing assignments that use a removed value remain readable until they are replaced or unassigned.

target-types uses the same five target kinds as the assignment API. It controls where the tag can be assigned and where it can appear through inheritance. v1 fixes the list at creation because a later change could reclassify many existing targets.

For example, a definition with target-types \= \[NAMESPACE, COLUMN\] may be assigned to a namespace or column. A namespace assignment applies to descendant columns, but not to intermediate tables.

version is an opaque string returned with the definition. Clients return it unchanged for update or rename to reject stale writes. It describes the definition state, while id identifies the tag.

Update replaces the complete editable definition, description and values. Rename is a separate operation, so it cannot be combined atomically with those edits.

### 2.2 Tag assignment

A **tag assignment** records one selected value for one tag definition on one target. The tag name and target query identify the assignment:

PUT /tags/sensitivity/assignments?target-type=COLUMN\&namespace=sales\&target-name=customers\&column=email

The request body contains only the selected value:

{ "values": \["confidential"\] }

Routes are relative to the base path in Section 5\.

The server rejects an assignment when the target kind is not in the definition's target-types list.

For one target and tag definition, v1 stores at most one assignment. Assigning the same tag again replaces the selected value. A target can have assignments from different tag definitions.

The API carries selected values in a list named values, but v1 accepts exactly one item. The list shape and assignment identity leave room for later multi-value support. Part 3, Section 5.2 describes the remaining rules and storage changes. Key-only and multi-value tags are outside v1.

Catalogs, namespaces, tables, and views have Polaris entity IDs. Columns do not. A column assignment stores the containing table ID and the Iceberg field ID in separate fields. The server resolves the requested column name against the current Iceberg schema before storing the field ID.

### 2.3 Reading tags and inheritance

A **direct tag assignment** is stored on the target being read. An **inherited tag assignment** is stored on a parent but applies to the target being read.

getObjectTags supports two views:

| View | What it returns |
| :---- | :---- |
| **Direct** (direct) | Assignments stored on the target. Omitting view returns this view. |
| **Effective** (effective) | The final tag set after reading the target and its parents. |

An effective read returns a tag only when the queried target kind appears in target-types. The closest assignment that applies is the **winning assignment**. Values from different hierarchy levels do not accumulate.

For a definition with target-types \= \[NAMESPACE, COLUMN\]:

namespace sales       \-\> direct assignment  
  table orders        \-\> no effective tag  
    column ssn         \-\> inherits from namespace sales

The table is skipped. It is not an inheritance barrier. A child namespace remains eligible because NAMESPACE is listed.

Each winning result says two things:

* apply-method says how the tag applies, such as DIRECT or INHERITED.  
* assigned-at identifies the target that stores the winning assignment.

A **reverse lookup** starts from a tag definition and returns targets with their own direct assignment. It does not expand inherited results.

v1 has no negative assignment that blocks inheritance. If the child kind is allowed, it inherits the closest value unless it has its own assignment.

## 3\. What v1 delivers

### Supported operations

* Create catalog-scoped tag definitions with allowed values and allowed target kinds.  
* Assign one selected value to any supported target kind listed by the definition.  
* Use one definition for namespace and column targets so a namespace assignment reaches columns without applying to intermediate tables.  
* Read direct assignments or the final classifications that apply after inheritance.  
* Find targets with a direct assignment and optionally filter by selected value.  
* Change allowed values for future writes without rewriting existing assignments.  
* Keep assignments through renames of definitions or targets that preserve the same IDs.  
* Rename a definition separately from other editable-field changes.  
* Compare returned IDs to distinguish a renamed tag from a same-name replacement.  
* Permanently delete a tag definition after handling assignments that still use it.

### Outside the current scope

* Integrate tags into data access decisions.  
* Support key-only tags, unrestricted free-form values, typed values, or multiple selected values.  
* Rename an allowed value and automatically migrate existing assignments.  
* Change target-types after the definition is created.  
* Tag nested fields or columns in GENERIC\_TABLE tables.  
* Tag columns in views. Whole Iceberg views are supported.  
* Assign tags across catalogs, to a realm, or to unsupported target kinds.  
* Tag a namespace, table, view, or column when a namespace name contains the namespace separator character. Part 1 explains the shared target-query encoding for reads and writes.  
* Tag many current or future targets from a rule.  
* Return every descendant that only inherits a tag in reverse lookup.  
* Block inheritance with a negative or null assignment.  
* Restore or load a permanently deleted tag definition.  
* Define one standard asynchronous cleanup mechanism for orphaned tag assignments and similar relationship records.

Whole-view assignments use the view entity ID. Supporting view columns later would require their own identity and replacement rules.

Part 3 discusses possible later extensions. Those examples show what a later design could add. They are not commitments.

When a definition still has assignments, detach-all=true asks Polaris to remove the assignments and the definition.

Each effective read builds its response from target names, hierarchy, definitions, and assignments as they existed together during that request. A Tag write makes all its changes visible together. A write that aborts before taking effect leaves those facts unchanged. A lost response does not tell the client whether the write took effect.

With detach-all=true, the definition and all its assignments disappear from Tag reads together. Stored assignment rows may be cleaned up later. An **orphaned tag assignment** refers to a permanently removed definition, target entity, or Iceberg field. Reads hide these rows, including rows left by overlapping assignment and deletion requests.

The proposed built-in JDBC deletion flow also attempts **best-effort cleanup** when a target is permanently removed. Best-effort means cleanup failure does not fail target deletion. Any remaining assignment is hidden as an orphaned tag assignment. Other implementations may use a different cleanup mechanism.

## 4\. Key design decisions

Parts 1 and 2 contain the detailed API and durable-model rules.

### 4.1 Tag definitions

| Decision | v1 choice | Why |
| :---- | :---- | :---- |
| First-class definition | A tag definition is a Polaris TAG entity under a catalog. | Assignments refer to its entity ID, and permissions can be checked on the definition. |
| Catalog scope | The name is unique within the catalog. | Catalog is the existing resource and authorization boundary for most Polaris data. |
| Public definition identity | Return an opaque, read-only ID stable across rename and update. Requests remain name-based. | Clients can correlate a renamed definition or distinguish a same-name replacement. |
| Required allowed values | Every definition has a non-empty list of allowed strings. | v1 is a controlled classification system. |
| Per-definition target kinds | Creation defaults to all five kinds. An explicit subset remains supported. Persist the concrete set and keep it immutable. | Simple creation stays concise, while a definition can still restrict direct assignment and inheritance. |
| Allowed-value identity | An allowed value is a string, not an identified object. | v1 does not add value-level rename, metadata, permissions, or migration. |
| Update conflict check | Each definition has an opaque string token. | Matching the current token prevents stale writes without requiring a numeric backend version. |

### 4.2 Tag assignments

| Decision | v1 choice | Why |
| :---- | :---- | :---- |
| Supported target kinds | Catalog, namespace, whole table, whole Iceberg view, and top-level Iceberg table column. | These cover the v1 discovery and governance use cases. |
| Definition-level restriction | A direct assignment is rejected when its target kind is not listed in target-types. | A definition should not classify a kind it excludes. |
| One selected value | A target can have at most one direct assignment for a tag, with exactly one value. | Reassigning the tag gives one clear current classification. |
| ID-based binding | Assignments store definition and target IDs rather than names. | Rename does not move or remove an assignment. |
| Column identity | A column assignment uses table identity plus stable column identity. v1 uses top-level field IDs for Iceberg tables. | Rename preserves the assignment. A same-name replacement does not inherit it. |
| Same catalog | The definition and target must belong to the same catalog. | v1 does not define cross-catalog identity or authorization. |

### 4.3 Reading tags and finding tagged targets

| Decision | v1 choice | Why |
| :---- | :---- | :---- |
| Direct and effective views | getObjectTags supports direct and effective. Omitting view returns direct. | One operation and one response shape keep the API small. |
| Where the tag applies | A tag applies only to target kinds in target-types. An excluded intermediate kind does not block an allowed descendant. | namespace plus column can classify columns without classifying tables. |
| Closest assignment wins | For columns: column, table, nearest namespace, catalog. For views: view, nearest namespace, catalog. The closest eligible assignment wins. | A more specific classification overrides a broader one. |
| Reverse lookup | listObjectsByTag returns direct assignments only and can filter by one exact value. | It avoids expanding one parent assignment into every descendant. |
| Consistent reverse result | One item must describe one direct assignment that actually existed. | A target cannot be paired with a value from another state. |
| Result origin | Each target-read result includes apply-method and assigned-at. | A caller can see how the tag applies and where to change it. |

### 4.4 Security boundaries

| Decision | v1 choice | Why |
| :---- | :---- | :---- |
| Effective-read visibility | Permission to read a target's properties reveals the complete tags that apply to it. The server does not separately authorize parent sources or each returned definition. | Filtering would return an incomplete classification for the queried target. |
| Reverse-lookup visibility | Check TAG\_READ on the named definition, then property-read permission on each returned target. Columns use their containing table. | A readable definition must not disclose targets the caller cannot read. |
| Rename authorization | Require drop permission on the original definition and create permission on its catalog. | Renaming removes the old name and introduces a new one. |
| Bulk assignment cleanup | detach-all=true requires permission to delete the definition and permission to remove assignments of that tag. The server does not check every attached target. | Removing all assignments changes classification across the catalog. |

### 4.5 Changes and deletion

| Decision | v1 choice | Why |
| :---- | :---- | :---- |
| Editable definition replacement | Update requires both description and values. Explicit null clears description. | Clients state the complete desired editable state. |
| Separate rename | A dedicated operation changes the name with a version check. | Rename has an explicit source and destination. Other definition edits remain separate. |
| Definition-write retries | When enabled, live keys recognize create, update, and rename success under current authorization. Create/update use name lookup. Rename can recover the original identity after later renames. | Recognition avoids repeating a completed mutation. It requires the original tag to survive. Part 1 defines lookup and no-op limits. |
| Retry responses | Recognized create/update retries return the current definition. Rename returns no body. | Acknowledging success does not restore earlier fields or names. |
| Assignment retries | Assign/unassign use ordinary replacement/removal semantics without key-based success recognition. | Internal concurrency checks protect one execution. They do not identify a delayed retry. |
| Allowed-value changes | Existing assignments remain readable. Later writes use the current list. | A definition update should not rewrite stored classifications. |
| Overlapping changes | Readers see all the changes from a Tag write or none of them. Reads hide any orphan rows left by overlapping assignment and deletion requests. | Callers must not see a partly completed update or deletion. |
| Definition deletion | v1 permanently deletes the definition. Live assignments block normal deletion. Confirmed orphaned or soft-dropped-target assignments do not. detach-all=true removes all assignments. | v1 has no restore contract. |
| Target lifecycle | Soft drop keeps assignments while the definition exists. Deleting that definition prevents restoration of its assignments. Permanent removal may leave orphaned assignments, which normal reads and reverse lookup hide. | An assignment never moves to a new same-name target. |
| Built-in deletion cleanup | The proposed built-in JDBC flow synchronously tries to remove assignments while it still has the target ID. Failure does not fail target deletion. | Reliable async cleanup needs a shared design across grants, Policy mappings, and tags. |

### 4.6 API shape

| Decision | v1 choice | Why |
| :---- | :---- | :---- |
| Assignment relationship | The tag is named in /tags/{tag-name}/assignments. Query parameters identify one target. | The URI identifies the relation without a separate assignment ID. |
| Assignment verbs | PUT creates or replaces. DELETE removes without a body. | The same target query works for writes and target reads. |
| Target address | Required target-type, with namespace, target-name, and column as appropriate. | Explicit kinds distinguish tables and views and prevent implicit catalog operations. |
| Invalid target request | Fields that do not match the declared kind return 400. | Malformed addressing must not select another target or bulk operation. |
| Target query encoding | Iceberg namespace query encoding and one URI-encoding step. Other names are ordinary query values. | Preserve established encoding while recording its limits. |

## 5\. REST surface

The Tag API uses the catalog extension base URL:

/api/catalog/polaris/v1/{prefix}

This keeps Tags within the catalog extension API layout. Routes below are relative to this base.

prefix selects the catalog using the existing Polaris catalog-extension routing semantics. In Polaris, its value is the catalog name encoded as one path parameter. Namespace and target names are supplied separately.

| Operation | Relative route | Purpose |
| :---- | :---- | :---- |
| createTag | POST /tags | Create a definition. |
| listTags | GET /tags | List definition names and IDs. |
| loadTag | GET /tags/{tag-name} | Load one definition. |
| updateTag | PUT /tags/{tag-name} | Replace description and values. |
| renameTag | POST /tags/rename | Rename a definition with a version check. |
| dropTag | DELETE /tags/{tag-name} | Delete a definition, optionally with detach-all=true. |
| assignTag | PUT /tags/{tag-name}/assignments?target-type=... | Create or replace one direct assignment. |
| unassignTag | DELETE /tags/{tag-name}/assignments?target-type=... | Remove one direct assignment. |
| getObjectTags | GET /object-tags?target-type=... | Read one target's direct or effective tags. |
| listObjectsByTag | GET /tags/{tag-name}/assignments?value=... | List matching direct assignments. |

**Collections are paginated by default.** Omitting pagination parameters returns the first page. An empty pageToken also requests the first page. Request pagination=false for all results in one response. Do not combine it with pageToken or pageSize.

Both modes have finite response-size and work limits, including without deployment overrides. Full-result requests return the complete result or an error. Paged requests may return a short page with safe continuation, or fail when safe progress is impossible. Part 1, Section 5.6 defines the parameters and limits.

## 6\. How peer systems answer the same design questions

Part 3 contains the full comparison and links to public sources. This table shows the design dimensions relevant to Polaris v1. Public name-based addressing does not establish how a system stores column identity.

| Design question | Snowflake | Databricks UC | Apache Gravitino | DataHub | AWS LF-Tags | GCP Knowledge Catalog | Polaris v1 |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| Where is a governed definition owned? | Schema | Account | Metalake | Global | Account / Data Catalog | Project / location | Catalog |
| Does an assignment carry a value? | One or multiple strings | Controlled value or key-only | Zero, one, or multiple strings | No tag value | One allowed value | Typed fields | One selected value |
| How do classifications reach other targets? | Hierarchy inheritance and propagation | ABAC inheritance, excluding columns | Hierarchy inheritance | Entity and field associations | Database/table/column inheritance | Entry and column aspects | Hierarchy inheritance on allowed target kinds |
| Can users start from a tag and find targets? | Tag-reference lookup and reporting | SQL tag views | Objects-by-tag API | Search index | LF-Tag search APIs | Catalog search | Direct lookup with an optional value filter |
| How are columns addressed or identified? | Object and column names in public lookup | Name fields in SQL views | Object type and full name | fieldPath | Column name | Schema fields | Table ID plus top-level Iceberg field ID |
| How do value constraints change? | Existing assignments survive narrowing | Allowed-list replacement | Constraints fixed at creation | No tag value model | Removal blocked while in use | Aspect template updates | Existing assignments survive narrowing |
| Where does data-access enforcement belong? | Separate policies | ABAC policies | Outside the compared tag API | Outside the compared tag API | Lake Formation grants | BigQuery policy tags | Outside Tag v1 |

## 7\. Where to read more

| Part | Purpose |
| :---- | :---- |
| Part 1 | REST shapes, validation, reads, errors, and authorization. |
| Part 2 | Durable identity, consistency, Java data shapes, and the proposed JDBC layout. |
| Part 3 | Alternatives, Policy prior art, future extensions, and evidence. |

# REST API

# Part 1: REST API

Part 1 defines request and response shapes, validation, errors, and authorization. Part 2 defines the durable logical model and the proposed built-in JDBC layout.

This part uses five terms:

* A **target** is one catalog object or top-level Iceberg column named by a request.  
* A **target kind** is CATALOG, NAMESPACE, TABLE, VIEW, or COLUMN.  
* A **tag definition** gives a classification a name, allowed values, and allowed target kinds.  
* A **tag assignment** stores one selected value for one tag definition on one target.  
* A **reverse lookup** starts from one tag definition and finds targets with their own assignment.

The base URL is /api/catalog/polaris/v1/{prefix}. All operation examples below are relative to this base.

This keeps Tag alongside the Policy and Generic Tables catalog extensions. Tag v1 shares their URL versioning and routing boundary rather than having an independently versioned API root.

prefix is the required catalog-scoping parameter used by the existing Polaris catalog extensions. In Polaris, it resolves to the catalog name. Encode the original value once as one UTF-8 URI path parameter, subject to catalog naming rules. Namespace, table, view, and column names belong in the target query. Define this parameter locally in the Tag OpenAPI document with these semantics.

A v1 assignment always links a definition and target from the same catalog.

A Tag write makes all its changes visible together. A write that aborts before taking effect leaves the definition and assignments unchanged. If the response is lost after the write takes effect, the client cannot infer that nothing changed. Part 2, Section 6 defines completion and overlapping requests.

Concurrent assignment and deletion may leave an orphan row, but later reads never expose it (Part 2, Sections 6.2–6.4).

Each effective-read response uses hierarchy, definitions, and assignments consistent with one logical point in time during that request. This applies to a full response and to each requested page. Separate pages have no shared snapshot guarantee. Part 2, Section 6.5 defines the implementation obligation.

## 1\. Tag definition

A tag definition has this shape:

Tag {

  id              string        required, read-only, opaque

  name            string        required

  description     string        optional

  values          \[string\]      required

  target-types    \[TargetType\]  required, immutable

  version         string        server-managed, opaque

}

TargetType \= CATALOG | NAMESPACE | TABLE | VIEW | COLUMN

| Field | Meaning |
| :---- | :---- |
| id | Read-only identity for comparison within the same realm and catalog. |
| name | The classification name. It is unique within the catalog. |
| description | Optional text that explains the classification. |
| values | The values an assignment may select. The list must be non-empty. |
| target-types | The target kinds this definition may classify, directly or through inheritance. The list must be non-empty. |
| version | A non-empty token checked by update and rename. |

A tag definition belongs to the catalog identified by {prefix}.

version is a non-empty opaque string. Clients return it unchanged in current-tag-version and must not parse, order, or increment it.

The token identifies the current revision of this definition. The server returns the fields and token from the same definition revision. The API specifies no initial token, numeric sequence, or history.

A new update or rename attempt must match the current token, including a new no-op update. An unrecognized request with a stale token returns 409 TagVersionMismatch. Recognizing an already completed update or rename performs no new write and precedes the current-token comparison (Section 3.7).

The token check and any definition change must be atomic. Every supported definition-write path must participate, including native writes outside the Tag API. A definition change invalidates earlier tokens, even if its fields later return to their previous values.

A token from a deleted definition cannot authorize an update or rename of a same-name replacement. A no-op update leaves definition fields unchanged and returns the current token.

An implementation may use a catalog-wide revision. Unrelated catalog changes may then invalidate a token even when this definition is unchanged.

name uses the TagName schema. It must match ^\[A-Za-z0-9\\-\_\]+$: one or more ASCII letters, digits, hyphens, or underscores. The schema defines no tag-specific maximum length. In particular, / is not a valid TagName character. This restriction does not apply to namespace, table, view, or column names.

id remains unchanged across rename and definition updates. Deleting and recreating a definition produces a different ID, even when the name is reused. Compare IDs only within the same realm and catalog. The API makes no cross-deployment uniqueness or synchronization-preservation promise.

Clients may store, compare, and correlate IDs. They must not parse them, infer ordering, or derive behavior from their representation. The ID does not replace name-based addressing, version checks, authorization, or an idempotency key.

TagIdentifier is the compact response form used by list results and object-tag results:

TagIdentifier {

  name  string  required

  id    string  required, read-only, opaque

}

Both fields describe the same definition. This response object does not introduce ID-based request addressing. The built-in implementation serializes its existing entity ID as a string (Part 2, Section 1). The sample ID tag-identity-a illustrates an opaque response value, not the built-in numeric representation.

The version schema retains minLength: 1. Requiring the field alone does not reject an empty string. Opaque tokens have no client-interpreted format.

### 1.1 Allowed values

values uses exact, case-sensitive string matching.

On create:

* the field is required.  
* null and an empty list are invalid.  
* every member must be a non-empty string.  
* duplicate members are invalid.

On update:

* the field is required and replaces the complete list.  
* missing, null, or empty input is invalid.  
* every member must be a distinct, non-empty string.

Update also requires description, which accepts explicit null to clear it (Section 3.4).

Polaris preserves the submitted order for display. Position does not change whether a value is allowed. Polaris does not trim values or change letter case.

The OpenAPI array omits minItems and uniqueItems. The server rejects duplicates and requires at least one item. This lets the generated model preserve the submitted order. The list shape leaves room for later value forms, but v1 gives an empty list no meaning.

**Changing values never rewrites existing assignments.** The new list controls later writes. An assignment whose stored value was removed from the list is a **grandfathered tag assignment**. It remains readable until it is replaced or unassigned.

Example:

Before:

  sensitivity values \= \["public", "confidential"\]

  customers.email sensitivity \= "confidential"

After the definition removes "confidential":

  the existing assignment remains readable

  a new or replacement "confidential" write fails

  replacing it with "public" succeeds

### 1.2 Target types

target-types uses the five TargetType values defined above. It is a set. List order has no meaning.

On create:

* omitting the field selects CATALOG, NAMESPACE, TABLE, VIEW, and COLUMN.  
* an explicit non-empty subset is allowed.  
* explicit null, empty lists, duplicate members, and unknown kinds are invalid.

The server persists the expanded concrete set and returns it explicitly. It does not store an expanding wildcard. Adding a target kind in a later API design does not expand existing definitions.

The request array uses the TargetType enum and carries no minItems or uniqueItems. The server validates the submitted list before converting it to a set. The response-side Tag.target-types keeps minItems: 1 and uniqueItems: true. Defaulting must distinguish omission from explicit null.

The list controls four outcomes:

1. assignTag accepts only a listed target kind.  
2. A direct read returns the tag only on a listed queried target kind.  
3. An effective read returns the tag only on a listed queried target kind.  
4. An effective read ignores assignments stored on unlisted target kinds.

An excluded intermediate kind is skipped. It does not stop inheritance to an allowed descendant.

For example:

target-types \= \[NAMESPACE, COLUMN\]

namespace sales       \-\> direct assignment allowed

  table orders        \-\> the tag does not apply

    column ssn         \-\> inherits from namespace sales

A child namespace is still a namespace target. It may inherit when NAMESPACE is listed.

target-types is create-only, whether supplied explicitly or defaulted. Supplying it on update, including explicit JSON null, returns 400. Allowing changes would require rules for existing direct assignments and for descendants that gain or lose inherited tags. v1 leaves those changes outside its scope.

### 1.3 Allowed-value identity and rename

The tag definition has a Polaris entity ID. Individual allowed values are strings and do not have separate IDs.

Replacing one string with another is a removal and an addition, not an identity-preserving rename:

Before: values \= \["public", "confidential"\]

After:  values \= \["public", "restricted"\]

Existing "confidential" assignments remain readable. Polaris does not change them to "restricted".

v1 defines no atomic value rename or bulk migration. A later design could add value IDs, a rename operation, or a migration operation.

Each allowed or selected value is limited to 2000 bytes when encoded as UTF-8. A longer value returns 400 BadRequest.

## 2\. Target address

PUT assign, DELETE unassign, and getObjectTags share one single-target query. target-type is required and case-sensitive. Each address selects one target within the catalog selected by prefix.

| target-type | namespace | target-name | column |
| :---- | :---- | :---- | :---- |
| CATALOG | Absent | Absent | Absent |
| NAMESPACE | One or more namespace levels | Absent | Absent |
| TABLE | One or more namespace levels | Table name | Absent |
| VIEW | One or more namespace levels | Iceberg view name | Absent |
| COLUMN | One or more namespace levels | Containing Iceberg table name | One top-level column name |

Every other combination returns 400. Missing target-type never implies catalog scope or bulk removal. Repeated address parameters, including target-type, are invalid even when their values agree. Present empty values and unknown target kinds are invalid.

For a nested namespace \["sales", "eu"\], each name is a **namespace level**. Each level must satisfy the existing Polaris namespace naming rules, including being non-empty and excluding U+001F. Percent-encoding does not make a forbidden name valid. The invisible separator joins levels in the query. For example, \["sales", "eu"\] becomes namespace=sales%1Feu.

Clients must check each level before joining the names. After joining, the server cannot distinguish a separator originally inside a name from one placed between names. It can reject empty split levels. Section 5.2 defines transport decoding and round trips for all three operations.

TABLE covers whole ICEBERG\_TABLE and supported GENERIC\_TABLE entities. VIEW covers whole Iceberg view entities. COLUMN supports only top-level Iceberg table columns. Nested fields, view columns, and generic-table columns are outside v1.

target-name and column each carry one name. A dot or slash in either value is name content, subject to that object's naming rules. It is not a nested-field path.

Column resolution uses the current Iceberg schema through the normal metadata-loading path. This may read Object Storage. Within a request, the server may reuse metadata for the same table if it describes the table state used by the operation. A cached schema alone does not prove that it is current.

Before an assignment is stored, the server resolves names to durable identities:

| API object | Durable identity |
| :---- | :---- |
| Tag definition | Tag entity ID |
| Catalog, namespace, table, or view target | Resolved Polaris entity ID |
| Iceberg table column | Containing table ID plus Iceberg field ID, stored separately |
| Tag value | The selected string, with no separate value ID |

Renames preserve assignments when those identities remain. A same-name replacement with another identity does not inherit them.

An **orphaned tag assignment** refers to a permanently missing definition, target entity, or Iceberg field ID. A soft-dropped target retained for restore is not orphaned. Reads hide both orphaned assignments and assignments on soft-dropped targets. A metadata failure does not prove that an entity or field is missing.

### 2.1 Response target shape

TagAttachmentTarget is the structured response address for assigned-at and reverse-lookup targets. It is not accepted as a target in assignment request bodies.

TagAttachmentTarget {

  type    TargetType  required

  path    \[string\]    namespace levels, plus table/view name when applicable

  column  \[string\]    one top-level name for COLUMN, absent or empty otherwise

}

The response address uses these field combinations:

* CATALOG: path and column are absent or empty.  
* NAMESPACE: path contains namespace levels. column is absent or empty.  
* TABLE: path contains namespace levels followed by the table name. column is absent or empty.  
* VIEW: path contains namespace levels followed by the view name. column is absent or empty.  
* COLUMN: path contains namespace levels followed by the table name. column contains exactly one top-level column name.

Response names are original strings, without URI encoding.

For example, { "type": "VIEW", "path": \["sales", "summary"\] } names the whole view. { "type": "COLUMN", "path": \["sales", "customers"\], "column": \["email"\] } names a table column.

The same namespace/name can resolve differently for TABLE and VIEW. Explicit target-type determines which kind is requested. GET assignments is a collection operation and does not require this single-target query.

## 3\. Tag definition operations

### 3.1 createTag

POST /tags

Idempotency-Key: 260d4de3-079a-4a7a-8de2-cf2c932f7968

Request:

{

  "name": "sensitivity",

  "description": "Data sensitivity level",

  "values": \["public", "internal", "confidential", "restricted"\],

  "target-types": \["CATALOG", "NAMESPACE", "TABLE", "VIEW", "COLUMN"\]

}

Response:

{

  "tag": {

    "id": "tag-identity-a",

    "name": "sensitivity",

    "description": "Data sensitivity level",

    "values": \["public", "internal", "confidential", "restricted"\],

    "target-types": \["CATALOG", "NAMESPACE", "TABLE", "VIEW", "COLUMN"\],

    "version": "rev-a"

  }

}

Rules:

* Success returns 200 OK.  
* Invalid values returns 400 BadRequest.  
* Omitted target-types defaults to all five kinds. Explicit null, empty, duplicate, or unknown input returns 400 BadRequest.  
* A tag-name collision within the catalog returns 409 AlreadyExists, unless a live key recognizes a completed create.

Idempotency-Key is optional. With enabled support, a recognized retry returns 200 and the current Tag at the requested name. It does not restore the creation fields (Section 3.7).

Create locates a record only through its requested name. If creation of sensitivity succeeded and the tag was later renamed, that request does not recover the renamed definition. If sensitivity is free, the retry may create a new definition. If another tag occupies it without the live key, the retry returns the ordinary name conflict. If the original tag has returned to that name, its live key may again be recognized. Deleting the original tag ends recognition of its success.

### 3.2 listTags

GET /tags?pageToken=\&pageSize=100

Response:

{

  "next-page-token": "...",

  "identifiers": \[ { "name": "sensitivity", "id": "tag-identity-a" } \]

}

The operation returns TagIdentifier objects containing definition names and IDs in the catalog. It returns the first page by default. An empty pageToken also requests the first page. Use pagination=false alone to request the complete collection, subject to the full-result limits (Section 5.6).

### 3.3 loadTag

GET /tags/{tag-name}

Response:

{

  "tag": {

    "id": "tag-identity-a",

    "name": "sensitivity",

    "description": "Data sensitivity level",

    "values": \["public", "internal", "confidential", "restricted"\],

    "target-types": \["CATALOG", "NAMESPACE", "TABLE", "VIEW", "COLUMN"\],

    "version": "rev-b"

  }

}

A missing tag definition returns 404 NoSuchTag.

### 3.4 updateTag

PUT /tags/{tag-name}

Idempotency-Key: ea22bec8-2e2b-4c3f-9c33-7a63b7e27a18

Request:

{

  "description": "Reviewed data sensitivity level",

  "values": \["public", "internal", "confidential", "restricted"\],

  "current-tag-version": "rev-b"

}

Response:

{

  "tag": {

    "id": "tag-identity-a",

    "name": "sensitivity",

    "description": "Reviewed data sensitivity level",

    "values": \["public", "internal", "confidential", "restricted"\],

    "target-types": \["CATALOG", "NAMESPACE", "TABLE", "VIEW", "COLUMN"\],

    "version": "rev-c"

  }

}

Update replaces the complete editable definition. Both description and values must be present. Identity, name, target kinds, and assignments are outside this replacement.

| Input | Result |
| :---- | :---- |
| description is a string | Replace the description. The existing empty-string clearing behavior remains supported. |
| description is explicit null | Clear the description. |
| description is omitted | Return 400 ValidationError. |
| values is a valid non-empty list | Replace the complete allowed-value list. |
| values is missing, null, empty, or otherwise invalid | Return 400 BadRequest. |
| target-types is present, including null | Return 400 BadRequest. It is create-only. |

Rules:

* current-tag-version must be a non-empty JSON string. Missing, null, empty, or non-string input returns 400 ValidationError.  
* For an unrecognized request, a stale version returns 409 TagVersionMismatch. A missing named definition returns 404 NoSuchTag.  
* name is changed through renameTag. The update request schema contains only description, values, and current-tag-version.  
* id and response version are read-only. Clients copy only editable fields when building an update from a loaded definition.  
* The token check and replacement are atomic. A successful update returns the resulting definition and its token.  
* A new no-op request still checks the token, leaves the fields unchanged, and returns the current token without advancing it.  
* Allowed-value changes do not rewrite existing assignments (Section 1.1).

To clear the description, send description: null with the complete desired values list and the current token. Missing and null are distinct at the JSON boundary. Generated models and validation must preserve that distinction.

Idempotency-Key is optional. With enabled support, recognize a live key before comparing the current definition version. Return 200 and the current Tag without reapplying the earlier replacement (Section 3.7).

Update uses ordinary name lookup. It does not recover a definition under a different name. If the requested name now refers to a replacement, the old definition token cannot authorize a new write to that replacement.

A successful no-op need not persist a key. If no key was recorded and the definition later changes, retrying the no-op may return 409 TagVersionMismatch. An implementation may record the key, but that internal change must not advance the public definition revision.

### 3.5 dropTag

Without detach-all=true, a definition can be deleted only when no assignments on live targets use it. Confirmed orphaned assignments and assignments on soft-dropped targets do not block deletion.

DELETE /tags/{tag-name}?detach-all=false

A normal drop needs permission to delete the definition. detach-all=true also needs permission to remove assignments of that tag. Section 7 defines the proposed privileges.

Rules:

* Success returns 204 No Content.  
* A missing definition returns 404 NoSuchTag.  
* If an assignment on a live target remains, detach-all=false returns 400 TagInUse and changes nothing.  
* Confirmed orphaned and soft-dropped-target assignments do not block deletion. Successful deletion makes their relations permanently ineffective. Restoring a target cannot restore a deleted tag.  
* Failure to load required entity or schema metadata is not proof of absence. If the server cannot establish the deletion precondition, it fails without visible changes.  
* With detach-all=true, callers see every assignment and the definition removed, or no change.  
* A Tag read starting after deletion succeeds must not return the deleted definition or its assignments, including through another node. A read overlapping deletion may observe the earlier state.  
* Physical cleanup may finish later. Cleanup failure must not undo completed deletion or expose retained rows.  
* Returning 204 after partial visible deletion is invalid.  
* The server does not check every attached target during this catalog-wide removal.  
* Both permissions are required even when no assignments exist at authorization time.  
* v1 permanently deletes the definition and releases its name.  
* v1 has no restore mode.

Drop does not provide key-based recognition after deletion, including with detach-all=true. Retrying after a completed deletion ordinarily returns 404 NoSuchTag. A same-name replacement follows ordinary drop resolution and authorization. No retained success record protects a delayed drop from acting on that replacement.

Part 2 defines ordering when assignment, update, target deletion, and definition deletion overlap.

### 3.6 renameTag

Rename changes one definition's name within its existing catalog. Its public ID, description, allowed values, target kinds, assignments, and grants remain unchanged. It cannot be combined with an editable-definition update.

POST /tags/rename

Idempotency-Key: 8b944a74-7bf2-4d30-893d-b7c15a06d4ec

Request:

{

  "source": "sensitivity",

  "destination": "data\_sensitivity",

  "current-tag-version": "rev-c"

}

source and destination are required tag names using the TagName rules. Both belong to the catalog selected by prefix. current-tag-version is a required non-empty string. The optional header identifies retries of this one logical rename, not the tag itself.

Response: 204 No Content, with no response body, for both a completed rename and a recognized successful retry.

For a new rename:

* Resolve the source definition. Require TAG\_DROP on that definition and TAG\_CREATE on its owning catalog (Section 7.1).  
* Match the current token and change the name atomically. A stale token returns 409 TagVersionMismatch.  
* A destination occupied by another definition returns 409 AlreadyExists. A missing source returns 404 NoSuchTag unless the request is recognized as a successful retry below.  
* A name change invalidates the previous version token. The public ID, assignments, and grants stay attached to the same definition.  
* Success means the name change has taken effect. Load the definition to obtain its current fields and version.

Reusing TAG\_DROP for authorization does not make rename a deletion. Live assignments do not block it, and it requires neither TAG\_DETACH nor the deletion precondition behind TagInUse.

#### Retrying a completed rename

A retry sends the same key, source, destination, and original current-tag-version. With enabled support, an unexpired key on the surviving original definition recognizes success. The caller must currently have TAG\_DROP on that definition and TAG\_CREATE on its catalog. Request validation still applies.

Return 204 before comparing the supplied token with the definition's current version. Recognition performs no new rename and does not advance the definition version. It acknowledges prior success without asserting that the current name or fields still equal that request's result.

The server must locate the original identity even when neither requested name still refers to it. Later renames or updates preserve live keys. Reusing either name for another tag cannot transfer a record to the replacement.

These examples assume enabled support, a live key on the original definition, and successful current authorization:

| Events before retrying the original request | Result |
| :---- | :---- |
| Rename sensitivity to data\_sensitivity succeeds, but its response is lost | Return 204 without renaming again. The original token need not match the current version. |
| That same tag is then renamed to classification | Return 204 for the original retry. Keep the name classification. |
| That same tag is later renamed back to sensitivity | Return 204 for the original retry. Do not rename it to data\_sensitivity again. |
| Another tag takes the old source or destination name | Recognize success on the surviving original identity. Leave the replacement unchanged. |

If no live success is recognized, use ordinary source-name lookup, authorization, and version checks. That path may return 404 NoSuchTag or 409 TagVersionMismatch. Deleting the original definition ends recognition, even if storage retains its physical row. Section 3.7 defines shared key handling and concurrent retries.

### 3.7 Write retries

Idempotency-Key supports **successful-completion recognition**: an authorized retry can acknowledge a completed write without performing it again. This is also called a recognized retry below. The server retains a key with its expiry on the affected definition. It does not retain the original HTTP response or replay terminal errors.

#### Supported operations

Support must be enabled, and the recorded key must still be live on a surviving definition. Each operation also has its own lookup and authorization requirements.

| Operation | Lookup for recognition | Recognized response | Boundary |
| :---- | :---- | :---- | :---- |
| createTag | Requested name | 200 with current Tag | Does not find a tag under another name. Section 3.1 defines ordinary retry behavior after rename. |
| updateTag | Requested name | 200 with current Tag | Recognition precedes the new-write version check. A no-op may have no record (Section 3.4). |
| renameTag | Original definition identity, even after another rename | 204, no body | Recognition precedes the new-write version check. The original definition must survive (Section 3.6). |
| dropTag, including detach-all | No post-deletion recognition | No retry-success guarantee | No retained receipt or tombstone is required. Ordinary name-based deletion applies. |
| assignTag | No key-based recognition | Ordinary replacement | A delayed retry may replace a later value. |
| unassignTag | No key-based recognition | Ordinary removal | A missing assignment returns 404. A delayed retry may remove a recreated relationship. |

For create/update, return the definition carrying the live key. A same-name replacement must not supply the response. Its fields and version describe one revision observed by the retry and may differ from the first response. For example, update K writes description \= A, then another update writes B. A recognized retry of K returns B and its current token, without writing A again. Later changes may occur while the response is in transit.

#### Client and server responsibilities

Clients use a globally unique UUID key for each distinct logical operation. Retries keep the same key and all request arguments unchanged. A key identifies an operation, not a tag or a client session.

The server recognizes the live key within the operation's lookup scope. It does not require a stored request fingerprint or comparison of the original source, destination, version, or other arguments. Reusing a key with changed arguments violates the client contract. This specification promises no particular response to that misuse and introduces no key-conflict error.

Identity recovery for rename remains scoped to the same realm, catalog, and tag entity type. It does not expose an ID-addressed endpoint. A replacement identity does not inherit records from the original definition.

Every recognized retry checks current operation permissions. Create requires TAG\_CREATE on the catalog. Update requires TAG\_WRITE on the resolved tag. Rename requires the two permissions in Section 3.6. Returning the current Tag for create/update adds no separate TAG\_READ requirement. An independent loadTag still requires TAG\_READ.

Keys grant no access. Records are not bound to the first caller's principal, so a different currently authorized caller is not rejected solely for having a different principal.

#### Lifetime and discovery

Tag uses shared Polaris idempotency enablement and lifetime. The lifetime starts when the server captures the request key, rather than when the write completes. Time spent processing the request consumes that window. Recognizing a retry does not renew the record.

The shared catalog configuration response's idempotency-key-lifetime, together with Tag endpoint availability and the operation table above, describes support. The lifetime field alone does not mean every write operation recognizes success. Tag adds no separate discovery field. A previously advertised lifetime does not require continued recognition after support is disabled.

With no key, disabled support, or no live record, use the ordinary operation path. Expired keys cannot establish success. Recognition also ends when the original definition is deleted. Tag keeps no separate journal for recovery after deletion.

Shared header handling accepts a UUID and adds no Tag-specific UUID-version requirement. Missing or blank headers supply no key. When shared handling is enabled, malformed non-blank input returns 400 InvalidIdempotencyKey. The filter can process a header on an operation outside the support table. Accepting that header does not add success recognition to that operation.

#### Persistence and concurrent retries

When a supported request changes a definition, persist its key and mutation together. Later writes must preserve all live records. An update that changes no fields may omit a record, as Section 3.4 permits.

Before reporting a conflict caused by a concurrent duplicate, read fresh state and recheck recognition. This covers create name collisions and update/rename version or conditional-write conflicts. A successful competing commit may now supply the live key. If it does not, return the ordinary result. This rule does not promise success for every concurrent request or for a failed dependency read.

A full key window must not evict a live key to admit another. Reject the new mutation with 503 ServiceUnavailable and Retry-After, leaving it unapplied.

An unreadable or unsupported stored key-window encoding follows shared behavior: treat it as no live record and use the ordinary operation path. A persistence or dependency read failure is different. It remains a service error and must not be treated as an empty record.

## 4\. Tag assignment operations

An assignment request has this shape:

AssignTagRequest {

  values  \[string\]  required

}

Every v1 assignment selects exactly one value from the current values list.

The API keeps values as a list so later value forms could reuse the field. v1 supports neither an empty list nor several selected values.

The OpenAPI array omits uniqueItems, minItems, and maxItems. The server checks that the list contains exactly one item and rejects duplicates. It must receive the submitted list unchanged. For example, \["a", "a"\] must produce a structured error. A generated Set would remove the duplicate before the server could validate it.

The tag definition and target identify the assignment. values is content, not identity. Reassigning the same tag replaces the complete selected-value list.

### 4.1 assignTag

PUT /tags/{tag-name}/assignments?target-type=COLUMN\&namespace=sales\&target-name=customers\&column=email

Request:

{

  "values": \["confidential"\]

}

Response: 204 No Content.

Rules:

* values must be present, non-null, non-empty, duplicate-free, and contain no empty member.  
* The selected value must appear in the current values list.  
* The target kind must appear in the definition's target-types list.  
* Reassigning the same tag replaces the stored value.  
* Repeating a grandfathered value is a replacement write and fails when that value is no longer allowed.  
* The target must be inside the catalog identified by {prefix}.  
* A missing definition returns 404 NoSuchTag.  
* A missing target or column returns 404 NoSuchTarget.  
* An unsupported or malformed target returns 400 BadRequest.  
* A supported target kind not listed by the definition returns 400 BadRequest.  
* Missing values, or a non-null values field that is not a JSON array, returns 400 ValidationError.  
* Explicit null values returns 400 BadRequest, as does an empty array. Neither input creates or replaces an assignment.  
* An array of strings that is empty, contains an empty or duplicate member, or selects multiple values returns 400 BadRequest.  
* A selected value outside the current list returns 400 BadRequest.

Array-member schema failures use the shared validation rules in Section 6.3. A present array alone does not establish that its members have valid types.

Polaris resolves and authorizes the definition and target before returning 400 BadRequest for an excluded target kind.

### 4.2 unassignTag

DELETE /tags/{tag-name}/assignments?target-type=TABLE\&namespace=sales\&target-name=customers

There is no request body. The required query identifies exactly one assignment.

Response: 204 No Content.

Rules:

* A missing definition returns 404 NoSuchTag.  
* A missing target or column returns 404 NoSuchTarget.  
* A missing assignment returns 404 NoSuchAssignment.  
* The target must be inside the catalog identified by {prefix}.  
* The operation does not re-check target-types when removing an existing assignment.

unassignTag works only on a current target that resolves from the request. A lookup failure never becomes a request to delete an orphaned row.

v1 exposes no manual orphan-cleanup REST operation. A failed target lookup stays NoSuchTarget. It is never reinterpreted as orphan cleanup. Part 2 defines **best-effort cleanup** for the proposed built-in JDBC flow. It tries to remove rows during permanent target deletion. Cleanup failure does not fail the deletion.

### 4.3 Why v1 uses this relationship route

The URI names the tag and identifies the target through query parameters. PUT creates or replaces that relation, with only values in its body. DELETE removes it without a body. The query uses the same encoding as getObjectTags.

GET on /tags/{tag-name}/assignments lists direct assignments. It accepts the collection's value filter and pagination parameters, not a required single-target query. No independent assignment ID or version is added.

### 4.4 v1 boundaries

Assign/unassign do not provide key-based success recognition or a client version precondition. Their internal concurrency checks protect each execution, without identifying a delayed retry. Repeating assign can overwrite a later value. Repeating unassign can remove a relationship recreated since the first request. A currently missing assignment still returns 404 NoSuchAssignment.

v1 defines no rule-driven, key-only, free-form, or multi-value assignment API. Whole Iceberg views are supported. View columns, nested fields, and generic-table columns remain future work.

## 5\. Read operations

There are two read directions:

* getObjectTags starts from one target and returns tags for that target.  
* listObjectsByTag starts from one definition and returns targets with their own assignment.

### 5.1 Direct and effective views

A **direct tag assignment** is stored on the target being read. An **inherited tag assignment** is stored on a parent but applies to that target.

getObjectTags supports two views:

* direct returns only assignments stored on the target. Omitting view returns this view.  
* effective reads the target and its parents, then returns the final result for each tag.

For each tag definition, the read follows these rules:

1. If the queried target kind is not in target-types, the tag is omitted.  
2. A direct read checks only the queried target.  
3. An effective read considers assignments only on listed target kinds.  
4. The parent walk continues through excluded intermediate kinds.  
5. Among the remaining assignments, the closest one wins. This is the **winning assignment**.

The precedence order remains:

column \> table \> nearest namespace \> catalog

view \> nearest namespace \> catalog

If several namespace assignments apply, the deepest namespace wins. Values from different levels are never combined.

Example for target-types \= \[NAMESPACE, COLUMN\]:

| Assignment stored at | Queried target | Result |
| :---- | :---- | :---- |
| Namespace sales | Namespace sales | Direct |
| Namespace sales | Child namespace sales.eu | Inherited |
| Namespace sales | Table sales.orders | No result |
| Namespace sales | Column sales.orders.ssn | Inherited from sales |
| Column sales.orders.ssn | That column | Direct and closer than the namespace assignment |

**Omitting view returns direct.** A caller that needs inherited classifications must request view=effective.

### 5.2 getObjectTags

GET /object-tags

prefix identifies only the catalog. The required target-type and type-appropriate query fields are defined in Section 2\.

view accepts direct or effective. It is optional and defaults to direct. A present empty, repeated, or unknown value returns 400.

Both views are paginated by default. Use pagination=false to request all tags for the target in one response, subject to full-result limits. Section 5.6 defines the pagination parameters and invalid combinations.

For the column sales.eu.customers.email:

GET /object-tags?target-type=COLUMN\&namespace=sales%1Feu\&target-name=customers\&column=email\&view=effective

To query the table, use target-type=TABLE and omit column. To query the namespace, use target-type=NAMESPACE and omit target-name and column. The catalog requires explicit target-type=CATALOG and no other address fields.

#### Query encoding

These rules apply to PUT assign, DELETE unassign, and GET target reads.

namespace uses Iceberg namespace query encoding. It joins names with the invisible separator U+001F, then encodes the result for the URL. For example, \["sales", "eu"\] becomes sales%1Feu. Use Iceberg's query encoding, not its path encoding.

Encoding follows two steps:

1. Check every namespace level against the existing naming rules, including non-empty names and no U+001F. Join the original names with U+001F. Do not URI-encode individual names.  
2. Encode the joined text as one UTF-8 URI query value. Encode target-name and column directly as plain string query values.

The separator is fixed for this Tag endpoint. An Iceberg catalog's configurable namespace separator does not change this contract.

For URI encoding, leave only ASCII letters, digits, \-, ., \_, and \~ unescaped. Percent-encode every other UTF-8 byte.

Encode space as %20 and literal plus as %2B. When decoding, raw \+ means space, including in target-name and column.

The server separates query parameters before decoding their values. It converts raw \+ to space before percent-decoding and UTF-8 decoding, each once. It then splits namespace on U+001F.

If the HTTP framework already decoded the query value, the handler must not URI-decode it again. Table and column names are not split or parsed as JSON.

A dot in column is part of one top-level column name, not a nested-field path. If a name contains the literal text %1F, encode it as %251F. One URI decode restores the text %1F, not the invisible separator.

A present target-name or column must be non-empty. A present namespace must contain one or more non-empty levels after splitting.

Preserve element order, case, whitespace inside names, and Unicode characters without normalization. Apply existing identifier-validation rules after decoding.

Malformed percent escapes and invalid target shapes return 400. A rejection by the HTTP layer uses its shared response. Validation in the Tag handler uses BadRequest.

Clients must send valid UTF-8. The shared HTTP layer handles invalid UTF-8 in v1. Tag v1 does not require every server to reject those bytes in the same way. Part 3, Section 5.9 describes that follow-up.

Omit namespace for a catalog target. An empty parameter or an empty namespace level is invalid. The server does not auto-detect JSON arrays or another separator format.

Each of target-type, namespace, target-name, column, and view appears at most once. Duplicate parameters return 400 BadRequest.

Client libraries that expose Tag target queries should provide namespace encode/decode helpers and share these conformance examples. Existing Iceberg query helpers may be reused. Before joining names, validate each level, including the empty-name and U+001F checks (Section 2).

The encode helper returns joined, unencoded text to the URI builder. The builder performs the single URI-encoding step. The namespace decode helper receives the already URI-decoded value.

Conformance examples below show exact query-value contents, without the parameter name. US in the middle column denotes one actual U+001F character, not the letters US.

| Input | Before URI encoding | Query-value contents |
| :---- | :---- | :---- |
| Namespace \["sales", "eu"\] | sales \+ US \+ eu | sales%1Feu |
| Namespace \["sales.eu"\] | sales.eu | sales.eu |
| Namespace \["R\&D", "tax=2026"\] | R\&D \+ US \+ tax=2026 | R%26D%1Ftax%3D2026 |
| Namespace \["tax%20rate", "%1F"\] | tax%20rate \+ US \+ %1F | tax%2520rate%1F%251F |
| Namespace \["with space", "café"\] | with space \+ US \+ café | with%20space%1Fcaf%C3%A9 |
| Namespace \["销售", "日本語"\] | 销售 \+ US \+ 日本語 | %E9%94%80%E5%94%AE%1F%E6%97%A5%E6%9C%AC%E8%AA%9E |
| Plain target-name: orders%20\&total | orders%20\&total | orders%2520%26total |
| Plain column: cost+tax | cost+tax | cost%2Btax |
| Plain column: a &?=+% | a &?=+% | a%20%26%3F%3D%2B%25 |
| Plain column: customer.name | customer.name | customer.name |
| Plain target-name: orders/2026 | orders/2026 | orders%2F2026 |

Examples test encoding only. They do not expand the identifiers accepted by the catalog or schema.

column=a+b decodes to a b. column=a%2Bb decodes to a+b. column=%252B decodes to the literal %2B, without another decoding pass.

namespace=sales%251Feu names one level, sales%1Feu. It does not name \["sales", "eu"\]. Double encoding cannot always be detected because literal percent sequences may be valid names.

The examples below show invalid query fragments, assuming otherwise valid required addressing. Each returns 400:

| Query input | Reason |
| :---- | :---- |
| namespace= | Empty parameter. |
| namespace=%1Fsales | Empty first namespace level. |
| namespace=sales%1F | Empty last namespace level. |
| namespace=sales%1F%1Feu | Empty middle namespace level. |
| namespace=% | Incomplete percent escape. |
| namespace=%GG | Non-hexadecimal percent escape. |
| namespace=sales\&target-name= | Empty table name. |
| namespace=sales\&target-name=orders\&column= | Empty column name. |
| namespace=sales\&namespace=eu | Duplicate target-address parameter. |

#### Response

Each result contains:

ObjectTag {

  tag           TagIdentifier

  values        \[string\]

  apply-method  string

  assigned-at   TagAttachmentTarget

}

Response:

{

  "object-tags": \[

    {

      "tag": { "name": "sensitivity", "id": "tag-identity-a" },

      "values": \["confidential"\],

      "apply-method": "INHERITED",

      "assigned-at": { "type": "NAMESPACE", "path": \["sales"\] }

    }

  \]

}

| Field | Meaning |
| :---- | :---- |
| tag | The definition name and read-only ID, describing the same tag. |
| values | The selected value, returned as a one-item list. |
| apply-method | How the classification applies to the target being read. |
| assigned-at | The target that stores the winning assignment. |

### 5.3 apply-method and assigned-at

The three fields answer different questions:

* view tells the server whether to read parents.  
* apply-method tells the caller how the tag applies.  
* assigned-at tells the caller where the winning assignment is stored.

| View | Winning assignment | apply-method | assigned-at |
| :---- | :---- | :---- | :---- |
| direct | Stored on the queried target | DIRECT | The queried target |
| effective | Stored on the queried target | DIRECT | The queried target |
| effective | Stored on a parent | INHERITED | The parent that stores it |

The server returns two values:

| Value | Meaning |
| :---- | :---- |
| DIRECT | The assignment is stored on the queried target, regardless of whether a person or automation created it. |
| INHERITED | The winning assignment is stored on a parent target. |

apply-method is output only. It does not select a view and is not accepted by assignTag.

The response field is a string so clients can accept methods added by a future specification. Servers implementing this API return only DIRECT or INHERITED, regardless of who created the assignment.

When an assignment is inherited, the effective result returns INHERITED. v1 does not separately expose its producer or automation mechanism.

Unassigning the queried target removes only its local assignment. It does not remove an inherited assignment from its parent. Removing a local override may reveal the parent value.

### 5.4 Read guarantees

getObjectTags follows these rules:

* **Complete hierarchy or error.** Every effective read considers all ancestors. Paging returned tags must not truncate the hierarchy or fall back to direct.  
* **One coherent response.** Hierarchy, definitions, and assignments describe one logical point in time during the request, for both a full response and each requested page.  
* **Target-kind filter.** Direct and effective results include a tag only when the queried target kind appears in that definition's target-types.  
* **Allowed assignment sources.** Effective reads ignore assignments stored on target kinds excluded by that definition.  
* **Skipped intermediate kinds.** An excluded intermediate kind does not stop the parent walk.  
* **Effective-read visibility.** Authorizing the queried target permits reading all its effective tags. Polaris does not separately authorize parent sources or each definition. Section 5.6 controls pagination.  
* **Grandfathered values remain classifications.** They remain visible until replaced or unassigned.  
* **Forward-compatible method field.** Clients accept unknown future method strings without dropping the classification. v1 servers return the two methods defined above.  
* **No inheritance block.** Missing a direct assignment does not block a parent assignment.  
* **Missing and soft-dropped targets stay hidden.** Results exclude soft-dropped targets and assignments whose definition, entity, or field is permanently absent.

### 5.5 listObjectsByTag

A reverse lookup starts from a definition and finds targets with their own assignment.

GET /tags/{tag-name}/assignments?pageToken=\&pageSize=100

The tag stays in the route because the operation starts from one definition. A catalog-level /objects?tag=... route would create a broader object-search API.

value is optional:

* Without value, include all direct assignments for the tag in the result set.  
* With value, include only direct assignments containing that exact, case-sensitive string.  
* A filter may use a grandfathered value.  
* Targets that only inherit the tag are not returned.

target-types does not expand reverse lookup. It only limits which direct assignments can exist and where inherited results may appear.

**Each item describes one direct assignment that actually existed.** Its target, values, and direct apply-method must come from the same assignment state.

This endpoint is paginated by default. Use pagination=false to request the complete result, subject to full-result limits (Section 5.6). Separate page requests may observe changes between pages. The definition response does not contain its assigned targets. This endpoint lists them separately.

The example response below has no value filter. With value=confidential, only the column item would match.

{

  "next-page-token": "...",

  "objects": \[

    {

      "target": { "type": "TABLE", "path": \["sales", "customers"\] },

      "values": \["internal"\],

      "apply-method": "DIRECT"

    },

    {

      "target": {

        "type": "COLUMN",

        "path": \["sales", "customers"\],

        "column": \["email"\]

      },

      "values": \["confidential"\],

      "apply-method": "DIRECT"

    }

  \]

}

The server first checks TAG\_READ on the named definition. Each returned target also requires its corresponding READ\_PROPERTIES permission. For a column, check the containing table. Targets denied that permission are omitted.

Rules:

* A missing definition returns 404 NoSuchTag.  
* Results contain only direct assignments. Pagination parameters and full-result limits are defined in Section 5.6.  
* Each returned target kind appears in the definition's target-types list.  
* Soft-dropped targets and orphaned assignments are filtered out.  
* Grandfathered values are returned as stored.  
* v1 returns exactly one value per item.  
* apply-method is never INHERITED in this response.  
* A failed definition authorization rejects the operation. A denied target is filtered from results. An authorization-system failure must not be treated as an ordinary deny.

### 5.6 Pagination

listTags, listObjectsByTag, and getObjectTags are paginated by default. Requests use the optional query parameters pagination, pageToken, and pageSize. Responses use next-page-token.

pagination is a boolean query parameter that defaults to true. Omitting it or sending pagination=true selects paged mode. pagination=false requests the full result in one response, subject to the limits below. The choice changes only result delivery. Filters, authorization, and read-consistency guarantees remain the same in both modes.

Clients should normally process results one page at a time because they may not know the total result size. Request all results in one response only when the client can safely receive and process the complete result. A server-side limit does not guarantee that the response fits in the client's memory.

The paged-mode rows below apply when pagination is omitted or true.

| Request | Server behavior |
| :---- | :---- |
| No pagination parameters | Return the first page using the server's finite, positive default page size. |
| Paged mode, pageToken omitted or empty | Return the first page. |
| Paged mode, pageToken=\<token\> | Continue from the position identified by the returned token. |
| Paged mode, pageSize=\<positive integer\> | Use the requested size as an upper bound, including without a token. The server may return fewer items. |
| Paged mode, pageSize omitted | Use the server's current finite, positive default, including on continuations. Do not inherit the earlier request's size from its token. |
| pagination=false, no pageToken or pageSize | Return the full result in one response or fail at an identified limit. |
| pagination=false with any pageToken or pageSize | Return 400 BadRequest, including when the conflicting parameter is empty. |

A supplied pagination accepts only true or false. Empty or other values return 400 BadRequest. Each pagination parameter may appear at most once. Duplicate pagination, pageToken, or pageSize parameters return 400 BadRequest, even when their values agree.

pageToken is an opaque string. Clients return it unchanged and URI-encode it once as a query value. A non-empty token must belong to the requested query. Invalid or expired tokens return 400 BadRequest. Clients do not need to send pagination=true when continuing with a token.

A supplied pageSize must be a positive integer. Zero, negative, empty, and non-integer values return 400 BadRequest. The server may cap a valid requested size at its configured maximum. Exceeding that maximum alone need not cause rejection. Neither a special size value nor an empty token selects full-result mode. Shared pagination flags or helpers must not change these input or mode rules.

A non-empty next-page-token means the client must continue, even after a short or empty page. An absent or null next-page-token means the result is complete. Responses must not use an empty string for this field. A full-result response omits the field or sets it to null.

For example, GET /tags requests the first page. GET /tags?pageSize=100 requests the first page with at most 100 definitions. GET /tags?pageToken=\&pageSize=100 has the same meaning. A later request sends the returned token as pageToken. GET /tags?pagination=false requests all definitions in one response. Combining it with pageToken= returns 400 BadRequest.

**Both modes have finite response-size and request-work limits.** These protections apply without deployment overrides. A page-size bound alone does not bound work, because filtering may examine many candidates without returning them. Deployments document their limits. This contract does not prescribe configuration keys, numeric values, units, or enforcement mechanisms.

**Full-result requests return the complete result or an error.** Exceeding an identified limit returns 400 BadRequest. The message directs the client to retry in paged mode by removing pagination=false or setting it to true. Preserve the target and filters when retrying. Detect the limit before committing a successful response. Do not silently truncate, switch to paged mode, or return a successful partial body. Internal batching alone does not bound total response size or request work.

**Paged requests may stop early with safe continuation.** A response-size or work budget may produce a short or empty page. A non-empty continuation token must advance past consumed candidates so the client can continue making progress. A candidate is consumed only after it has been returned or rejected by the query's checks. Fetching a batch does not consume all its rows. Fetched but unconsumed candidates must remain reachable.

For example, a batch contains 100 candidates but the budget allows processing only the first 20\. Resume after candidate 20, including when all 20 were filtered out. Advancing past candidate 100 would lose unprocessed results.

If a request cannot make safe progress within an identified budget, return 400 BadRequest. Do not issue an endless sequence of continuation tokens that makes no progress. An error while reading metadata or evaluating authorization is a service failure, not a budget limitation, ordinary denial, or evidence of absence.

Bind tokens to the query's catalog, definition or target identity, view, and filters as applicable. Reauthorize each request. A token grants no access.

One effective-read response must describe one logical point in time during the request, including when internal batches build a full response. A paged effective result still considers the complete hierarchy for its returned tags. Each reverse-lookup item must describe one assignment state during its request. Separate page requests need not share a snapshot (Part 2, Sections 6.5–6.6).

## 6\. Errors

This section defines the Tag error contract. OpenAPI descriptions, generated models, and server responses must preserve these status codes and error types.

### 6.1 Error response

Tag errors use a Polaris-owned schema:

{

  "error": {

    "message": "Tag sensitivity does not exist",

    "type": "NoSuchTag",

    "code": 404

  }

}

error.message, error.type, and error.code are required. The code equals the HTTP response status. The message explains the failure for a person. Clients distinguish causes through the type rather than parsing the message.

The error.type values in Sections 6.2 and 6.3 omit the Exception suffix. Internal exception class names do not determine those wire values.

### 6.2 Tag operation errors

| Condition | Operations | HTTP code | Error type |
| :---- | :---- | ----: | :---- |
| Named tag definition not found, with no recognized successful retry for rename | Load, update, rename, drop, assign, unassign, reverse lookup | 404 | NoSuchTag |
| Tag-name collision within the catalog, with no recognized successful retry | Create or rename | 409 | AlreadyExists |
| Stale or non-matching definition token for a new write | Update or rename | 409 | TagVersionMismatch |
| Conditional update conflicts after validation, with no recognized successful retry | Update or rename | 409 | CommitConflict |
| A live-target assignment prevents normal deletion | Drop without detach-all | 400 | TagInUse |
| Assignment not found on the resolved target | Unassign | 404 | NoSuchAssignment |
| Target path or column not found | Assign, unassign, target read | 404 | NoSuchTarget |
| Invalid tag name detected by Tag validation | Create or rename | 400 | BadRequest |
| Missing, null, empty, or invalid definition values list | Create or update | 400 | BadRequest |
| Explicit null, empty, duplicate, or unknown target-types | Create | 400 | BadRequest |
| Any presence of target-types, including null | Update | 400 | BadRequest |
| Missing values, or a non-null values field that is not a JSON array | Assign | 400 | ValidationError |
| Explicit null values | Assign | 400 | BadRequest |
| An array of strings that is empty, contains an empty or duplicate member, or selects multiple values | Assign | 400 | BadRequest |
| Selected value is not currently allowed | Assign | 400 | BadRequest |
| Supported target kind is excluded by the definition | Assign | 400 | BadRequest |
| Missing or malformed target address, unsupported kind, nested field, or unsupported column target | Assign, unassign, target read | 400 | BadRequest |
| Invalid encoding, read view, or duplicate parameter detected by the Tag handler | Affected operation | 400 | BadRequest |
| Invalid pagination input, duplicate parameter, conflicting mode parameters, or invalid token | List definitions, reverse lookup, target read | 400 | BadRequest |
| Full-result limit exceeded, or paged request cannot make safe progress within an identified budget | List definitions, reverse lookup, target read | 400 | BadRequest |

NoSuchAssignment means the resolved target has no direct assignment for the named tag.

A generic 404 response covers a required resource that cannot be found, including the surrounding catalog. A generic 409 response covers conflicting state. The types above distinguish missing definitions, missing targets, name collisions, stale tokens, and conditional-write conflicts.

After TagVersionMismatch, the client reloads the definition before deciding whether to retry. AlreadyExists requires resolving the name collision. CommitConflict means the conditional write did not commit and may be retried after reevaluating its preconditions.

### 6.3 Shared request and service failures

Tag endpoints use the following fixed wire types for failures shared with other catalog operations. These values use the envelope in Section 6.1. The server translates internal failures into this table. An exception class name does not select the wire type.

| Shared failure | HTTP status | Error type |
| :---- | ----: | :---- |
| Request body is not syntactically valid JSON | 400 | InvalidJson |
| Required field is missing or a request field has an invalid schema type, unless Section 6.2 names a Tag validation error | 400 | ValidationError |
| Missing, null, empty, or non-string current-tag-version | 400 | ValidationError |
| Non-blank malformed Idempotency-Key when the shared idempotency handler is enabled | 400 | InvalidIdempotencyKey |
| Missing or invalid authentication | 401 | Unauthorized |
| Required operation permission denied | 403 | Forbidden |
| Surrounding catalog not found | 404 | NoSuchCatalog |
| A required metadata, storage, or authorization dependency is temporarily unavailable | 503 | ServiceUnavailable |
| A supported mutation cannot record another key because its live-key window is full | 503 | ServiceUnavailable |
| Unexpected service failure, or a required dependency fails for a reason not covered above | 500 | InternalServerError |

Section 6.2 takes precedence for the conditions it names. Missing definition values on create or update returns BadRequest. Missing assignment values, or a non-null value that is not an array, returns the shared ValidationError. Explicit null assignment values returns BadRequest.

Missing update description or current-tag-version returns ValidationError. Omitted creation target-types selects the default set. A missing catalog returns NoSuchCatalog, while a missing definition within a resolved catalog returns NoSuchTag.

A full key window returns Retry-After with its 503 response.

These are wire literals, independent of internal exceptions. The mapping applies to Tag endpoints and does not change other APIs.

A dependency failure is not evidence that a target or field is absent. It returns a service error, not NoSuchTarget or a successful empty result. A 5xx response or transport failure alone does not prove that a write had no effect (Part 2, Section 6).

For these failures, the server returns an error. It must not return a successful empty result or silently omit reverse-lookup candidates whose checks could not be completed. The shared 400 rules cover schema failures for which this specification requires rejection. Update explicitly accepts null description for clearing it. Null values is invalid. Creation does not acquire new null semantics for name or description.

Tag validation uses the error types listed for each condition in Section 6.2. Malformed URIs may be rejected by ingress before the Tag handler. Such rejection uses the HTTP layer's shared response and is not guaranteed to carry a Tag-specific payload.

## 7\. Authorization

Authorization follows six rules:

1. Create and list check the parent catalog.  
2. Load, update, and drop check the tag definition.  
3. Rename checks the original definition and its owning catalog, including when recognizing a successful retry.  
4. Assign and unassign check both the definition and current target.  
5. A normal drop and detach-all=true use different permission sets.  
6. Target reads check the queried target. Reverse lookup checks the named definition and each returned target.

The privilege names below are illustrative. Public API terms remain assign and unassign.

### 7.1 Tag definition operations

| Operation | Check | Proposed privilege | Notes |
| :---- | :---- | :---- | :---- |
| createTag | Parent catalog | TAG\_CREATE on CATALOG | Applies to new creation and recognized retries. |
| listTags | Parent catalog | TAG\_LIST on CATALOG | Lists definition names and IDs in the catalog. |
| loadTag | Tag definition | TAG\_READ on TAG | Reads one definition. |
| updateTag | Tag definition | TAG\_WRITE on TAG | Applies to replacement and recognized retries. target-types is immutable. |
| renameTag | Original definition and its owning catalog | TAG\_DROP on TAG and TAG\_CREATE on CATALOG | Both are required for a new rename and recognized retry. Check the original identity even after another rename. |
| Normal dropTag | Tag definition | TAG\_DROP on TAG | Live-target assignments block normal deletion. |
| dropTag?detach-all=true | Tag definition | TAG\_DROP plus TAG\_DETACH on TAG | Removes the definition and all assignments as one visible result. |

Recognized create/update retries use their original operation permissions and add no TAG\_READ check for the returned current definition. Every retry checks the current caller, and possession of a key grants no permission.

Rename is a distinct authorization operation from update. TAG\_WRITE alone does not permit rename. Built-in and external authorizers must apply both rename checks without changing the update permission. Rename preserves grants and assignments, so it does not require TAG\_DETACH or test whether the tag is in use.

TAG\_DROP permits normal deletion when only confirmed orphaned or soft-dropped-target assignments remain. Those assignments stay hidden even if a target is restored. No additional target permission is needed. detach-all=true always also requires TAG\_DETACH on the definition, even when no assignments exist. Polaris does not check every attached target in this path.

### 7.2 Tag assignment operations

assignTag and unassignTag require both checks:

| Operation | Definition check | Target check |
| :---- | :---- | :---- |
| assignTag | TAG\_ATTACH on TAG | The target-side attach-tag privilege below |
| unassignTag | TAG\_DETACH on TAG | The target-side detach-tag privilege below |

| Target | assignTag target-side privilege | unassignTag target-side privilege | Notes |
| :---- | :---- | :---- | :---- |
| Catalog | CATALOG\_ATTACH\_TAG | CATALOG\_DETACH\_TAG | Checked on the catalog identified by {prefix}. |
| Namespace | NAMESPACE\_ATTACH\_TAG | NAMESPACE\_DETACH\_TAG | Checked on the namespace. |
| Table | TABLE\_ATTACH\_TAG | TABLE\_DETACH\_TAG | Checked on the table. |
| View | VIEW\_ATTACH\_TAG | VIEW\_DETACH\_TAG | Checked on the view. |
| Column | TABLE\_ATTACH\_TAG | TABLE\_DETACH\_TAG | Checked on the containing table. |

For assignTag, both authorization checks and the target-types check must pass. For unassignTag, both authorization checks must pass, and target-types are not re-checked, as Section 4.2 states.

The target-side check does not run once per target during dropTag?detach-all=true. That path requires both TAG\_DROP and TAG\_DETACH on the definition (Section 7.1).

unassignTag is authorized only after the target resolves. v1 defines no public orphan-cleanup operation or Tag-specific cleanup privilege.

### 7.3 Read operations

| Operation | Check | Proposed privilege | What it reveals |
| :---- | :---- | :---- | :---- |
| getObjectTags | Queried target | {CATALOG,NAMESPACE,TABLE,VIEW}\_READ\_PROPERTIES | The complete direct or effective tags for that target. Column reads check the containing table. |
| listObjectsByTag | Named definition and each returned target | TAG\_READ, then the target's READ\_PROPERTIES | Only readable targets, their selected values, and DIRECT. Columns use the table check. |

Permission to read a target's properties includes the complete tags that apply to it. Polaris does not separately authorize parent sources or each returned definition.

Reverse lookup does not introduce a catalog-wide bypass. TAG\_READ permits reading the named definition but does not reveal unreadable targets. Each candidate requires the target property-read decision. An authorizer needs the caller context, the action being checked, and the resolved definition or target resource. Concrete authorization interfaces remain implementation choices.

### 7.4 Future authorization input

V1 defines tag management and reads. Integrating tags into access decisions is outside this proposal's scope. An **authorizer** is a built-in or external component that returns an allow or deny decision.

A later authorizer could use the complete effective tags as facts about the target. Polaris would apply target-types, inheritance, and closest-wins rules before authorization uses the result.

That integration must use an internal effective-tag path, not the public REST endpoint. If the full result cannot be resolved, authorization must fail rather than treat the target as untagged.

This is an extension path, not a v1 authorization feature. v1 defines no attribute-based access control language or policy-administration API.

---

# Durable Data Model

# Part 2: Durable logical data model

Part 1 defines the REST API. This part defines the durable facts and results that every compatible implementation must preserve.

This part uses five terms:

* A **target** is one catalog object or top-level Iceberg column.  
* A **target kind** is CATALOG, NAMESPACE, TABLE, VIEW, or COLUMN.  
* A **tag definition** names a classification and defines its allowed values and target kinds.  
* A **tag assignment** stores one selected value for one definition on one target.  
* A **reverse lookup** starts from one definition and finds targets with their own assignment.

IDs are resolved within a Polaris realm, the tenant that contains catalogs. The logical definition and assignment records below omit this common context.

Sections 9 and 10 illustrate Java data and operations and one JDBC layout. Other implementations may use different mechanisms to store these facts and provide the behavior defined here.

Polaris keeps two durable facts:

1. A tag definition is a catalog child with its own Polaris entity ID.  
2. A tag assignment links that definition ID to one target identity and the string selected for that assignment.

For one definition and one target, v1 keeps at most one current assignment.

A Tag write makes all its changes visible together. A write that aborts before taking effect leaves the definition and assignments unchanged. A lost response does not prove that the write aborted (Section 6). With detach-all=true, the definition and all its assignments disappear from reads together. Stored assignment rows may remain after deletion, provided reads keep them hidden.

In this document, **best-effort cleanup** means target deletion tries to remove related rows, but deletion does not depend on that cleanup succeeding.

## 1\. Tag definition

Within one realm, a tag definition is a Polaris TAG entity under a catalog.

| Field | Type | Meaning |
| :---- | :---- | :---- |
| catalog | id-ref | The catalog that owns the definition. |
| id | id | A server-generated Polaris entity ID, exposed as a read-only opaque string. |
| name | string | The classification name, unique within the catalog. |
| description | string | Optional explanatory text. |
| values | ordered list of strings | Values that later assignments may select. Required and non-empty. |
| target-types | set of target kinds | A persisted, non-empty, immutable set. Creation expands an omitted field to the five v1 kinds. |
| version | opaque string | A server-managed stale-update token for this definition. |

Within one realm, definition identity is:

(catalog, id)

Name uniqueness is:

(catalog, name)

Assignments refer to id, not to the definition name or version. Renaming a definition or changing allowed values does not rewrite assignments.

The public ID is the existing entity ID serialized as a string in the built-in implementation. Rename and update preserve it. Delete-and-recreate uses a different identity. A provider preserves the same visible identity rules without requiring a separate public UUID.

The ID supports comparison within the same realm and catalog. It does not select a write target or replace the version token. Internal migrations must preserve the public identity contract. Cross-catalog synchronization may maintain source-to-destination mappings.

Allowed values are plain strings. They have no separate IDs or lifecycle in v1.

### 1.1 Update version

version prevents update or rename from overwriting a newer definition state.

* A client returns its last-read string unchanged in current-tag-version.  
* The current token must match before a new update or rename write. A new no-op update also checks it.  
* Recognizing a completed update or rename requires a live key on the surviving definition and current authorization, within its lookup scope. It precedes the current-version comparison (Section 6.7).  
* The token check and any definition change are atomic.  
* A no-op update leaves definition fields unchanged and returns the current token.  
* A stale token cannot authorize a new write, including when it belongs to a deleted same-name definition.  
* Every supported definition-write path participates in the check.  
* Definition changes invalidate earlier tokens, even if the fields later return to their previous values.  
* The API defines no initial value, ordering, increment rule, or history.  
* Assignments do not store the token.

The backend may use a row revision, commit ID, or another conditional-write token. A catalog-wide revision may cause conflicts after unrelated changes.

The proposed built-in JDBC representation uses an integer internally. Section 10 maps that storage value to the API token.

### 1.2 Target kinds

target-types contains one or more of:

CATALOG | NAMESPACE | TABLE | VIEW | COLUMN

target-types is a set. Order has no meaning. Creation stores the supplied set or expands an omitted field to all five v1 kinds. Later kinds do not silently join an existing set.

The set controls both direct assignment and inherited results.

* assignTag accepts only a listed target kind. Otherwise, the server returns 400 BadRequest.  
* A direct read returns a tag only on a listed target kind.  
* An effective read returns a tag only when the queried target kind is listed.  
* An effective read considers assignments only from listed source kinds.  
* An excluded intermediate kind does not stop the parent walk.

Example:

target-types \= \[NAMESPACE, COLUMN\]

namespace sales       \-\> direct assignment allowed  
  table orders        \-\> tag does not apply  
    column ssn         \-\> inherits from namespace sales

A child namespace is still a namespace. It may inherit when NAMESPACE is listed.

target-types is fixed after creation. Changing it could add or remove effective tags across many existing targets. v1 does not define that migration.

The set is stored on the definition, not copied into each assignment. target-types adds no assignment column.

## 2\. Tag assignment

A tag assignment links a definition to a target and stores one selected string in v1. Callers can read assignments for a target or find targets assigned to a definition. The latter is reverse lookup, which supports exact value filtering and pagination by default. An explicit full-result request returns all matching results or an error.

Partitioning, sharding, and physical indexes remain backend choices. Definitions do not embed a growing collection of assigned targets.

An assignment is not a Polaris entity. It has no standalone name, entity ID, version, or restore lifecycle. The tag route and single-target query identify the relation for PUT and DELETE. GET on the same collection lists assignments.

Assign/unassign have no client version precondition or key-based success recognition in v1. Internal conditional writes protect one execution. They cannot distinguish a delayed retry from a new replacement or removal (Part 1, Section 4.4).

A **stable column identity** identifies one column within its containing table. It survives rename and distinguishes a removed column from a same-name replacement.

Only column assignments have a column-id. Assignments to a catalog, namespace, table, or view have no column-id. For a column, this ID identifies the column within its table. v1 uses the Iceberg field ID.

The column ID is unique within its table. While the table retains its identity, that column ID must not be reused for a different column. Each implementation chooses how to represent the ID.

| Target kind | Durable target identity |
| :---- | :---- |
| Catalog | Catalog Polaris entity ID, without a column identity. |
| Namespace | Namespace Polaris entity ID, without a column identity. |
| Table | Table Polaris entity ID, without a column identity. |
| View | View Polaris entity ID, without a column identity. |
| Top-level column | Containing table entity ID and stable column identity, stored separately. |

Within one realm, an assignment stores:

| Field | Type | Meaning |
| :---- | :---- | :---- |
| target-catalog | id-ref | Catalog that contains the target. |
| target-id | id-ref | Target entity ID. For a column, the containing table ID. |
| column-id | optional stable column identity | Used only for a column assignment. v1 uses the top-level Iceberg field ID. |
| tag-catalog | id-ref | Catalog that owns the definition. |
| tag-id | id-ref | Definition Polaris entity ID. |
| selected-value | string | One selected value. Part 1 exposes it as values\[0\]. |

The assignment does not store:

| Data | Reason |
| :---- | :---- |
| Definition version | It checks updates. It does not identify the relationship. |
| Earlier allowed-value lists | v1 keeps no definition history. |
| Copied target kind | The target entity and column ID determine the kind. |
| target-types | The set belongs to the definition. |
| apply-method | Reads derive it from how the assignment reaches the queried target. |
| assigned-at | Reads derive it from the assignment source. |

Within one realm, assignment identity is:

(target-catalog, target-id, column-id, tag-catalog, tag-id)

The selected value is not part of the key. Assigning the same definition to the same target replaces the value.

### 2.1 Same-catalog rule

v1 requires:

target-catalog \= tag-catalog

The API cannot assign a definition from one catalog to a target in another catalog.

Both catalog references remain explicit in the relationship. An implementation may enforce equality in validation, storage, or an equivalent check.

### 2.2 From names to durable identity

The REST API uses names. Polaris resolves them before storing an assignment.

Part 1, Section 2 requires namespace levels to satisfy existing naming rules, including non-empty names without U+001F. The shared target query uses that invisible character to separate names. The rule applies to reads and writes. Query encoding does not change stored IDs, assignment keys, or rename behavior.

* CATALOG: store the catalog entity ID without a column identity.  
* NAMESPACE: resolve the namespace ID without a column identity.  
* TABLE: resolve the table ID without a column identity.  
* VIEW: resolve the Iceberg view ID without a column identity.  
* COLUMN: resolve the table, then resolve the column name in the current Iceberg schema.

The request fields target-type, namespace, target-name, and column are addresses. They are not copied into the relationship.

When reading a stored assignment:

* Without a column-id, target-id identifies the catalog, namespace, table, or view being tagged.  
* With a column-id, target-id identifies the table and column-id identifies the column being tagged.

The target kind comes from the resolved entity type and column ID. The assignment table does not need a copied target-kind column.

Before writing, Polaris checks that kind against the definition's target-types. An excluded kind returns 400 BadRequest.

The rejected write creates no row and changes no assignment key, table, or index.

unassignTag removes an existing relationship and does not re-check target-types.

### 2.3 Rename, recreate, and orphaning

Names locate a definition or target. Stored IDs keep the relationship on the same identity.

An **orphaned tag assignment** points to a target entity ID or tag definition ID that no longer exists. A column assignment is also orphaned when its column ID is absent from the current schema. A soft-dropped target kept for undrop is not orphaned. All orphaned rows are hidden from normal reads and reverse lookup.

| Event | Assignment result |
| :---- | :---- |
| Rename the definition | No change. The relationship still uses tag-id. |
| Update the definition version | No change. Assignments do not store it. |
| Rename a namespace, table, or view | No change while its entity ID remains. |
| Rename an Iceberg column | No change while its field ID remains. |
| Soft-drop a target | Keep the relationship. Normal reads hide it. |
| Permanently remove a target entity | Try best-effort cleanup. Any row left behind is orphaned and hidden. |
| Remove the stored Iceberg field ID | The relationship becomes orphaned and hidden. |
| Permanently delete the tag definition | Any retained assignment row is orphaned and hidden. |
| Recreate the same name | The new identity does not inherit the old assignment. |

### 2.4 API values and stored value

The API uses a list while the v1 model stores one string:

request.values\[0\]  \-\> selected-value  
selected-value     \-\> response.values \= \[selected-value\]

The list is required and duplicate-free. v1 accepts exactly one item.

Reassigning a tag replaces the complete value list. Effective reads never combine values from different hierarchy levels.

The list shape leaves room for possible later forms. It does not commit Polaris to key-only or multi-value tags.

## 3\. Allowed values

Every v1 definition has a non-empty, duplicate-free list of non-empty strings.

Matching is exact and case-sensitive. Polaris keeps list order for display only.

Polaris checks the value when it creates or replaces an assignment. It does not re-check stored assignments when it:

* reads tags.  
* narrows allowed values.  
* runs reverse lookup.

Changing values does not scan, rewrite, hide, delete, or invalidate existing assignments.

A **grandfathered tag assignment** stores a value that is no longer allowed. It remains readable until replaced or unassigned.

### 3.1 Existing values survive a definition update

Before:  
  sensitivity values \= \[public, confidential\]  
  customers.email sensitivity \= confidential

After removing confidential:  
  the stored assignment remains readable  
  a new confidential write fails  
  replacing it with public succeeds

This rule avoids silent reclassification and keeps definition-update cost independent of assignment count.

### 3.2 Replacement uses the current list

assignTag creates or replaces the complete selected-value list. Every successful call must satisfy the current definition.

PUT values=\[confidential\] on a grandfathered confidential assignment  
  \-\> 400 BadRequest  
  \-\> stored assignment unchanged

The old assignment remains because it was valid when written. The new request fails under the current list.

### 3.3 Value identity and rename

Allowed values are strings, not identified objects.

confidential \-\> restricted

is represented as:

remove confidential  
add restricted

Existing confidential assignments stay unchanged. v1 defines no atomic rename or automatic bulk migration.

A later design could add value identity, a rename operation, or migration rules. v1 does not choose one.

### 3.4 History

v1 does not record which earlier allowed-value list accepted an assignment. That would require definition history, assignment snapshots, or an audit record.

## 4\. Column targets

A column is not a Polaris entity in v1. Its logical assignment uses two separate fields:

target-id          \= containing table entity ID  
column-id          \= stable column identity (Iceberg field ID in v1)

Resolving a column needs the current Iceberg schema through the normal metadata-loading path. This may require Object Storage I/O. Within a request, the server may reuse metadata for the same table if it describes the table state used by the operation. A failed metadata load does not show that a field is missing. Performance optimizations may change batching or reuse while preserving these results.

v1 rules:

* the query column contains one top-level name. The response represents it as a one-item list.  
* matching is exact and case-sensitive.  
* only top-level columns are supported.  
* Iceberg tables and whole Iceberg views are supported. Whole views use VIEW targets. View columns are outside v1.  
* GENERIC\_TABLE columns are rejected.  
* nested fields are rejected.  
* a removed field ID makes the assignment orphaned.  
* a same-name replacement field does not inherit the old assignment.

Catalog, namespace, table, and view assignments have no column-id in the logical model. The built-in JDBC store represents these assignments with field\_id \= 0.

Other table implementations may support columns later after defining equivalent stable identity, resolution, and lifecycle rules. v1 adds no generic column-provider SPI.

## 5\. Reads

A **direct tag assignment** is stored on the queried target. An **inherited tag assignment** is stored on a parent but applies to the queried target.

The direct view returns only direct assignments. The effective view also reads parents and returns one final result per definition. Inheritance is computed from direct assignments on the target and its ancestors. Inherited results are not stored as additional direct assignments on descendants.

Cached results must pass the same target-kind and visibility checks as uncached results (Section 5.3). An effective response must use facts that coexisted during its request, whether cached or freshly read. Every effective read considers the complete hierarchy or fails. Cached inherited results never appear as direct assignments.

The **winning assignment** is the closest assignment that passes these rules.

* apply-method explains how the tag reaches the queried target.  
* assigned-at identifies the target that stores the winner.

### 5.1 Direct view

A direct read starts from:

(target-catalog, target-id, column-id)

For each stored assignment, Polaris resolves the definition and target kind.

The result is returned only when the queried target kind appears in the definition's target-types set.

For each result:

* return the stored value as written.  
* return apply-method=DIRECT for every compliant v1 implementation.  
* set assigned-at to the queried target.

### 5.2 Effective view

An effective read resolves this hierarchy:

catalog \-\> outer namespaces \-\> nearest namespace \-\> table \-\> column  
catalog \-\> outer namespaces \-\> nearest namespace \-\> view

For each definition:

1. Determine the queried target kind.  
2. Omit the definition when that kind is not in target-types.  
3. Read assignments on the queried target and every parent.  
4. Ignore assignments stored on target kinds outside target-types.  
5. Continue through excluded intermediate kinds.  
6. Keep the closest remaining assignment.  
7. Return one result for that winner.

The precedence order stays:

column \> table \> nearest namespace \> catalog  
view \> nearest namespace \> catalog

If several namespace assignments remain, the deepest namespace wins. Values from different levels are never combined. v1 has no negative assignment. Missing a direct assignment does not block a parent assignment.

Example:

| Definition target kinds | Assignment source | Queried target | Result |
| :---- | :---- | :---- | :---- |
| NAMESPACE, COLUMN | Namespace sales | Namespace sales | Direct |
| NAMESPACE, COLUMN | Namespace sales | Child namespace sales.eu | Inherited |
| NAMESPACE, COLUMN | Namespace sales | Table sales.orders | No result |
| NAMESPACE, COLUMN | Namespace sales | Column sales.orders.ssn | Inherited from sales |
| NAMESPACE, COLUMN | Column sales.orders.ssn | Same column | Direct (closer than namespace) |

The table between a namespace and column is skipped. It does not stop inheritance.

### 5.3 Read guarantees

Tag reads follow these rules:

* **Complete hierarchy or error.** Every effective read considers the complete hierarchy. Pagination may bound returned tags, but must not truncate ancestor traversal or fall back to direct.  
* **One state per effective response.** Target names, parent chain, definitions, and assignments must have coexisted during the request. This covers full responses and requested pages.  
* **Target-kind filter.** A result appears only when the queried target kind is listed by the definition.  
* **Allowed sources.** Effective reads use only assignments stored on listed target kinds.  
* **Skipped intermediates.** Excluded target kinds do not stop the parent walk.  
* **Target-based visibility.** Once the queried target is authorized, inherited results are not filtered by parent permissions.  
* **Grandfathered values remain valid facts.** They stay visible until replaced or unassigned.  
* **Future methods.** Clients accept future spec-defined method strings. v1 implementations use DIRECT and INHERITED.  
* **Missing and soft-dropped targets stay hidden.** Reads exclude assignments on soft-dropped targets or permanently absent entities, definitions, or fields.

### 5.4 Result origin

Every compliant v1 implementation returns:

* DIRECT when the winner is stored on the queried target.  
* INHERITED when the winner is stored on a parent.

apply-method is a string so clients can accept methods added by a future specification. In v1, servers return only DIRECT or INHERITED, regardless of how the assignment was created.

### 5.5 Reverse lookup

A reverse lookup finds targets with their own assignment. It does not expand inherited descendants.

Every returned row must satisfy all the MATCH conditions below. The pseudocode does not prescribe the order of checks or a backend interface. Pagination is the default. pagination=false requests all eligible results or an error at a documented limit (Part 1, Section 5.6).

LOOKUP direct assignments  
  SCOPE currentRealm, resolvedDefinition.catalog  
  TAG   resolvedDefinition.id

  MATCH all of  
    (request.value is absent OR storedValue \== request.value)  
    target.kind is in resolvedDefinition.targetTypes  
    target entity exists and is not soft-dropped  
    column field still exists, if the target is a column  
    caller may READ\_PROPERTIES on target (containing table for COLUMN)

  DELIVER according to request.pagination (default true)  
    PAGED: request.pageSize, request.pageToken  
    FULL: all eligible results or an error at a documented limit

  RETURN  
    target with resolved names  
    storedValue and direct applyMethod from the matched assignment  
    nextPageToken

The service checks TAG\_READ on the named definition and resolves it before lookup. It checks READ\_PROPERTIES for each returned target, using the containing table for a column. Persistence receives the scoped definition ID, optional exact value, and pagination inputs.

The backend evaluates the predicates it can support, including target checks where available. The service is responsible for checks the backend has not completed. A backend may combine target resolution and validation with the assignment query through joins or native queries.

Both paged and full-result reads enforce finite response-size and work limits, including without deployment overrides. Output count alone cannot bound work spent on rejected candidates. A paged response may stop at the last consumed candidate when it can safely continue. A full-result response must complete or fail before success.

Pagination must advance past rejected candidates without skipping candidates that have not yet been returned or rejected. Fetching a batch does not consume all its rows. If an identified budget prevents safe progress, return 400 BadRequest. Dependency failures remain service errors. Part 1, Section 5.6 defines the parameters and continuation contract. Section 10.5 illustrates one built-in execution strategy.

The operation:

* returns only direct assignments.  
* returns only target kinds listed in target-types.  
* optionally filters by exact, case-sensitive value.  
* can find grandfathered values.  
* hides soft-dropped targets and confirmed orphaned assignments.  
* filters targets denied property-read permission.  
* returns pages of target, value, and direct apply-method results.

**Each result describes one assignment that actually existed.**

Its target, value, and direct method must come from the same assignment state.

A later page may observe later changes. v1 does not require one snapshot across all pages.

Effective reverse lookup is outside v1 because one parent assignment can expand to many descendants.

Denied targets can produce a short or empty page with a continuation token. If the authorization system cannot complete a check, fail the request instead of omitting the candidate. A token resumes the same query and grants no access. Every request is reauthorized (Part 1, Sections 5.6 and 7.3).

## 6\. Overlapping changes

A concurrent operation may change facts that an assignment write relies on. These include the definition, assignment, target identity or existence, and relevant hierarchy or schema. An unrelated target property change does not necessarily conflict. Sections 6.1–6.7 define the allowed outcomes. Some deletion races may leave a hidden orphan row. A write must never attach to a new same-name identity or expose an assignment based on schema facts that are no longer valid.

A request is **subsequent to a write** when it starts after that write returns success, including when another server handles the request. In this section, “later request” has that meaning. A request that overlaps the write follows the operation-specific rules below.

A write **takes effect** when its changes become visible through the Tag API. This is its completion point, even when physical cleanup remains. Completion precedes the success response. An abort before that point leaves the prior definition and assignments unchanged.

A timeout or connection loss leaves the client uncertain about the outcome. Losing the response after completion does not undo the write. Reading again reveals the current state, but does not prove which request produced it.

Concurrent deletion is a specific exception to an assignment becoming visible: the assignment may commit as a hidden orphan (Sections 6.2–6.4). Success in those cases does not make the orphan readable.

The contract does not require global serialization. A backend with a catalog-wide revision may nevertheless report conflicts after unrelated changes.

Implementations may use transactions, locks, compare-and-swap, versions, atomic batches, or provider-specific operations.

### 6.1 Assignment and allowed-value update

The selected value must be allowed when the assignment takes effect. Reading an old definition before the write is not enough.

* If assignTag takes effect before the definition update, its value may become grandfathered when the update removes it.  
* If the update takes effect first, the assignment must use the updated list. A removed value returns 400 BadRequest without changing the assignment.  
* An assignment request starting after the update returns success uses the updated list, even on another server. A removed value remains unavailable until a subsequent update permits it again.

For example, an assignment of public that becomes visible before an update removes public remains readable. If the request only read the old list before that update, it cannot use that earlier read to write public afterward.

### 6.2 Assignment and definition deletion

* If assignTag becomes visible before dropTag checks for assignments, dropTag sees the relationship.  
* If dropTag deletes the definition before assignTag resolves it, assignTag returns NoSuchTag.

When assignTag races dropTag without detach-all=true, the concurrent assignTag request is not guaranteed to return NoSuchTag in every interleaving. A narrow JDBC interleaving may leave an assignment row after the definition is deleted. The row is orphaned. No later Tag API read or reverse lookup may expose it.

### 6.3 Definition deletion with detach-all=true

Part 1 requires TAG\_DROP \+ TAG\_DETACH on the definition.

After authorization, callers observe:

* every assignment and the definition removed, or  
* no change.

A Tag read starting after deletion succeeds must not expose the deleted definition or its assignments, including through another node. Overlapping reads may observe the earlier state. Separate requests straddling deletion need not agree.

Physical assignment cleanup may finish later. An implementation can atomically make the definition unavailable, then reclaim its assignment records separately.

Once the definition is hidden, all Tag reads must also hide its assignments, including results obtained from caches or reverse indexes. Target reads must still build their responses from names, parent chain, definitions, and assignments that existed together (Section 5.3).

If one batch of assignments disappears while another remains, a reader can see a partly deleted tag. Restoring an earlier batch after a later failure cannot undo what that reader already observed.

After deletion succeeds, a physical cleanup failure must not make the definition or its assignments reappear, or turn the completed deletion into a failed Tag write. v1 requires no cleanup worker or completion deadline.

As in Section 6.2, a narrow JDBC interleaving may let an assignTag request resolve the definition before deletion and commit after the assignment cleanup step. The row becomes orphaned when deletion succeeds and remains hidden.

### 6.4 Assignment and target deletion

* If assignment happens first and the target is soft-dropped, keep the relationship for undrop.  
* If the target is later removed, the built-in deletion flow tries best-effort cleanup.  
* Any row left behind becomes orphaned and hidden.  
* If deletion completes before assignTag resolves the target, assignTag fails because the target no longer exists or is soft-dropped.  
* After permanent deletion success, no later Tag API read or reverse lookup may expose an assignment on that identity.

As in Section 6.2, a narrow JDBC interleaving may let an assignTag request resolve the target before permanent deletion and commit after deletion succeeds. The resulting row is orphaned and remains hidden.

### 6.5 Effective read and concurrent writes

One effective-read response must use hierarchy, definitions, and assignments consistent with one logical point in time during that request. The point must fall between the server accepting the request and finishing construction of its response. A coherent state from before the request is insufficient unless those facts still hold at a point during the request.

Reading every ancestor establishes completeness, not consistency or freshness. Separate page requests do not share a promised snapshot.

Putting separate reads inside a transaction is not sufficient by itself. Database reads, cached entity data, and the Iceberg metadata pointer used to resolve columns must describe the same logical point in time. A shared snapshot, validation with retry, or an equivalent provider read may establish that state. Cached facts may be reused when they remain valid at the chosen point. A cache lifetime alone does not establish this.

If the result cannot be established, the read fails rather than mixing facts that never coexisted. The same requirement covers the entire unpaginated effective response. Internal batching must preserve that state across batches.

### 6.6 Reverse lookup and assignment changes

* Each returned item describes an assignment state that existed during that lookup request.  
* A lookup overlapping replacement may return the assignment before replacement or after replacement.  
* A value filter cannot match the old value and return the new value in one item.  
* A lookup starting after unassign or deletion returns success must omit the removed relationship, on any server. A subsequent assignment to that target may be returned.  
* A lookup starting after permanent target deletion returns success cannot expose a row for the removed identity.

The request boundary applies to continuations too. A token identifies where to continue. It does not permit returning an old assignment state. These rules apply per item and do not require all reverse-lookup items to share one snapshot.

target-types is immutable in v1, so reads and writes never race with a change to that set.

The following sequences summarize the boundaries above. They assume no further changes beyond those listed.

| Request sequence | Allowed result | Forbidden result |
| :---- | :---- | :---- |
| An update removes public and returns success. Another server then starts an assignment of public | 400 BadRequest. Existing grandfathered assignments remain readable | A new assignment accepted using a cached old list |
| An assignment reads a list containing public. The update removes it before that assignment takes effect | Revalidate and reject public | Treat the earlier validation as a grandfathered assignment |
| Unassign returns success. Another server starts reverse lookup | Omit the removed relationship | Return it from an old index or continuation state |
| Effective read overlaps assignment replacement | Return a complete state from before or after replacement within the request | Combine hierarchy or assignments that never existed together |
| Assignment replacement returns success before effective read starts | Use a state at or after that replacement | Return a complete but obsolete pre-request state |
| A write takes effect, but its response is lost | Current reads reflect the completed write | Infer rollback solely from the missing response |

### 6.7 Definition writes and retries

Create, update, and rename use the shared key window when support is enabled and a key is supplied. Recognition acknowledges a completed operation without applying it again. Create/update return the current definition with 200. Rename returns 204 without a body. No original response or terminal error is retained (Part 1, Section 3.7).

| Operation | Identity lookup | Current authorization | New-write condition |
| :---- | :---- | :---- | :---- |
| Create | Requested name in the catalog | TAG\_CREATE on the catalog | Name is available after checking recognition. |
| Update | Requested name in the catalog | TAG\_WRITE on the resolved definition | Token matches before replacing editable fields. |
| Rename | Surviving original identity for recognition, source name for a new write | TAG\_DROP on the original definition and TAG\_CREATE on its catalog | Token matches and destination is available. |

Recognizing success does not require a stored fingerprint of request arguments or the original principal. Clients must keep arguments unchanged for one key and use globally unique keys for distinct operations. The server promises no particular outcome for key reuse with different arguments. Realm, catalog, entity type, current authorization, and the operation's lookup scope still apply.

For an actual mutation, the key and definition change must become durable together. Recording success in a separate unprotected step leaves a crash window in which the mutation succeeded but cannot be recognized. Later definition writes must preserve every live record. Retaining only the latest key is insufficient. A recognized retry does not change definition fields, advance the public revision, or renew its recorded expiry.

New update and rename writes atomically check the definition revision. Recognition precedes that current-revision comparison. A new no-op update still checks the token and preserves it, but need not record a key. If unrecorded and followed by another definition change, retrying that no-op may return a version conflict. A separately recorded no-op key must not advance the public revision.

Create/update use ordinary name lookup. A create retry after rename may create a new tag under the original name when it is free. A name occupied without the live key produces the ordinary create conflict. Update does not recover an entity under another name, and its old token cannot authorize a write to a replacement.

Rename preserves identity, assignments, and grants. Its retry lookup must still find the original tag after subsequent renames or name reuse. For example, rename sensitivity to data\_sensitivity, then to classification. An authorized retry of the first request returns 204 while its key remains live, leaving the name classification.

The server may interpret identity information in the opaque version token or use another internal access path. Every lookup stays within the realm, catalog, and tag entity type and excludes deleted definitions. Token decoding grants no permission and adds no public ID-addressed operation. Finding only the original destination is insufficient after another rename.

Before reporting a conflict caused by a concurrent duplicate, read fresh state and recheck whether the winner committed the key. If recognition fails, use the ordinary result. The fresh read must be able to observe the competing commit. Reading the same stale cache or transaction snapshot does not satisfy this rule.

Expiry follows shared request-key capture and lifetime. A full window rejects a new mutation with 503 ServiceUnavailable and Retry-After, without evicting live keys. Disabled support, expiry, or an unreadable key-window encoding uses ordinary operation behavior. A failed persistence read remains a service error.

Deleting the original definition ends recognition, even if storage retains its physical row. A replacement inherits neither assignments nor old success records. Drop has no post-deletion recognition. Assign/unassign have no key window or retained success record in v1. Their ordinary concurrency and deletion guarantees remain in force.

## 7\. Deletion and orphaned assignments

### 7.1 Deleting a definition

v1 permanently deletes a definition.

* Normal drop needs TAG\_DROP.  
* If assignments on live targets exist and detach-all=false, return 400 TagInUse and change nothing. Confirmed orphaned and soft-dropped-target relations do not block deletion.  
* Successful deletion removes or permanently disables all relations of the deleted definition, including those on restorable targets.  
* detach-all=true also needs TAG\_DETACH.  
* The bulk path does not authorize each target separately.  
* Delete releases the name for reuse.  
* v1 has no definition restore.

### 7.2 Unassigning

unassignTag permanently removes one relationship. Assignments have no tombstone or restore operation.

It resolves and authorizes the current definition and target. A missing target returns NoSuchTarget. The failure is never treated as orphan cleanup.

### 7.3 Target lifecycle

| Target state | Assignment result |
| :---- | :---- |
| Exists and is not soft-dropped | Normal reads and writes. |
| Soft-dropped | Keep the relationship. Normal reads hide it. |
| Undropped | Show retained assignments when the same entity ID returns and the tag definition still exists. |
| Permanently removed entity ID | Try best-effort cleanup. Hide any row left behind. |
| Removed Iceberg field ID | Treat the row as orphaned and hide it. |

Normal definition deletion excludes confirmed orphaned rows and assignments on soft-dropped targets from its in-use check. It makes those relations permanently ineffective without requiring detach-all=true. Restoring the target must not restore the deleted tag. An already-restored target must not be treated as soft-dropped for the in-use check. Concurrent restore and normal drop must preserve these outcomes. Soft-dropping a target alone does not erase all its tags.

If checking a column requires metadata that cannot be read, fail the operation without changes. Do not treat unknown existence as confirmed absence.

### 7.4 Cleanup during permanent target deletion

During permanent target deletion, the proposed JDBC path still has the target entity ID. It synchronously tries to delete assignments for that identity.

Cleanup is best-effort. Failure does not fail or roll back target deletion.

A remaining row is orphaned. Normal reads and reverse lookup hide it, and a same-name replacement does not inherit it.

This work is internal lifecycle maintenance, not unassignTag. v1 adds no manual orphan-cleanup API or Tag-specific cleanup privilege.

Reliable reclamation needs a broader Polaris lifecycle design. Grants, Policy mappings, tags, and removed Iceberg fields share the same problem.

### 7.5 Definition history

After permanent deletion, the Tag API cannot load the old definition by name or ID.

A governance or audit record that must remain understandable should save the tag ID, name, values, and definition version with its decision.

v1 adds no Tag-specific audit store.

## 8\. Contract and behavior tests

Behavior tests check that API results follow Parts 1 and 2 across different storage implementations.

An integration may expose Tag v1 only when its backend supports every required operation, including detach-all=true. The tests below check the results of successful operations, failures, and concurrent requests.

Apache Polaris should provide reusable behavior tests. The built-in JDBC path must pass them. Other implementations may run the same suite.

At minimum, tests should cover:

| Scenario | Required result |
| :---- | :---- |
| Default target-types | Omission stores all five v1 kinds and returns the explicit set. Later kinds do not expand it. |
| Invalid target-types | Return BadRequest for explicit null, an empty list, duplicates, or an unknown kind. |
| Immutable target-types | Reject any update presence, including explicit JSON null and an empty list. |
| Direct assignment restriction | Return BadRequest and write no row. |
| Direct read filter | Return the tag only on listed target kinds. |
| Effective target filter | Omit the tag when the queried target kind is excluded. |
| Excluded assignment source | Ignore an assignment stored on an excluded kind and choose the closest allowed source. |
| Skipped intermediate | A namespace assignment can reach a listed column without applying to the table between them. |
| Child namespace | A namespace assignment reaches a child namespace when NAMESPACE is listed. |
| Effective precedence | Keep the closest allowed assignment and never combine values. |
| Effective read completeness | Consider the complete hierarchy for each coherent response or fail. Never downgrade to direct. |
| Default pagination | Omitted parameters request the first page. An empty pageToken also starts there. A supplied pageSize alone limits that first page. |
| Explicit full-result mode | pagination=false requests all results or an error. Combining it with pageToken or pageSize, even empty, returns 400 BadRequest. |
| Full-result request limit | Return 400 BadRequest before success. Direct the caller to remove pagination=false or set it to true. Never silently truncate or change mode. |
| Finite collection budgets | Bound response size and work in both modes, including default deployments. Few visible results must not permit unlimited candidate work. |
| Pagination input validation | Reject invalid or duplicate mode, size, or token parameters. A supplied size must be positive. Full-result mode forbids size and token parameters. |
| Page-size handling | Omitted paged size uses the current finite default, including on continuation. Valid large sizes may be capped. |
| Safe continuation | Resume after consumed candidates. Fetched but unconsumed rows remain reachable. Short or empty pages must make progress. |
| Budget versus dependency failure | Return budget 400 when safe progress is impossible. Preserve service errors for failed metadata, storage, or authorization reads. |
| Full effective-response consistency | Internal batching preserves one coherent state across the entire response, or the read fails. |
| Allowed-value update race | Order by when writes take effect. An earlier validation cannot authorize a removed value after update completion (Section 6.1). |
| Definition delete race | Reads starting after delete success cannot expose assignments for that definition ID, even if a hidden orphan row remains. |
| detach-all=true | Remove every assignment and the definition as one visible change, or change nothing. |
| Deferred physical cleanup | After success, hide retained rows through all reads, caches, and reverse lookup. |
| Cross-node deletion visibility | Reads starting after delete success on any node cannot return the deleted definition or assignments. Overlapping reads may observe the prior state. |
| Normal drop with hidden relations | Confirmed orphaned and soft-dropped-target assignments do not block deletion. Restoring the target cannot restore the deleted tag. |
| Metadata failure during drop | Fail without changes when required existence checks cannot be completed. Do not infer absence. |
| Reverse authorization | Require TAG\_READ and filter each target by READ\_PROPERTIES. Columns use the containing table. |
| Whole-view support | Resolve VIEW independently of TABLE. Whole-view identity and permissions apply, with no column-ID requirement. |
| Deletion abort before taking effect | Preserve definition and assignment visibility. A lost response after completion does not imply rollback. Cleanup failure cannot reverse the deletion. |
| Reverse lookup | Return only direct assignments on listed target kinds. |
| Relationship fan-out | Page direct assignments separately from the definition. Never expose derived cache entries as direct assignments. |
| Unassign existing relationship | Remove it without re-checking target-types. |
| Reverse item consistency | Target, value, and direct method come from one assignment state. |
| Orphan filtering | Never expose a row whose target or tag definition identity is permanently gone. |
| Same-name recreate | Never move an old assignment to the new identity. |
| Allowed-value replacement | Treat remove plus add as string changes, not a rename. |
| Editable definition replacement | Require description and values. Null or empty-string description clears it. Missing description and missing/null values fail. Test raw JSON presence. |
| Definition identity | Return the same ID through load, list, and object-tag results. Preserve it across update/rename, and change it after recreate. |
| Separate rename | Change only the name with a matching token. Preserve assignments, identity, and grants, and reject destination collisions. |
| Rename authorization | Require source TAG\_DROP and catalog TAG\_CREATE. Either missing permission rejects the request. TAG\_WRITE alone is insufficient. |
| Rename with assignments | Permit an authorized rename without TAG\_DETACH. Do not apply the normal-drop TagInUse check. |
| Rename replay | With enabled support, a live matching record, the original tag present, and current authorization, return 204 before current-version comparison without writing again. |
| Replay after later changes | Preserve every live record across later updates or renames, including a rename back to the source. Acknowledging an earlier success must not undo later state. |
| Client key responsibility | Distinct logical operations use distinct keys. Argument changes under one key have no promised response and require no new fingerprint or key-conflict error. |
| Create retry | Recognize a live key under the requested name and return the current Tag. Fresh-read after a concurrent duplicate's name collision. |
| Create after rename | Do not search globally by key. A free requested name can create a new definition. The surviving original may be recognized if it returns to that name. |
| Update retry | Return the current Tag before version comparison, without reapplying earlier fields. Ordinary name lookup does not recover another name. |
| Update no-op retry | A new no-op requires the current token and may omit its key. If omitted, retry after later changes may conflict. Key-only storage must not advance the public revision. |
| Concurrent duplicate update | Fresh-read after a competing commit before reporting its version or conditional-write conflict. Mutation and key commit together. |
| Shared window | Preserve live keys across later writes. Do not renew a recognized key. Reject capacity exhaustion with 503 and Retry-After before mutation. |
| Window failure boundary | Unreadable encoding uses ordinary behavior. A failed persistence read remains a service error. |
| Excluded operations | Drop adds no post-deletion recognition. Assign/unassign add no key window or client version. Missing assignments still return 404. |
| Support discovery | Shared lifetime and endpoint availability are interpreted with the operation matrix. Header parsing alone does not imply operation support. |
| Concurrent duplicate rename | Commit the name change and success record together. Recheck recognition before reporting a version or conditional-write conflict from a competing identical request. |
| Replay authorization | Recheck create/update operation permissions and both rename permissions. Keys grant no access, add no TAG\_READ check, and are not bound to the first principal. |
| Rename replay boundaries | Test source/destination name reuse, expiry, disabled support, and deletion. Never recognize a replacement or a retained physical row for a deleted definition. |
| No-op definition update | For an unrecognized request, require a matching token. Leave definition fields unchanged and return the current token. |
| Token representation | Require a non-empty opaque string. Malformed input returns 400. An unrecognized new write with a stale token returns 409 TagVersionMismatch (Part 1, Section 6). |
| Native definition update | A change through any supported write path invalidates an older token. |
| Same-name definition recreate | A token from the deleted definition cannot update its replacement. |
| Catalog-wide revision | Unrelated changes may cause 409, without changing the requested definition. |
| Column rename and replacement | Preserve the assignment across rename. Never transfer it to a new same-name column identity. |
| Target query round trip | Pass Part 1's namespace, table, view, and column examples. Preserve literal percent sequences and apply only the listed v1 rejection rules. |
| Namespace separator in an element | Reject client encode input before joining. The server rejects empty split levels but cannot infer the original source of a separator. |
| Effective assignment snapshot | Concurrent writes at different hierarchy levels never produce a combination of assignment rows that did not coexist. |
| Effective-read freshness | A complete, coherent state that ceased to hold before request start is rejected, including when another node serves cached data. |
| Reverse-lookup freshness | After unassign succeeds, another node or continuation request does not return the removed relationship unless it has been assigned again. |
| Shared error mapping | Request parsing, version validation, authentication, authorization, catalog absence, and dependency failures use Part 1, Section 6.3 literals. |
| Uncertain write outcome | Dropping the success response after a completed write does not cause rollback or change subsequent read results. |
| Definition lookup after assignment snapshot | Concurrent assignment replacement and definition rename must not produce a combination that never existed (Sections 5.3 and 6.5). Separate statements alone do not establish consistency. |

The built-in JDBC tests should also inject cleanup failure. Target deletion must still succeed, and leftover rows must remain hidden.

## 9\. Illustrative Java data and operations

The records below show data. Section 9.1 sketches the required operations. Neither defines a final Java SPI.

All records are interpreted within one surrounding realm, so realm is not repeated.

enum TargetType {  
  CATALOG,  
  NAMESPACE,  
  TABLE,  
  VIEW,  
  COLUMN  
}

record TagDefinitionId(  
    CatalogId catalog,  
    EntityId entityId) {}

record TagDefinitionData(  
    TagDefinitionId id,  
    String name,  
    Optional\<String\> description,  
    List\<String\> values,  
    Set\<TargetType\> targetTypes,  
    String version) {}

// StableColumnId is an illustrative format-specific identity type.  
record TargetIdentity(  
    CatalogId catalog,  
    EntityId entityId,  
    Optional\<StableColumnId\> columnId) {}

record TagAssignmentData(  
    TargetIdentity target,  
    TagDefinitionId tag,  
    String selectedValue) {}

The shapes mean:

* an absent columnId identifies a catalog, namespace, table, or view.  
* a present columnId identifies a column in the table.  
* v1 binds StableColumnId to an Iceberg field ID. Other formats remain future work.  
* target kind is derived from entity type and column ID, not copied into TagAssignmentData.  
* targetTypes belongs to the definition.  
* selectedValue is not part of assignment identity.  
* apply-method and assigned-at are read results, not assignment fields.

Another implementation may use different language or storage types while preserving the same facts.

### 9.1 Operation sketches

The proposed built-in design keeps Tag rules in shared code and storage access in persistence. A storage backend does not implement the whole readEffective operation. Shared Tag logic reads direct assignments and hierarchy data, applies target-types, and selects the closest assignment.

The table groups request handling and shared durable logic under “shared Tag implementation.” Their class and module boundaries remain implementation choices.

| Work | Shared Tag implementation | Persistence implementation |
| :---- | :---- | :---- |
| Resolve and authorize a request | Resolve names, invoke the authorizer, and resolve Iceberg columns through the metadata-loading path | Read scoped entities and definition records by name or ID |
| Read direct and effective tags | Select the target and ancestors, filter target kinds, compute closest-wins, and construct results | Read direct assignments and definitions for the requested identities, individually or in batches |
| Maintain one effective-read state | Identify every required fact, including cached entities and the Iceberg metadata pointer. Coordinate validation and retries | Supply snapshots or conditional reads that let the caller establish those facts at one point during the request |
| Write definitions and assignments | Validate Tag rules and specify the identities, expected versions, and other preconditions | Check those conditions and apply the write atomically within the required scope |
| Find targets by tag | Authorize the definition, perform remaining target/schema/permission checks, and track consumed candidates | Query by scoped definition ID and optional exact value, with bounded reads and continuation |
| Delete a definition | Determine whether live assignments block normal drop and which relations become ineffective | Atomically remove or hide the definition and preserve assignment invisibility. Physical reclamation may follow |

Persistence must provide both target-to-assignment and definition-to-assignment access, plus conditional definition and relationship writes. Existing entity reads can supply names and hierarchy. The signatures below describe the complete Tag operations. They are not a list of methods each backend must implement.

The shared implementation coordinates consistency across its data sources. A database snapshot does not make an independently stale cache or schema pointer current. If persistence cannot establish a required condition, shared code must retry where appropriate or fail, never construct a mixed result.

Backends may combine reads and supported predicates in joins or native queries. Provider bridges may use a different internal division when they return the same results. This design does not freeze Java interfaces or require a general persistence refactor. Part 1 defines authorization and errors.

NewDefinition carries the creation fields. EditableDefinition carries the complete replacement description and values. The REST layer validates field presence before constructing it. PageRequest specifies the size and token for a bounded internal fetch. Page\<T\> contains items and a continuation token.

The service selects REST delivery mode before building bounded internal fetch requests. REST pagination is the default. For pagination=false, the service may consume multiple internal pages to build one full-result response. Both modes require finite response-size and work limits and safe cursor handling (Part 1, Section 5.6). Internal batching does not determine REST delivery mode.

DefinitionWriteContext below carries optional shared key and expiry information. It illustrates context passed through create/update/rename, not a new public type or a request fingerprint. A request-scoped carrier may supply the same information.

TagDefinitionData createDefinition(  
    CatalogId catalog, NewDefinition fields, DefinitionWriteContext context);  
TagDefinitionData loadDefinition(CatalogId catalog, String name);  
Page\<TagDefinitionData\> listDefinitions(CatalogId catalog, PageRequest page);  
TagDefinitionData updateDefinition(  
    TagDefinitionId tag, String expectedVersion, EditableDefinition replacement,  
    DefinitionWriteContext context);  
void renameDefinition(  
    CatalogId catalog, String source, String destination,  
    String expectedVersion, DefinitionWriteContext context);

void assign(TagDefinitionId tag, TargetIdentity target, String selectedValue);  
void unassign(TagDefinitionId tag, TargetIdentity target);  
List\<DirectTagResult\> readDirect(TargetIdentity target);  
List\<EffectiveTagResult\> readEffective(TargetIdentity target);  
Page\<TaggedTarget\> lookupDirectAssignments(ReverseQuery query, PageRequest page);

void dropDefinition(TagDefinitionId tag, DropMode mode);

ReverseQuery carries the definition's catalog and ID plus an optional exact, case-sensitive value filter (Section 5.5). DropMode selects normal deletion or detach-all=true. Realm remains surrounding context. These illustrative types add no wire fields.

| Work | Required result |
| :---- | :---- |
| Definition create and load | Create a catalog-unique definition. Load it by catalog and name (Part 1, Sections 3.1 and 3.3). |
| Definition list | List names and IDs with pagination by default or explicit bounded full-result mode (Part 1, Sections 3.2 and 5.6). |
| Definition update | Recognize a live key before new-write version comparison. Otherwise check the token and replace editable fields atomically. A new no-op preserves the token (Section 6.7). |
| Definition rename | Authorize the original definition and catalog. Change the name conditionally, or recognize a live matching success without rewriting later state (Part 1, Section 3.6). |
| Assign | Write one currently allowed value on a listed kind. A completed definition update governs subsequent assignments (Sections 1.2, 3.2, and 6.1). |
| Unassign | Resolve and authorize the current definition and target, then remove the relationship. Missing assignments return 404 NoSuchAssignment. Do not re-check target-types (Part 1, Section 4.2). |
| Direct and effective reads | Direct reads use the target. Each effective-read response considers all parents and describes one state during its request, or fails (Sections 5.3 and 6.5). |
| Reverse lookup | Return readable, existing targets of listed kinds, excluding soft drops. Each item describes one assignment state (Sections 5.5 and 6.6). |
| Definition deletion | Live-target assignments block normal drop. With detach-all=true, reads show the definition and all assignments removed together, or no change. Orphans stay hidden (Sections 6.2, 6.3, and 7.1). |

Another request can change data between validation and storage. Whether these steps use one method or several, the implementation must enforce the allowed outcomes in Section 6\.

## 10\. Proposed built-in JDBC layout

This section shows one physical layout for the built-in JDBC path. Other implementations are not required to copy it.

Entity codes, property keys, constraint syntax, and index options below illustrate this layout. Implementations may choose equivalent representations while preserving the required identity and uniqueness rules.

### 10.1 Definition entity row

Tag definitions can use the existing entities table. Relevant columns include:

CREATE TABLE IF NOT EXISTS entities (  
  realm\_id TEXT NOT NULL,  
  catalog\_id BIGINT NOT NULL,  
  id BIGINT NOT NULL,  
  parent\_id BIGINT NOT NULL,  
  name TEXT NOT NULL,  
  entity\_version INT NOT NULL,  
  type\_code INT NOT NULL,  
  properties JSONB NOT NULL DEFAULT '{}'::JSONB,  
  PRIMARY KEY (realm\_id, id),  
  CONSTRAINT constraint\_name UNIQUE  
    (realm\_id, catalog\_id, parent\_id, type\_code, name)  
  *\-- existing lifecycle and internal columns omitted here*  
);

realm\_id distinguishes rows belonging to different tenants. It supplies the realm context omitted from the logical TAG record.

A definition uses the row as follows:

| Entity data | Tag use |
| :---- | :---- |
| catalog\_id, parent\_id | Owning catalog. |
| id | Definition entity ID stored in assignments and serialized as the public opaque string ID. |
| name | Definition name. |
| type\_code | New TAG entity type. |
| properties | Description, allowed values, target-types, and tag version. |
| entity\_version | Existing entity-storage version, separate from tag version unless deliberately bound. |

Find by name:

(realm\_id, catalog\_id, parent\_id, type\_code, name)

Join from assignment:

(realm\_id, id)

Definition properties include:

| Tag definition field | JDBC storage |
| :---- | :---- |
| description | Tag-specific string property. |
| values | Tag-specific ordered-list property. |
| target-types | Tag-specific concrete set property, expanded at creation if omitted. |
| version | API string derived from definition identity and a tag-specific integer property. |

The integer starts at 0 and increments only when the definition changes. A no-op preserves it. Prevent counter wraparound from revalidating an old token.

The API token binds that integer to the immutable definition identity, so a same-name recreation cannot reuse an old token.

Compare the submitted token against the current identity and integer, then protect the write with the existing entity compare-and-swap check.

No new token column is required. The external token format remains opaque.

For this definition-scoped implementation, a no-op preserves the token and unrelated definition updates do not invalidate it.

An internal key-record change may advance entity\_version without advancing the public definition counter. It must still participate in physical conditional writes and cache coherence. Concurrent property or grant writers must not overwrite live keys. Separating the counters does not permit bypassing the entity-storage version check.

### 10.2 Definition operations

Recognition in this table requires enabled support, a supplied live key, the surviving definition, and current operation authorization. Otherwise, use the ordinary operation path.

| Operation | Proposed JDBC action |
| :---- | :---- |
| createTag | Check the requested name for a live key. Otherwise validate values and target types, then insert a TAG child with version 0 and any active key. |
| listTags | List TAG children and return each name with its string ID. |
| loadTag | Find the child by catalog and name. |
| updateTag | Recognize a live key first. Otherwise validate the replacement and token, then conditionally write fields and any active key. Increment the public counter only for definition changes. |
| renameTag | Recognize authorized matching success first. Otherwise resolve the source name, authorize both resources, and conditionally change the name and version. |
| dropTag | Enforce normal or detach-all=true semantics without a partial result. |

The built-in design reuses EntityIdempotency for create, update, and rename. It records keys and expiry in the definition's existing internal properties. An actual mutation persists its record in the same conditional entity write. The illustrative DDL omits those internal columns. No request-fingerprint property, assignment column, or separate request table is required.

Use shared request context so expiry is computed once when the key is captured. The shared settings are polaris.idempotency.enabled and polaris.idempotency.ttl. The inspected upstream configuration defaults to disabled support and PT5M. These defaults are implementation settings, not Tag protocol constants (Part 3, Section 7.13).

The helper stores key and expiry, without original arguments, principal, HTTP body, or status. Shared Tag handling supplies operation-specific lookup, current authorization, response construction, and conflict rechecks. Preserve live keys through subsequent entity writes. An update no-op may skip persistence, or record its key without advancing the public definition counter.

Create/update locate records by name. Rename may decode definition identity from the opaque token, then load the original tag within the resolved realm and catalog. Check entity type, deletion state, and current permissions before recognizing success. Destination-name lookup alone cannot cover later renames.

The shared helper treats unreadable or unsupported window encodings as empty. This does not permit treating an entity-read failure as absence. Capacity exhaustion rejects before mutation, preserving live keys and the shared Retry-After response.

These paths describe the proposed integration. Existing helpers and entity lookup do not by themselves implement Tag retries. Part 3, Section 7.13 records the inspected source behavior and its boundaries.

The public ID is derived from the existing entity ID, not from the name or version counter. Reads and list projections return the name and ID from the same definition. Entity rekeying and migrations must preserve the identity contract.

### 10.3 Assignment table

Assignment rows store the definition ID. Reads obtain target-types from that definition.

CREATE TABLE IF NOT EXISTS tag\_assignment\_record (  
  realm\_id TEXT NOT NULL,  
  target\_catalog\_id BIGINT NOT NULL,  
  target\_id BIGINT NOT NULL,  
  field\_id INTEGER NOT NULL DEFAULT 0,  
  tag\_catalog\_id BIGINT NOT NULL,  
  tag\_id BIGINT NOT NULL,  
  tag\_value TEXT NOT NULL,  
  PRIMARY KEY (  
    realm\_id,  
    target\_catalog\_id,  
    target\_id,  
    field\_id,  
    tag\_catalog\_id,  
    tag\_id  
  )  
);

| Logical scope or field | JDBC representation |
| :---- | :---- |
| Realm scope | realm\_id |
| target-catalog | target\_catalog\_id |
| target-id | target\_id |
| column-id | field\_id: Iceberg field ID for a column assignment, or 0 for a catalog, namespace, table, or view assignment |
| tag-catalog | tag\_catalog\_id |
| tag-id | tag\_id |
| selected-value | tag\_value |

The implementation must enforce target\_catalog\_id \= tag\_catalog\_id.

The table does not copy target kind, target-types, definition version, old allowed values, apply-method, or assigned-at.

### 10.4 Indexes

The primary key begins with target identity and supports direct reads.

A tag-leading index supports reverse lookup:

CREATE INDEX tag\_assignment\_record\_by\_tag  
  ON tag\_assignment\_record (  
    realm\_id,  
    tag\_catalog\_id,  
    tag\_id  
  );

A tag-and-value index supports value filtering:

CREATE INDEX tag\_assignment\_record\_by\_tag\_value  
  ON tag\_assignment\_record (  
    realm\_id,  
    tag\_catalog\_id,  
    tag\_id,  
    tag\_value  
  );

The API limits both allowed and selected values to 2000 UTF-8 bytes (Part 1, Section 1.3). In this layout, the selected value is part of the composite reverse-lookup index. Definition and assignment validation apply the same byte limit. A value accepted in a definition therefore passes the assignment's size check.

[PostgreSQL limits each B-tree index entry](https://www.postgresql.org/docs/18/btree.html) to approximately one-third of a page. The complete entry includes realm\_id, tag\_catalog\_id, tag\_id, tag\_value, and index overhead. The storage and index design must accommodate the API value limit for supported realm identifiers and database configurations.

An indexed query may read the assignment and validate its target in the same statement. A separate assignment read is unnecessary when that statement already establishes the required result.

Every access path must find matching current direct assignments on existing, non-soft-dropped targets of listed kinds. Returned targets require property-read permission. Each item's target, value, and direct method must come from one assignment state (Sections 5.5 and 6.6).

An asynchronously maintained index can omit recent assignments as well as return stale entries. An implementation using that index must handle both cases. Rechecking returned hits cannot find assignments that the index omitted.

target-types requires no new assignment index. Extra covering or ordering columns remain JDBC optimization choices.

### 10.5 Operation paths

| Operation | Proposed JDBC path |
| :---- | :---- |
| assignTag | Resolve definition and target, check allowed value and target kind, then insert or replace one row. |
| unassignTag | Resolve and authorize the current definition and target, then delete one row. |
| Direct read | Scan the target-key prefix, join definitions, apply the target-kind filter, and return valid rows. |
| Effective read | Resolve the parent chain, join definitions, apply source and queried-kind filters, then keep the closest row. |
| Reverse lookup | Query assignments by tag and optional value, apply available target checks, then batch remaining target resolution. The flow below shows pagination and column checks. |
| dropTag | Delete definition and assignments in one JDBC transaction for detach-all. Live-target assignments block normal drop. Concurrent writes may leave hidden orphans (Section 6.2). |
| Permanent target deletion | Try synchronous cleanup while the target ID is available. Hide any row left behind. |
| Iceberg field removal | Recheck field IDs against the current schema and hide missing fields. |

**Proposed built-in reverse-lookup flow**

The diagram shows one batch after TAG\_READ authorization and definition resolution. Per-target property-read checks remain before returning results. It illustrates one execution strategy without defining a backend interface.

![][image1]

The database filters by scope, tag ID, and optional exact value before returning rows. It may join target data in the same query. The service batches any remaining entity lookups. Column checks may reuse metadata for each distinct table within the request, under the rules in Section 4\. Loading metadata may require Object Storage reads. These batching and reuse choices can be optimized separately.

An assignment whose target or field no longer exists is hidden. Failure to load metadata does not prove that a target is gone. If the read cannot establish the required result, it fails instead of silently dropping the candidate (Section 5.3).

Response values come from the matched assignment records. Each item's target, value, and direct method must describe the same assignment state. A value filter cannot match the old value and return its replacement (Sections 5.5 and 6.6).

Enforce finite response-size and work budgets around the applicable database, metadata, and authorization work. Bounded batches alone do not bound total request work. Exact counters, configuration values, and enforcement points remain implementation choices.

Advance pagination past returned or rejected candidates. Fetched but unconsumed candidates remain reachable on continuation. The diagram does not prescribe a loop that fills every page. A paged response may stop early when it can return a safe advancing token. If an identified budget prevents safe progress, return 400 BadRequest. In full-result mode, complete the result or fail before success. Dependency failures remain service errors (Part 1, Section 5.6).

Permanent target deletion can succeed even if assignment cleanup fails, because Tag reads hide assignments for the removed target. With detach-all=true, reads must hide the definition and all its assignments together. Implementations may reclaim the hidden rows synchronously, through hooks or later reconciliation, or leave them stored.

## 11\. Possible later extensions

These possibilities are not part of v1 and are not commitments.

### 11.1 Changing target-types

A later design could allow the set to change. It would need to define:

* existing direct assignments on a removed kind.  
* newly inherited results on an added kind.  
* what effective reads may observe while the set changes.  
* reverse lookup during migration.  
* authorization and audit.  
* whether a catalog-wide scan is required.

v1 avoids these questions by making the set immutable.

A single set cannot express "namespace may store the assignment but child namespaces may not inherit it." Separate source and destination rules would be needed.

### 11.2 Rule-driven assignments

A later rule engine could search within a catalog or namespace, then create ordinary assignments on concrete matching targets.

Rule identity, selectors, timing, ownership, and match evidence would remain separate from assignment identity.

### 11.3 Key-only, free-form, and multi-value forms

A later design could add an explicit value mode.

* Free-form could keep one text value and skip list membership checks.  
* Key-only could keep assignment identity but represent no selected value.  
* Multi-value could keep assignment identity but use another value representation.

Existing controlled-tag behavior would need to remain unchanged.

### 11.4 Allowed-value rename or migration

A later design could add value identity, rename, or bulk migration.

It would need rules for assignment changes, all-or-nothing behavior, reverse lookup, authorization, audit, and old clients.

### 11.5 Reliable cleanup after target deletion

A shared Polaris lifecycle design could cover grants, Policy mappings, tag assignments, and removed Iceberg fields.

Possible tools include deletion records, tombstones, retryable workers, bounded reconciliation, or provider-specific cleanup.

### 11.6 Future authorization input

An **authorizer** is a built-in or external component that returns allow or deny. A later authorizer could use one target's complete effective tags.

Useful facts include definition ID, current name, and selected values. The authorization path must apply target-types, inheritance, and closest-wins rules internally.

It must not call the public Tag REST endpoint. If complete effective tags cannot be resolved, authorization must fail rather than treat the target as untagged.

Part 3 compares these options and larger alternatives.

# Appendix

# Part 3: Design context and future extensions

Parts 1 and 2 define the Tag v1 contract. This part explains the main choices, compares nearby systems, and records possible later work.

This part uses these terms:

* A **target** is one catalog object or top-level Iceberg column.  
* A **target kind** is CATALOG, NAMESPACE, TABLE, VIEW, or COLUMN.  
* A **tag definition** names one classification and declares its values and target kinds.  
* The definition's **target-types** set is stored, non-empty, and immutable. Creation defaults an omitted field to all five v1 kinds.  
* A **tag assignment** stores one selected value for one definition on one target.  
* A **direct assignment** is stored on the queried target.  
* An **inherited assignment** is stored on a parent and applies to the queried target.  
* The **assignment source** is the target that stores an assignment used by a read.  
* The **effective view** reads the target and its parents, then keeps the closest valid assignment for each definition.  
* A **reverse lookup** starts from one definition and returns targets with their own assignment.  
* An **orphaned assignment** refers to a permanently removed target entity, tag definition, or Iceberg field ID. Normal reads and reverse lookup hide it.  
* The **durable logical model** defines the facts and results that every implementation must preserve.  
* The **built-in JDBC implementation** is the proposed out-of-the-box implementation that Apache Polaris would ship and test.  
* A **provider** may serve the same API through another system.  
* An **authorizer** returns an allow or deny decision.

This part answers eight questions:

1. Which industry patterns support the v1 model?  
2. Which Polaris Policy patterns can Tags reuse?  
3. Why does target-types use one immutable set?  
4. How does the assignment URI identify a relationship without a separate ID?  
5. Which later tag forms could keep the current identity model?  
6. What would allowed-value rename require?  
7. How should Polaris handle relationship cleanup after target deletion?  
8. Can future authorizers consume effective tags without changing Tag identity?

## 1\. Industry signals

The comparison is evidence, not a parity checklist. Polaris keeps the patterns that fit its catalog and authorization model.

Product notes in Section 4 link to public documentation. They compare documented API behavior, without inferring hidden storage identity from public name-based addresses. The GCP references use Dataplex and Knowledge Catalog naming.

### 1.1 Systems used in the comparison

| System | Useful signal |
| :---- | :---- |
| Snowflake | Definition and assignment split, values on assignments, inheritance, reverse lookup, rich provenance, automation, and restore. |
| Databricks UC | Governed definitions with controlled values and a separate attribute-based access-control layer. |
| Apache Gravitino | Managed tag entities, assignment values, inherited reads, and objects-by-tag lookup. |
| DataHub | Tag and glossary models, field-level associations, and search. |
| AWS LF-Tags | Controlled key/value definitions, inheritance, reverse lookup, and tag-based grants. |
| GCP Knowledge Catalog | Typed metadata definitions and field-level attachment. |
| BigQuery policy tags | A separate taxonomy used for column enforcement. |
| Apache Atlas | First-class column identity, propagation, search, and external enforcement. |
| Free-form resource tags | A contrast: simple labels without one governed definition. |

### 1.2 Definition, value, and assignment model

| System | Definition scope | Assignment value |
| :---- | :---- | :---- |
| Snowflake | Schema | One or multiple strings |
| Databricks governed tags | Account | Controlled value or key-only |
| Apache Gravitino | Metalake | Zero, one, or multiple strings |
| DataHub | Global tag or glossary registry | Tag associations have no value |
| AWS LF-Tags | Account / Data Catalog | One allowed value |
| GCP Knowledge Catalog | Project / location | Typed field values |
| Apache Atlas | Instance-wide type registry | Label or typed attributes |
| Polaris v1 | Catalog | One selected string |

The common pattern is a managed definition plus a separate relationship to a target. Polaris adds a required controlled value list and one selected value per assignment.

The reviewed sources do not establish a peer field with the same semantics as Polaris target-types. The field is a Polaris design choice. It lets one definition classify selected target kinds without changing the catalog hierarchy.

### 1.3 Value-constraint changes

| System | How value constraints change |
| :---- | :---- |
| Snowflake | Existing assignments remain. Later writes use the new list. |
| Databricks UC | A submitted allowed-value list replaces the previous list. |
| Apache Gravitino | Value constraints are fixed at creation. |
| AWS LF-Tags | Removal is blocked while the value is still attached. |
| GCP Knowledge Catalog | Aspect templates can be updated. Fields can be deprecated but cannot be deleted. |
| Polaris v1 | Existing assignments remain readable. Later writes use the new list. |

Polaris preserves accepted classifications. Updating a definition does not silently rewrite assignments.

Allowed values are strings, not identified objects. Replacing confidential with restricted is removal plus addition. It is not an identity-preserving rename.

### 1.4 Identity, reads, and enforcement

| System | Column address or identity | Effective read or propagation | Reverse lookup | Enforcement |
| :---- | :---- | :---- | :---- | :---- |
| Snowflake | Object and column names in public lookup | Direct and inherited tags. Separate propagation features | Tag-reference functions and views | Separate policy features |
| Databricks UC | Name fields in SQL views | ABAC-only inheritance, excluding columns | SQL tag views | Separate ABAC policies |
| Apache Gravitino | Object type and full name | Direct and inherited reads | Objects-by-tag API | Outside the compared tag API |
| DataHub | fieldPath | Entity and field associations | Search index | Outside the compared tag API |
| AWS LF-Tags | Column name | Database to table to column inheritance | LF-Tag search APIs | Lake Formation grants |
| GCP Knowledge Catalog | Schema field metadata | Entry and column aspects | Catalog search | BigQuery policy tags enforce separately |
| Apache Atlas | First-class column GUID | Classification propagation across relationships | Search by classification | Outside the compared classification API |
| Polaris v1 | Table entity ID plus Iceberg field ID | Direct and effective views over catalog containment | Direct assignments only | No enforcement in v1 |

Polaris uses these comparisons to motivate five design choices:

* definitions and assignments should have separate identity.  
* a column assignment should survive rename.  
* a same-name replacement must not inherit the old assignment.  
* effective reads and reverse lookup are both useful.  
* classification storage should stay separate from authorization enforcement.

## 2\. How Tags compare with Polaris Policy

Polaris Policy is the closest in-tree pattern. Both features use a managed definition and a separate target relationship.

### 2.1 Definitions

| Point | Tag definition | Policy definition |
| :---- | :---- | :---- |
| Parent | Catalog | Namespace |
| Main fields | ID, name, description, allowed values, target-types, version | Name, policy type, description, content, version |
| Name update | Separate renameTag operation | Not part of UpdatePolicyRequest |
| Type field | No subtype in v1 | Required policy type |
| Update token | Opaque string | Integer version |
| Target-kind rule | Immutable stored set, defaulted when omitted on creation | No equivalent field in the current Policy API |
| Drop while linked | Live-target assignments block normal deletion. Confirmed orphaned and soft-dropped-target relations do not | Policy-in-use unless detach-all=true |

Policy content is interpreted by a policy type. A tag carries a controlled classification, not an enforcement rule.

The proposed built-in JDBC implementation can store both definitions in the shared entity table. target-types belongs to the tag definition properties. It is not copied into assignments.

### 2.2 Relationships

| Point | Tag assignment | Policy mapping |
| :---- | :---- | :---- |
| Route | /tags/{tag-name}/assignments | /policies/{policy-name}/mappings |
| Create or replace | PUT | PUT |
| Remove | DELETE, without a body | POST |
| Target address | Required target type and query fields | Type and path in the request body |
| Relationship data | One selected value | Optional parameters |
| Client-visible relationship ID | None | None |
| Direct target restriction | Definition target-types | Policy-type and attachment rules |

Both APIs put the definition in the route. Tags identify the target in query parameters and use PUT/DELETE. This Tag change does not migrate the existing Policy API.

The built-in JDBC assignment row does not store target-types. Reads resolve the definition and apply the set then.

### 2.3 Reads and visibility

| Point | Tags | Policy |
| :---- | :---- | :---- |
| Direct read | getObjectTags(view=direct) | No direct-only public operation |
| Effective read | getObjectTags(view=effective) | getApplicablePolicies |
| Result origin | apply-method and assigned-at | inherited and policy namespace |
| Target-kind filter | Definition target-types | Policy-type behavior |
| Reverse lookup | Direct tag assignments | No public equivalent |

Tag effective reads add one rule that Policy does not need: the queried target kind and assignment source kind must appear in target-types. An excluded intermediate kind is skipped, not treated as a barrier.

Tag shares these authorization patterns with Policy:

* create and list check the parent.  
* load, update, and drop check the definition.  
* relationship writes check the definition and target.  
* target reads authorize the queried target once.

Tag rename additionally checks drop permission on the original definition and create permission on its catalog.

Tag reverse lookup checks the definition and filters targets by property-read permission. Bulk detach requires TAG\_DROP and TAG\_DETACH on the definition, without per-target checks (Part 1, Section 7.1).

### 2.4 Possible future Policy use

The current Policy feature already provides versioned definitions, typed content, mappings, and applicable reads.

It does not yet define a portable authorization language. Missing pieces include:

* subject and action fields.  
* allow and deny effects.  
* tag predicates.  
* rule combination.  
* conflict handling.  
* translation to built-in, OPA, Ranger, or other authorizers.

A later proposal could use Policy as an administration foundation. Tag v1 does not promise that model.

### 2.5 Encoding and revision boundaries

Tags reuse Policy's definition and relationship pattern. Part 1 selects Iceberg namespace query encoding and specifies one URI-encoding step for every target query value.

A nested namespace such as \["sales", "eu"\] contains two names. GET must preserve where one name ends and the next begins. Table and column parameters each contain only one name. URI query-value encoding preserves reserved characters in all three parameters.

Naming restrictions do not remove that need. The built-in validator accepts names such as R\&D and tax%20rate. Incorrect encoding or a second decoding pass can change them.

Each option first represents the namespace names as one string, then encodes that string for the URL. U+001F is the invisible character Iceberg uses to separate names. It appears as %1F in the URL:

| Option | Namespace boundary | Main cost | v1 decision |
| :---- | :---- | :---- | :---- |
| Iceberg query encoding | Join names with U+001F, then URI-encode the whole value | Excludes the separator inside names. Some ingress rules reject it between names | Retained for v1 to limit scope. Section 7.12 records the trade-off |
| JSON array | JSON preserves separate names and their contents, followed by URI encoding | Longer URLs and an additional JSON parsing contract | Deferred candidate. Avoids inserting a control separator between ordinary names |
| Printable escaping | A delimiter and escape grammar preserve levels, followed by URI encoding | Must select, specify, and maintain the exact grammar | Nessie is prior art. v1 does not introduce another grammar |

The built-in validator already rejects U+001F inside namespace names. Tag v1 applies this supported-name restriction to every provider and every shared target query. Clients enforce it before joining levels. The joined query cannot prove where each separator originated.

A native system may allow this character in a namespace name. Tag v1 cannot address that namespace or its tables, views, and columns. The restriction describes what Tag v1 supports, not what every native system allows.

The query helpers join and split original namespace levels. Iceberg's path helpers use a different encoding path. A handler receiving decoded query values must not call the path decoder.

For \["sales", "eu"\], the selected query value is sales%1Feu. The JSON alternative is %5B%22sales%22%2C%22eu%22%5D. Neither option removes the need to encode table and column values.

A deployment must preserve the encoded query through its client, ingress, HTTP layer, and handler. A codec's local round trip alone cannot establish that behavior. Path-level Servlet restrictions also do not establish a query failure.

Section 5.9 defines the deferred shared transport work. Section 7.12 compares candidate formats and the validation needed before adoption. Section 7.11 identifies public primary sources. The selected encoding applies to Tag target queries and leaves existing Policy and Iceberg API contracts in place.

Tag definition versions are opaque strings. This lets JDBC keep an integer revision internally while other backends use native revisions. Policy retains its separate integer-version contract.

## 3\. Why target-types uses one immutable set

### 3.1 The use case

One definition may need to classify namespaces and columns, but not tables.

target-types \= \[NAMESPACE, COLUMN\]

namespace sales       \-\> direct assignment allowed  
  table orders        \-\> no effective tag  
    column ssn         \-\> inherits from namespace sales

The table is skipped. It does not block inheritance from the namespace to the column.

A child namespace is still a namespace. It may inherit because NAMESPACE is listed.

### 3.2 Normative meaning

Each definition stores one non-empty, immutable target-types set. Creation may omit the field to select all five v1 kinds. An explicit subset keeps the definition narrower. The set controls four outcomes:

1. assignTag accepts only listed target kinds.  
2. A direct read returns the tag only on a listed queried kind.  
3. An effective read returns the tag only on a listed queried kind.  
4. An effective read considers assignments only from listed source kinds.

The parent walk still follows the normal hierarchy. Excluded intermediate kinds do not stop it.

Closest-wins order remains:

column \> table \> nearest namespace \> catalog  
view \> nearest namespace \> catalog

Reverse lookup remains direct-only. target-types never expands one namespace assignment into every descendant column.

### 3.3 Why one set

| Option | Benefit | Cost |
| :---- | :---- | :---- |
| One set for direct and inherited use | Small model and clear definition meaning | Cannot separate assignment sources from inherited destinations |
| Separate direct-source and inherited-destination sets | More expressive | Adds another field, more validation, and more migration rules |
| Inheritance barriers on target kinds | Can stop traversal at selected levels | Does not express “skip table but continue to column” cleanly |
| Per-assignment target rules | Fine-grained | Duplicates definition policy on every relationship and can drift |
| General predicate language | Maximum flexibility | Turns a tag definition into a rule engine |

V1 chooses one set because it solves the current use case with one rule:

A tag definition states which target kinds it may classify.

The same rule applies whether the classification is direct or inherited.

Defaulting simplifies creation without removing the narrower-set use case. Persisting the expanded set also prevents a later target kind from silently changing old definitions. Explicit null remains invalid because it is not an omitted default.

### 3.4 Why the set is immutable

Changing target-types has immediate live effects.

Removing TABLE could remove the tag from many current tables. Adding COLUMN could make many existing columns inherit the tag.

A mutable field would require decisions about:

* existing direct assignments on removed kinds.  
* large effective-state changes.  
* concurrent reads and updates.  
* reverse lookup during migration.  
* future authorization decisions that consume effective tags.  
* rollback after partial failure.

V1 avoids those questions by fixing the set at creation. A later design could add an explicit migration operation or require a new definition.

### 3.5 Known limit

One set cannot express this rule:

namespace may store a direct assignment  
child namespace must not inherit it  
column may inherit it

That design needs separate source and destination rules. V1 records the limit instead of adding unused fields.

### 3.6 Testable requirements

* creation defaults an omitted list to all five v1 kinds and persists the explicit set.  
* creation rejects explicit null, empty, duplicate, or unknown target-types.  
* update rejects any presence of target-types, including explicit JSON null and an empty list.  
* assignment to an excluded kind returns BadRequest and writes nothing.  
* direct reads omit the tag on an excluded queried kind.  
* effective reads omit the tag on an excluded queried kind.  
* effective reads ignore assignments from excluded source kinds.  
* excluded intermediate kinds do not stop the parent walk.  
* child namespaces inherit when NAMESPACE is listed.  
* reverse lookup returns direct assignments only on listed kinds.  
* unassign removes an existing relationship without re-checking target-types.  
* assignment identity depends on the definition and target identities, and assignment rows do not store target-types.

## 4\. Product notes

This section keeps only the product facts used by the proposal.

### 4.1 Snowflake

* Tags are schema-scoped objects with string-valued assignments. Multi-value tags can carry several strings on one target.  
* Allowed-list narrowing leaves existing assignments unchanged.  
* Tags inherit through the object hierarchy. Propagation is a separate feature.  
* Tag-reference functions and views expose assignments and their provenance.  
* UNDROP TAG can restore a tag and its assignments within the retention period.  
* Future grants of privileges on tags are not supported.

These behaviors are documented in [object tagging](https://docs.snowflake.com/en/user-guide/object-tagging/introduction), [ALTER TAG](https://docs.snowflake.com/en/sql-reference/sql/alter-tag), [TAG\_REFERENCES](https://docs.snowflake.com/en/sql-reference/functions/tag_references), and [UNDROP TAG](https://docs.snowflake.com/en/sql-reference/sql/undrop-tag).

Polaris adopts separate definitions and assignments, selected values, and provenance. It chooses catalog scope, required controlled values, permanent v1 deletion, and one immutable target-kind set. Polaris column identity is defined by its own table-ID and Iceberg-field-ID contract.

### 4.2 Databricks UC

* Governed tags are account-scoped definitions with optional controlled values. Key-only governed tags are supported.  
* Updating the allowed-value list replaces it. Creating governance over existing tags can leave earlier out-of-policy assignments in place.  
* Tag inheritance applies during ABAC evaluation and excludes columns.  
* SQL tag views address columns through catalog, schema, table, and column names.

Sources: [governed tag management](https://docs.databricks.com/aws/en/admin/governed-tags/manage-governed-tags), [ALTER GOVERNED TAG](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-governed-tag), [tag inheritance](https://docs.databricks.com/aws/en/database-objects/tags), and [COLUMN\_TAGS](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/column_tags).

Polaris follows the governed-definition pattern. It requires controlled values, keeps ABAC outside Tag v1, and uses Iceberg field IDs for column identity.

### 4.3 Apache Gravitino

* Tags are metalake-scoped managed entities.  
* Assignments can carry zero, one, or multiple string values.  
* Value constraints are fixed at creation and can permit any value, no value, or a specified list.  
* Reads include inherited tags. Direct assignments override inherited values for the same tag.  
* Objects-by-tag lookup returns direct assignments and supports a value filter.

Sources: the public [tag model](https://github.com/apache/gravitino/blob/main/docs/tags.md) and [tag API documentation](https://github.com/apache/gravitino/blob/main/docs/manage-tags-in-gravitino.md). These describe the inspected development documentation, not a claim about every released version.

Both designs separate definitions from assignments and compute inherited results on reads. Polaris v1 requires one selected value and gives Iceberg columns a table-ID and field-ID key.

### 4.4 DataHub

* Tags are reusable label entities with URNs.  
* Tag associations carry a tag reference rather than a per-target classification value.  
* Column associations use fieldPath within a dataset.  
* Tags support search and discovery. A tag entity has lifecycle status that can represent soft deletion.

Sources: [tags](https://docs.datahub.com/docs/tags/), the [Tag entity model](https://docs.datahub.com/docs/generated/metamodel/entities/tag/), and the [Tags API tutorial](https://docs.datahub.com/docs/api/tutorials/tags/).

DataHub illustrates search and field-level metadata. Polaris uses controlled values and stable Iceberg field IDs in place of field paths for durable column identity.

### 4.5 AWS LF-Tags

* LF-Tags belong to an account's Data Catalog and contain allowed values.  
* Assignments inherit from database to table to column. Direct values override inherited values.  
* Search APIs find resources by LF-Tag expressions. Lake Formation grants control access.  
* Allowed-value removal is blocked while the value is attached to a resource.

Sources: [LF-Tag inheritance and access control](https://docs.aws.amazon.com/lake-formation/latest/dg/tag-based-access-control.html), [LF-Tag assignment](https://docs.aws.amazon.com/lake-formation/latest/dg/TBAC-assigning-tags.html), [UpdateLFTag](https://docs.aws.amazon.com/lake-formation/latest/APIReference/API_UpdateLFTag.html), and [SearchTablesByLFTags](https://docs.aws.amazon.com/lake-formation/latest/APIReference/API_SearchTablesByLFTags.html).

LF-Tags illustrate controlled values, inheritance, discovery, and separate enforcement. Polaris differs by retaining assignments whose values were removed from the definition.

### 4.6 GCP metadata and policy tags

* Knowledge Catalog aspect types define typed metadata in a project and location.  
* Aspects attach to entries and columns.  
* Aspect templates can change, but existing fields cannot be deleted. Fields can be marked deprecated.  
* BigQuery policy tags are a separate column-security taxonomy.

Sources: [aspect types and metadata](https://docs.cloud.google.com/dataplex/docs/enrich-entries-metadata) and [BigQuery column-level access control](https://docs.cloud.google.com/bigquery/docs/column-level-security-intro).

Polaris keeps one string-based classification model. It separates classification storage from enforcement.

### 4.7 Apache Atlas

* Classifications are managed types and can carry attributes.  
* The Hive model represents columns as entities with stable GUIDs.  
* Classification propagation follows configured relationships.  
* Classification is a search dimension.

Sources: the [type system](https://atlas.apache.org/2.0.0/TypeSystem.html), [classification propagation](https://atlas.apache.org/2.0.0/ClassificationPropagation.html), and [search API](https://atlas.apache.org/api/v2/resource_DiscoveryREST.html).

Atlas illustrates first-class column entities. Polaris avoids adding column entities and uses Iceberg field IDs in v1.

## 5\. Possible later extensions

These paths are examples, not commitments.

### 5.1 Rule-driven assignment

A later rule could search within a catalog or namespace, then create ordinary assignments on matching targets.

The rule scope would limit candidate search. The definition's target-types set would still control which matches may receive assignments.

A rule design would need:

* selector and scope.  
* current-only, future-only, or both.  
* one-time or continuous maintenance.  
* ownership and version.  
* retries and reconciliation.  
* conflict rules when several rules match.

Rule state should stay separate from assignment identity.

### 5.2 Multi-value assignments

The REST field is already a list, but v1 accepts one member. The design leaves room for several values on the same assignment. It does not represent them as duplicate assignments for the same tag and target.

A later multi-value contract would need to define:

* capability negotiation.  
* contains-any, contains-all, or exact filtering.  
* response ordering.  
* JDBC migration.  
* old-client behavior.  
* rule and manual-write interaction.

The current assignment identity and closest-wins rule could remain.

### 5.3 Key-only and free-form tags

One possible design could add an immutable value-mode:

| Illustrative mode | Definition values | Assignment values |
| :---- | :---- | :---- |
| CONTROLLED | Required non-empty list | One current member in v1 |
| FREE\_FORM | Empty | One arbitrary non-empty string |
| KEY\_ONLY | Empty | Empty list |

An explicit mode is clearer than inferring behavior from an empty list.

Free-form mode could reuse scalar value storage. Key-only mode would need a missing-value representation. Changing modes would require migration rules.

### 5.4 Changing target-kind rules

A later design could support one of these models:

| Model | Use |
| :---- | :---- |
| Explicit migration of target-types | Change one definition while checking existing assignments and effective results |
| Separate direct-source and inherited-destination sets | Allow a namespace source without child-namespace inheritance |
| Inheritance barriers | Stop traversal at selected kinds |
| New definition plus assignment migration | Keep v1 definitions immutable |

Any change must define concurrency, authorization, effective-read transitions, and rollback.

### 5.5 Allowed-value rename or migration

| Model | Benefit | Main cost |
| :---- | :---- | :---- |
| Bulk string migration | Keeps current value storage | Needs scope, retry, authorization, and conflict rules |
| First-class value IDs | Preserves identity across rename | Changes definitions and assignments |
| Aliases or deprecated names | Helps old writers and readers | Needs canonical output and retirement rules |
| Remove plus add | Keeps v1 small | Callers migrate assignments separately |

Hidden IDs alone do not solve rename. The update must carry explicit rename or migration intent. Section 7.8 compares user-declared IDs and the cost of keeping strings in v1.

### 5.6 Assignment addressing and later identity

V1 addresses one relation using the definition route plus a single-target query. PUT creates or replaces it. DELETE removes it without a request body. The assignment does not gain a separate durable ID.

A later independent assignment ID or version would require its own identity, lifecycle, authorization, and compatibility rules. A broader Policy relationship redesign can proceed separately. It is not a prerequisite for the Tag URI change.

### 5.7 Future authorizer input

**Resource attributes** are facts about a target that an authorizer may evaluate. A later authorizer could receive complete effective tags as resource attributes.

The internal path must:

* return a tag only when its definition lists the queried target kind.  
* consider assignments only from listed kinds, while continuing through excluded intermediate kinds.  
* choose the closest remaining assignment and keep its stored value, including a grandfathered value.  
* use hierarchy, definitions, and assignments from one logical point in time.

It must not call the public Tag endpoint during authorization. If complete effective tags cannot be resolved, authorization must fail rather than treat the target as untagged.

### 5.8 Named extensions

New value modes, multi-value support, mutable target rules, or tag-aware authorization should be named contract extensions. Existing v1 clients should not be expected to infer them from new server behavior.

### 5.9 Shared identifier and query handling

A broader transport design should address namespace encoding, uniform strict UTF-8 rejection, shared client tooling, and compatibility across Iceberg, Policy, and other Polaris APIs. Tag v1 still joins namespace names with U+001F and URI-encodes the joined value once (Part 1, Section 5.2). Section 7.12 compares candidate formats and their tested limits.

Tag v1 requires correct round trips for supported, valid UTF-8 names. Malformed percent escapes, invalid target shapes, and duplicate query parameters return 400 (Part 1, Section 5.2). Empty namespace levels and U+001F inside a level are rejected before joining names (Part 1, Section 2).

The shared HTTP layer handles invalid UTF-8. Tag v1 does not require uniform rejection of those bytes, but clients must still send valid UTF-8. Tag v1 does not migrate other endpoints.

Later shared tooling must preserve the published v1 encoding. Changing the format or allowing U+001F inside a namespace name requires an explicit compatibility decision. Existing v1 requests must keep their original meaning.

## 6\. What v1 does not include

| Later item | What v1 does now | Later work would need |
| :---- | :---- | :---- |
| Change target-types | The set is fixed at creation | Migration, concurrency, rollback, and effective-state rules |
| Separate source and destination kinds | One set controls both | Two-field semantics and compatibility rules |
| Inheritance barriers | Excluded intermediate kinds are skipped | Explicit stop rules and precedence |
| Rule-driven assignment | Only concrete assignments exist | Rule identity, selectors, execution, and reconciliation |
| Multi-value assignment | One selected value | Filtering, ordering, storage, and client compatibility |
| Key-only or free-form tags | Controlled values only | Explicit modes and storage rules |
| Allowed-value rename | Remove plus add | Rename identity or migration operation |
| Nested-field tagging | One top-level column segment | Nested identity and resolution |
| View columns | Whole views use VIEW entity identity | Stable column identity and replacement rules |
| Effective reverse lookup | Direct assignments only | Bounded descendant expansion |
| Definition restore | Permanent v1 deletion | Retention, conflicts, and restore API |
| Realm or cross-catalog tags | One catalog owns definition and target | Cross-catalog identity and authorization |
| Independent assignment ID | Definition route plus target query | Identity, lifecycle, version, and compatibility rules |
| Assignment retry recognition | Ordinary replacement/removal, with no key-based recognition | A demonstrated client need, durable key ownership, atomic relationship writes, and lifecycle rules |
| Recognition after definition deletion | No retained success record | Retention, identity reuse, cleanup, and a separate recovery contract |
| Shared query handling | Explicit Tag encoding and scoped validation | Section 5.9 covers strict UTF-8 handling, shared tooling, and cross-API compatibility |
| Reliable relationship cleanup | Hide orphaned assignments. JDBC tries to remove their rows during target deletion. | Durable signals, retries, and cross-backend recovery |
| Historical definition lookup | No Tag history store | Tombstones, snapshots, or audit contract |
| Tag-aware authorization | Tags are classification facts | Internal effective-tag input and authorizer changes |
| Portable ABAC administration | No shared rule language | Subjects, actions, effects, predicates, and translation |
| Row filters or masks | Allow or deny is outside Tag v1 | Rich decisions and execution support |

## 7\. Technical appendices

### 7.1 Contract and implementation boundary

Parts 1 and 2 define visible behavior. They do not require one database or transaction mechanism.

| Contract result | Implementation choices |
| :---- | :---- |
| Effective tags that existed together during the request, after considering every ancestor | Snapshot, versioned read, validation with retry, or provider-native operation |
| No partial visible write, even when operations overlap | Transaction, lock, compare-and-swap, batch, or fencing (Part 2, Section 6\) |
| The definition and all its assignments disappear from reads together | Local transaction, equivalent native operation, or atomic definition removal that hides assignments while their rows await cleanup |
| Consistent reverse-lookup item | Current index, candidate revalidation, cache, or direct scan |
| Target-kind filtering | Definition join, cached definition, or provider-native filter |
| Orphan hiding | Current definition and target visibility checks, including caches and reverse indexes |

A reverse-lookup item must take its target, value, and direct method from one assignment state. Callers never see half of a visible write. Accepted assignment/deletion races may leave orphan rows, but later reads never expose them (Part 2, Sections 6.2–6.4).

The proposed JDBC tables and indexes are one implementation. Another implementation may use different shapes.

A backend can use one atomic batch only if it can check every required precondition and apply every change together within its batch-size limit. Resources belonging to one catalog do not necessarily fit in one such batch.

If an implementation exposes changes through several commits, undoing earlier commits after a later failure cannot hide intermediate results that callers already read. Compensation alone cannot make those changes atomic. This applies both to bulk deletion and to checking current allowed values while writing an assignment.

The proposed built-in implementation shares inheritance, target-kind filtering, and closest-wins logic above persistence. Persistence supplies scoped entity and assignment reads, reverse lookup, and conditional writes. Part 2, Section 9.1 defines that division without prescribing final Java interfaces.

Effective reads use one state during the request. A complete but obsolete cache snapshot cannot satisfy that rule. The shared implementation must coordinate database reads with cached entities and schema pointers, while persistence supplies the conditions or snapshot needed to establish their consistency.

Implementations may use different module boundaries while preserving these results. A provider may use native operations when they preserve the same results.

Tag v1 requires no shared cleanup framework, generic column SPI, or tag-based authorization feature.

### 7.2 Source-backed path for tag-aware authorization

Polaris can delegate allow/deny decisions to a built-in authorizer or an external decision engine.

Source facts:

* [PolarisAuthorizer](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisAuthorizer.java) defines the authorizer contract.  
* [AuthorizationState](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/auth/AuthorizationState.java) carries request state.  
* [PolarisSecurable](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisSecurable.java) represents authorization targets.  
* The OPA and Ranger extensions show that external decision engines can implement the same broad role.

A later contract could add effective tags to authorization resource attributes. Existing tag identities and assignments could stay in place, while authorizers would need code to consume the new attributes.

The contract would need to define:

stable tag definition ID  
current tag name  
selected values  
optional provenance

The authorization path must resolve one complete effective state. It must apply target-types before the authorizer evaluates the target.

### 7.3 Policy as a possible ABAC administration foundation

Current Policy already has:

* versioned definitions.  
* typed content.  
* target mappings.  
* applicable reads.

A portable ABAC model would still need:

* subjects and actions.  
* effects and deny behavior.  
* tag predicates.  
* rule combination.  
* external-authorizer translation.  
* policy-store ownership and failure rules.

That work belongs in a separate proposal.

### 7.4 Why effective reads authorize the queried target once

Decision:

Permission to read the queried target's properties allows the caller to see its complete effective tag set.

The server does not separately authorize every parent source or returned definition.

Reasons:

* effective tags describe the queried target.  
* source-level filtering would return incomplete classifications.  
* moving an assignment between parent levels should not change visibility.  
* future authorizers need one complete input.

A failure to build the complete effective result must fail the request. It must not return a filtered subset.

### 7.5 Why reverse lookup filters each target

Reverse lookup reveals target identities, values, and direct assignment provenance. A readable tag definition does not grant access to every object carrying it.

The server checks TAG\_READ on the named definition, then the corresponding READ\_PROPERTIES permission on each candidate target. A column uses its containing table. Denied targets are omitted. Authorization-system errors must not be treated as ordinary denials.

Filtering and pagination must work together. A short or empty page may still carry a continuation token. Continue after consumed candidates without losing unconsumed ones, and reauthorize every page request.

### 7.6 Why detach-all=true needs two permissions on the definition

A normal drop removes the definition when no live-target assignments remain. Confirmed orphaned and soft-dropped-target relations do not block it.

detach-all=true also removes every assignment for that definition.

Ordinary unassignTag is two-sided. It needs TAG\_DETACH on the definition and the target-side detach-tag privilege. The bulk path does not repeat that target-side check for every assignment.

Decision:

TAG\_DROP \+ TAG\_DETACH

Reasons:

* deleting a definition and removing its assignments from every target are different permissions.  
* per-target checks would be unbounded and race-prone.  
* a separate TAG\_DETACH\_ALL privilege has no demonstrated role yet.

The operation still has one visible result: all assignments and the definition are removed, or nothing changes.

### 7.7 Relationship cleanup after target deletion

Ordinary relationship APIs resolve a current target. After permanent deletion, that name or path may no longer resolve to the old ID.

Permanent target deletion can trigger cleanup. Soft drop alone retains relationships for possible restore. Separately, deleting a tag definition makes its orphaned and soft-dropped-target relations permanently ineffective. Those rows do not block normal definition deletion, and restoring a target cannot restore the deleted tag.

Here, **best-effort cleanup** means deletion tries to remove related rows, but deletion does not depend on cleanup succeeding.

Current Polaris source already uses deletion-time best-effort cleanup for grants and Policy mappings in [AtomicOperationMetaStoreManager](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/persistence/AtomicOperationMetaStoreManager.java).

The proposed built-in JDBC deletion path works as follows:

* target deletion uses the ID still available in the deletion flow.  
* cleanup is attempted synchronously.  
* cleanup failure does not fail target deletion.  
* leftover assignments are orphaned and hidden.  
* column field removal has no equivalent entity-deletion hook.

Synchronous cleanup is not a portable solution because:

* fan-out can make deletion slow.  
* cleanup failure should not block normal deletion.  
* one transaction may be too large.  
* relationships may use another backend.  
* concurrent writes may race with the scan.  
* removing an Iceberg field does not delete a Polaris entity, so entity-deletion hooks cannot detect it.

V1 defines no manual orphan-cleanup API or Tag-specific cleanup privilege.

A shared lifecycle design should cover grant records, Policy mappings, Tag assignments, removed Iceberg fields, and future relationships. It should define durable deletion signals, retries, batches, idempotency, tombstones, cross-backend recovery, and operational visibility.

### 7.8 Why allowed-value replacement is not rename

Decision:

V1 values are strings. Replacing one string with another is removal plus addition.

Reasons:

* assignments store the selected string.  
* definition updates do not scan assignments.  
* a normal list replacement carries no rename intent.  
* hidden value IDs would not reveal which remove/add pair is a rename.

Testable results:

* old assignments remain readable.  
* new writes with the removed string fail.  
* reverse lookup can still filter by the grandfathered string.  
* no automatic migration occurs.

User-declared value IDs are a different design. An assignment can keep the same ID when its value name changes. That resolves rename intent when the caller preserves the ID. The ambiguity above concerns hidden IDs behind a string-only update.

V1 keeps string values so clients do not need to manage a second identity for each allowed value. Renaming a widely assigned value therefore requires migrating assignments. V1 provides no atomic bulk rename.

One later option is an optional allowed-value-ids field on the definition. Values with declared IDs could preserve identity through rename, while values without IDs retain name-based behavior. This is a candidate extension, not a v1 field or a complete migration design.

Before adoption, that design must specify how assignments refer to IDs and how existing string assignments are interpreted or migrated. It also needs ID-reuse, retired-value, name-filter, and old-client rules. Adding the definition field alone does not establish rename behavior.

### 7.9 Why assignment targets belong in the URI

V1 uses /tags/{tag-name}/assignments with an explicit single-target query for PUT and DELETE. The query identifies the relation. PUT's body contains only the selected value, and DELETE has no body. GET on the collection lists direct assignments.

This reuses the namespace query rules already needed for target reads. It also distinguishes TABLE and VIEW and prevents a missing target from silently becoming a catalog operation.

The relation still has no independent ID, metadata, version, or restore lifecycle. Query addressing does not require one. Policy can retain its existing relationship contract while Tags adopt these verbs.

[RFC 9110](https://www.rfc-editor.org/rfc/rfc9110#section-9.3.5) gives DELETE request content no general semantics. Target query parameters avoid depending on such content.

### 7.10 target-types behavior tests

Behavior tests should verify:

* create defaults omitted target kinds to the five v1 kinds and preserves that stored set.  
* create rejects explicit null, empty, duplicate, or unknown target kinds.  
* update rejects any presence of target-types, including explicit JSON null and an empty list.  
* assignment to an excluded kind writes nothing.  
* direct and effective reads omit excluded queried kinds.  
* effective reads ignore excluded source kinds.  
* excluded intermediate kinds do not stop the parent walk.  
* child namespaces remain eligible when NAMESPACE is listed.  
* reverse lookup returns direct assignments only on listed kinds.  
* assignment identity depends on the definition and target identities, and assignment rows do not store target-types.

### 7.11 Source map

| Topic | Primary sources |
| :---- | :---- |
| Authorizer contract and decision shape | [PolarisAuthorizer.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisAuthorizer.java), [AuthorizationDecision.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/AuthorizationDecision.java) |
| Authorization state and target shape | [AuthorizationState.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/AuthorizationState.java), [PolarisSecurable.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisSecurable.java) |
| OPA and Ranger implementations | [OpaPolarisAuthorizer.java](https://github.com/apache/polaris/blob/7866ce5c00/extensions/auth/opa/src/main/java/org/apache/polaris/extension/auth/opa/OpaPolarisAuthorizer.java), [Ranger extension](https://github.com/apache/polaris/tree/7866ce5c00/extensions/auth/ranger) |
| Policy API and implementation | [policy-apis.yaml](https://github.com/apache/polaris/blob/7866ce5c00/spec/polaris-catalog-apis/policy-apis.yaml), [PolicyCatalog.java](https://github.com/apache/polaris/blob/7866ce5c00/runtime/service/src/main/java/org/apache/polaris/service/catalog/policy/PolicyCatalog.java), [PolicyCatalogHandler.java](https://github.com/apache/polaris/blob/7866ce5c00/runtime/service/src/main/java/org/apache/polaris/service/catalog/policy/PolicyCatalogHandler.java) |
| Entity model and built-in JDBC schema | [PolarisEntity.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/entity/PolarisEntity.java), [schema-v4.sql](https://github.com/apache/polaris/blob/7866ce5c00/persistence/relational-jdbc/src/main/resources/postgres/schema-v4.sql) |
| Deletion cleanup and relationship addressing | [AtomicOperationMetaStoreManager.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/persistence/AtomicOperationMetaStoreManager.java), [PolarisGrantManager.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisGrantManager.java) |
| Query encoding | [Iceberg 1.11.0 RESTUtil](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/rest/RESTUtil.java), [RFC 3986](https://www.rfc-editor.org/rfc/rfc3986#section-2.4), [form-query decoding](https://url.spec.whatwg.org/#concept-urlencoded-parser) |
| Encoding alternatives and namespace domain | [RFC 8259](https://www.rfc-editor.org/rfc/rfc8259#section-7), [Nessie printable escaping](https://github.com/projectnessie/nessie/blob/nessie-0.108.4/api/model/src/main/java/org/projectnessie/model/Util.java), [Polaris validator](https://github.com/apache/polaris/blob/0858f790357704d3da37c47e59c3c4f1076ea138/runtime/service/src/main/java/org/apache/polaris/service/catalog/validation/EntityNameValidator.java), [Iceberg Namespace](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/api/src/main/java/org/apache/iceberg/catalog/Namespace.java) |
| HTTP and path-processing references | [Polaris dependency versions](https://github.com/apache/polaris/blob/0858f790357704d3da37c47e59c3c4f1076ea138/gradle/libs.versions.toml), [Quarkus HTTP](https://quarkus.io/guides/http-reference/), [Servlet path canonicalization](https://jakarta.ee/specifications/servlet/6.1/jakarta-servlet-spec-6.1#uri-path-canonicalization) |
| DELETE request content guidance | [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.3.5), [OpenAPI 3.1.2](https://spec.openapis.org/oas/v3.1.2.html#operation-object) |
| target-types design | Parts 1 and 2 of this proposal. Reviewed peer sources do not establish an equivalent field |

### 7.12 Namespace encoding follow-up

**Tag v1 retains the Iceberg namespace query convention in Part 1\.** A broader codec change belongs in a separate transport design. The comparison below evaluates alternatives without selecting a replacement for v1.

**Why this remains a real interoperability issue**

Path decoding, query-value decoding, and namespace boundaries are different concerns. A path-helper defect does not establish a defect in the query helper. The [Iceberg discussion](https://lists.apache.org/thread/c498svln0x18vvm42998b9nm9j6ck5yh) and [Polaris discussion](https://lists.apache.org/thread/gpjlhkof6fwy3jhrp8rjv0zq5nmyhsb7) are related discussion entry points.

There is also a query-specific deployment concern. \["sales", "eu"\] becomes namespace=sales%1Feu, even though both names contain only letters. The separator itself can trigger ingress validation.

[OWASP CRS 4.0.0 rule 920271](https://github.com/coreruleset/coreruleset/blob/v4.0.0/rules/REQUEST-920-PROTOCOL-ENFORCEMENT.conf) inspects query arguments and excludes byte 31 after URL decoding. It applies at paranoia level 2\. Actual blocking depends on enabled rules and anomaly thresholds. This finding comes from the rule source, not a deployed WAF test.

Excluding the separator from names does not remove the separator inserted between names. Retaining this format controls Tag scope but does not prove compatibility with every deployment.

**Candidate formats**

The follow-up should preserve readable simple URLs and round-trip valid Unicode without changing names or namespace boundaries. Representing a name does not mean a catalog must accept it. Existing naming rules remain separate.

| Format | Simple namespace in the URL | Benefit | Cost or limit |
| :---- | :---- | :---- | :---- |
| Iceberg query convention | sales%1Feu | Existing namespace convention | Structural control character and excluded separator inside names |
| Printable dot/escape candidate | sales.eu | Simple names stay readable. UTF-8 byte escapes preserve contents | New grammar, client helpers, and conformance rules to maintain |
| JSON array query value | %5B%22sales%22%2C%22eu%22%5D | Standard string escaping and explicit element boundaries | Longer URLs and strict array/string validation |

One printable candidate would preserve ASCII letters, digits, hyphens, and underscores. It would write other UTF-8 bytes as \~HH and join names with dots. A dot inside a name becomes \~2E. A tilde becomes \~7E.

| Original namespace | Printable codec text before URI serialization |
| :---- | :---- |
| \["sales", "eu"\] | sales.eu |
| \["sales.eu"\] | sales\~2Eeu |
| \["a\~2Eb"\] | a\~7E2Eb |
| \["销售", "😀"\] | \~E9\~94\~80\~E5\~94\~AE.\~F0\~9F\~98\~80 |

A decoder for this candidate would consume one URI-decoded value, split on literal dots, restore escaped bytes, and decode strict UTF-8. It would preserve case and Unicode normalization without decoding recovered text again. It would reject malformed escapes, empty levels, invalid UTF-8, and unpaired surrogates.

This is a new candidate grammar, not the existing Iceberg or Nessie codec. Nessie's [printable encoding](https://github.com/projectnessie/nessie/blob/nessie-0.108.4/api/model/src/main/java/org/projectnessie/model/Util.java) supplies prior art. Its [name validation](https://github.com/projectnessie/nessie/blob/nessie-0.108.4/api/model/src/main/java/org/projectnessie/model/Elements.java) and legacy decoding behavior require separate review before reuse.

**Validation before adoption**

A replacement needs cross-language round-trip tests, malformed-input tests, and checks through the actual HTTP and ingress path. Cover Unicode scalar values, literal escape text, empty levels, invalid UTF-8, and serializers that encode \~ as %7E.

Validation must include namespace, table, and column parameters. A namespace codec alone cannot establish strict transport handling for the other names. If the HTTP layer replaces invalid bytes before parsing, a later parser cannot recover the original bytes.

These candidate grammars are design options. This proposal does not claim that they have passed production endpoint, generated-client, or deployed ingress tests.

A future adoption must define endpoint/client behavior and an explicit compatibility path. Do not guess the format: namespace=sales.eu means one name under v1 and two under the printable candidate. Shared adoption and migration remain outside Tag v1.

### 7.13 Definition identity and native idempotency

Rename has its own endpoint so a request identifies both old and new names explicitly. Updating the other editable fields is separate. This deliberately gives up one atomic rename-plus-edit request.

The public ID answers whether two results refer to the same tag. The version token protects a new write against stale state. An idempotency key identifies a logical operation whose recorded success can be recognized. These serve different purposes.

The built-in design uses the existing entity ID as an opaque string. It avoids maintaining another UUID solely for comparison. Clients may correlate identities but continue to address operations by name. Cross-catalog synchronization can maintain mappings without preserving IDs.

#### Authorization

Rename requires TAG\_DROP on the original definition and TAG\_CREATE on its owning catalog. The old name stops resolving and a new name becomes available. These permissions control both effects. TAG\_WRITE continues to govern editable-field replacement.

This follows the two-sided structure of table and view rename. Their built-in checks require source drop and destination list/create privileges. Tag names its definition-drop and catalog-create requirements explicitly (Part 1, Section 7.1).

Rename preserves the entity, grants, and assignments. Reusing a drop privilege does not invoke deletion or require assignment cleanup. A caller does not need TAG\_DETACH, and live assignments do not cause TagInUse.

Recognized create/update retries check their current operation permissions without adding TAG\_READ. Keys grant no permission, and records have no original-principal binding. The server checks the current caller before returning success.

#### Why recognition precedes version comparison

An update or rename that changes the definition advances its version. Checking the old token first would reject the retry that the key is intended to recognize. Recognition instead acknowledges success without applying another mutation.

Create/update return the current definition. A later update may have changed it since the original request. Rename returns 204. Neither response restores earlier state or reproduces a stored HTTP response.

Create/update use ordinary name lookup. Create has no original identity token, so finding a renamed tag would require another access path. The contract accepts ordinary create behavior under the requested name, including creation of another tag when that name is free.

Rename preserves a stronger lookup boundary. If sensitivity becomes data\_sensitivity and then classification, the first request's destination no longer locates the entity. Its live key still belongs to the surviving original identity. The opaque version token can carry server-interpreted identity for an internal lookup. That lookup remains scoped and authorized, without exposing public ID addressing.

Clients use globally unique keys and keep arguments unchanged on retries. Server recognition uses the live key without a request fingerprint or argument binding. Key reuse with different arguments violates the client contract and has no promised response. This follows the existing entity helper and avoids a separate request-signature protocol.

#### Why assignment retries remain ordinary operations

An assignment is a relationship without its own entity or client version. Internal compare-and-swap or locking can protect one execution, but cannot identify a delayed retry. An old assign may overwrite a later value. An old unassign may delete a recreated relation.

Adding recognition would require a durable owner for keys and an atomic boundary spanning that owner and the relationship. The inspected Policy attachment and grant paths do not provide this native pattern. Tag v1 retains ordinary assignment retries until a concrete client requirement justifies that added contract. Existing allowed-value, identity, and deletion guarantees still apply.

Drop loses the entity that owns the records. Recognition after deletion would need retained state and its own lifecycle. Tag v1 adds no such journal or tombstone.

#### Existing mechanisms and their limits

The following upstream sources are pinned to commit c90944ea04db9b0c2fe954a178af0634e39b9c6b, unless a row identifies another revision. They establish reusable behavior, not a completed Tag integration.

| Source | What it establishes | Boundary |
| :---- | :---- | :---- |
| [Rename authorization](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/polaris-core/src/main/java/org/apache/polaris/core/auth/RbacOperationSemantics.java) | Table/view rename checks source drop and destination list/create. | Tag uses its own two checks. It must not inherit update permissions. |
| [EntityIdempotency](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/idempotency/EntityIdempotency.java) | Internal properties hold key/expiry pairs. Live keys are retained. A full window fails with 503 and Retry-After. Unreadable encodings become empty. | No request fingerprint, principal, response snapshot, or post-deletion record is stored. Persistence read failures are separate from decoder fallback. |
| [Request context](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/idempotency/IdempotencyRequestContext.java) | Capturing the request key computes its expiry from the configured lifetime. | The window does not start at commit. A slow request consumes its own recognition time. |
| [Table create/update](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/catalog/iceberg/IcebergCatalogHandler.java) | Authorized key recognition returns current state. A fresh lookup after a competing commit can recognize its success. | Name lookup does not supply general recovery after rename. |
| [Rename adapters](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/catalog/iceberg/IcebergCatalogAdapter.java) | Table/view rename adapters accept an idempotency argument. | Those paths do not implement recognition. Header acceptance is not a completed rename integration to copy. |
| [No-change update](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/catalog/iceberg/CatalogHandlerUtils.java) | An unchanged metadata update returns without committing. | Tag likewise need not persist a key for a version-checked no-op. |
| [Header filter](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/idempotency/IdempotencyKeyFilter.java) | Enabled handling parses a UUID. Malformed non-blank input returns 400 InvalidIdempotencyKey. | No additional UUID-version check is made. Parsing alone does not provide endpoint recognition. |
| [Configuration](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/idempotency/IdempotencyConfiguration.java) and [discovery](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/catalog/config/CatalogConfigHandler.java) | Shared settings control enablement/lifetime. Enabled support advertises idempotency-key-lifetime. The configuration defaults to disabled and PT5M. | Defaults are not protocol constants. Interpret discovery with endpoint availability and Part 1's operation coverage. |
| [Entity lookup](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/polaris-core/src/main/java/org/apache/polaris/core/persistence/PolarisMetaStoreManager.java) | Internal lookup accepts catalog, entity ID, and entity type. | A returned entity may have been dropped. Existence alone does not establish retry eligibility or authorization. |
| [Tag token example](https://github.com/apache/polaris/blob/db254b0586fa42cbff4b25cc8f7ba9856baf2e2c/polaris-core/src/main/java/org/apache/polaris/core/tag/TagVersionToken.java) | This separate Tag revision encodes definition ID and entity version. | It supports an internal lookup option. Part 2 proposes a separate public definition counter. Neither establishes completed retry wiring. |
| [Policy handler](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/runtime/service/src/main/java/org/apache/polaris/service/catalog/policy/PolicyCatalogHandler.java) and [grant manager](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisGrantManager.java) | The inspected relationship paths do not integrate the entity key helper. | Definition integration is not evidence of native key-based recognition for non-entity relationships. |
| [JDBC ID generator](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/persistence/relational-jdbc/src/main/java/org/apache/polaris/persistence/relational/jdbc/IdGenerator.java) | The JDBC path generates entity IDs. | Live-row uniqueness alone does not prove every historical ID-reuse or migration guarantee. |

The [Iceberg OpenAPI description](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/spec/iceberg-rest-catalog-open-api.yaml) describes stronger response-replay behavior. Tag adopts the inspected helper's successful-completion recognition. It does not promise original-response or terminal-error replay. Part 1 is the Tag contract.

### 7.14 Pagination bounds and shared helpers

Tag collections paginate by default. pagination=false explicitly requests the complete result or an error. This retains a single-response full-result capability without making it the default. Both modes bound response size and request work, including when deployment configuration uses defaults.

The [Iceberg REST pagination contract](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/spec/iceberg-rest-catalog-open-api.yaml) requests all results when pageToken is absent. An empty token starts pagination. Tag preserves the empty first-page token, opaque continuation tokens, positive page sizes, and next-page-token response field. Its omitted-parameter behavior deliberately differs from IRC. Familiar pagination fields do not make the two default behaviors interchangeable.

The explicit mode parameter keeps pageSize as a size bound and pageToken as a cursor. Special size values or reversing the empty-token meaning would make familiar fields select unexpected modes. Full-result requests reject either parameter, including an empty token, rather than silently ignore conflicting intent. A client adapting IRC calls to Tag must explicitly request full-result mode or consume the paginated result.

Output size and work are different limits. Reverse lookup may reject many candidates for permissions or target liveness. A small page can still require substantial database, authorization, or schema work. Safe continuation may stop after consumed candidates without filling the page. It must keep fetched but unconsumed candidates reachable.

If an identified budget prevents safe progress, the request returns 400 BadRequest. A dependency read failure remains a service error. Limits do not relax hierarchy completeness or per-response consistency. Exact configuration keys, defaults, units, and enforcement points remain implementation choices.

The inspected [maximum-page-size configuration](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/polaris-core/src/main/java/org/apache/polaris/core/config/FeatureConfiguration.java) defaults to an unlimited maximum. It is not a configured default page size. The inspected [pagination helper](https://github.com/apache/polaris/blob/c90944ea04db9b0c2fe954a178af0634e39b9c6b/polaris-core/src/main/java/org/apache/polaris/core/persistence/pagination/PageTokenUtil.java) also has different input and mode rules:

| Shared helper behavior | Tag contract |
| :---- | :---- |
| A configured maximum can cause pagination without a token. | Pagination is the default, independent of token presence. Only pagination=false requests full-result mode. |
| Zero is accepted as a requested size. | A supplied size must be positive. Full-result mode rejects any size parameter. |
| Continuation may inherit the old size from its token. | Omitted size uses the server's current finite default. |
| Disabling pagination can ignore request parameters. | Input validation and the explicit mode rules still apply. |

Reuse therefore requires adaptation at the Tag boundary. A maximum-page-size setting alone does not supply the full-result or work-budget contract. This proposal does not change table or federation listing behavior.

## 8\. References

Apache Polaris:

* [https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/entity/PolarisEntity.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/entity/PolarisEntity.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/persistence/AtomicOperationMetaStoreManager.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/persistence/AtomicOperationMetaStoreManager.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisGrantManager.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisGrantManager.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisAuthorizer.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisAuthorizer.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/AuthorizationState.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/AuthorizationState.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisSecurable.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/PolarisSecurable.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/AuthorizationDecision.java](https://github.com/apache/polaris/blob/7866ce5c00/polaris-core/src/main/java/org/apache/polaris/core/auth/AuthorizationDecision.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/extensions/auth/opa/src/main/java/org/apache/polaris/extension/auth/opa/OpaPolarisAuthorizer.java](https://github.com/apache/polaris/blob/7866ce5c00/extensions/auth/opa/src/main/java/org/apache/polaris/extension/auth/opa/OpaPolarisAuthorizer.java)  
* [https://github.com/apache/polaris/tree/7866ce5c00/extensions/auth/ranger](https://github.com/apache/polaris/tree/7866ce5c00/extensions/auth/ranger)  
* [https://github.com/apache/polaris/blob/7866ce5c00/spec/polaris-catalog-apis/policy-apis.yaml](https://github.com/apache/polaris/blob/7866ce5c00/spec/polaris-catalog-apis/policy-apis.yaml)  
* [https://github.com/apache/polaris/blob/7866ce5c00/runtime/service/src/main/java/org/apache/polaris/service/catalog/policy/PolicyCatalog.java](https://github.com/apache/polaris/blob/7866ce5c00/runtime/service/src/main/java/org/apache/polaris/service/catalog/policy/PolicyCatalog.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/runtime/service/src/main/java/org/apache/polaris/service/catalog/policy/PolicyCatalogHandler.java](https://github.com/apache/polaris/blob/7866ce5c00/runtime/service/src/main/java/org/apache/polaris/service/catalog/policy/PolicyCatalogHandler.java)  
* [https://github.com/apache/polaris/blob/7866ce5c00/persistence/relational-jdbc/src/main/resources/postgres/schema-v4.sql](https://github.com/apache/polaris/blob/7866ce5c00/persistence/relational-jdbc/src/main/resources/postgres/schema-v4.sql)  
* [https://polaris.apache.org/releases/1.7.0/managing-security/external-pdp/opa/](https://polaris.apache.org/releases/1.7.0/managing-security/external-pdp/opa/)  
* [https://polaris.apache.org/releases/1.7.0/policy/](https://polaris.apache.org/releases/1.7.0/policy/)

HTTP and OpenAPI:

* [https://www.rfc-editor.org/rfc/rfc9110.html\#section-9.3.5](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.3.5)  
* [https://spec.openapis.org/oas/v3.1.2.html\#operation-object](https://spec.openapis.org/oas/v3.1.2.html#operation-object)

Snowflake:

* [https://docs.snowflake.com/en/user-guide/object-tagging](https://docs.snowflake.com/en/user-guide/object-tagging)  
* [https://docs.snowflake.com/en/sql-reference/sql/create-tag](https://docs.snowflake.com/en/sql-reference/sql/create-tag)  
* [https://docs.snowflake.com/en/sql-reference/functions/tag\_references](https://docs.snowflake.com/en/sql-reference/functions/tag_references)  
* [https://docs.snowflake.com/en/developer-guide/snowflake-rest-api/reference/tag](https://docs.snowflake.com/en/developer-guide/snowflake-rest-api/reference/tag)  
* [https://docs.snowflake.com/en/user-guide/object-tagging/propagation](https://docs.snowflake.com/en/user-guide/object-tagging/propagation)  
* [https://docs.snowflake.com/en/user-guide/security-access-control-configure](https://docs.snowflake.com/en/user-guide/security-access-control-configure)  
* [https://github.com/snowflakedb/snowflake-rest-api-specs](https://github.com/snowflakedb/snowflake-rest-api-specs)

Databricks UC:

* [https://docs.databricks.com/aws/en/database-objects/tags](https://docs.databricks.com/aws/en/database-objects/tags)  
* [https://docs.databricks.com/aws/en/admin/governed-tags/manage-governed-tags](https://docs.databricks.com/aws/en/admin/governed-tags/manage-governed-tags)  
* [https://docs.databricks.com/aws/en/admin/tag-policies](https://docs.databricks.com/aws/en/admin/tag-policies)  
* [https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/)  
* [https://docs.databricks.com/aws/en/sql/language-manual/information-schema/column\_tags](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/column_tags)

Apache Gravitino:

* [https://github.com/apache/gravitino/blob/main/docs/manage-tags-in-gravitino.md](https://github.com/apache/gravitino/blob/main/docs/manage-tags-in-gravitino.md)  
* [https://gravitino.apache.org/docs/latest/security/access-control](https://gravitino.apache.org/docs/latest/security/access-control)

DataHub:

* [https://docs.datahub.com/docs/tags/](https://docs.datahub.com/docs/tags/)  
* [https://docs.datahub.com/docs/generated/metamodel/entities/tag/](https://docs.datahub.com/docs/generated/metamodel/entities/tag/)  
* [https://docs.datahub.com/docs/generated/metamodel/entities/glossaryterm/](https://docs.datahub.com/docs/generated/metamodel/entities/glossaryterm/)  
* [https://docs.datahub.com/docs/api/tutorials/tags/](https://docs.datahub.com/docs/api/tutorials/tags/)  
* [https://docs.datahub.com/docs/authorization/policies/](https://docs.datahub.com/docs/authorization/policies/)

AWS:

* [https://docs.aws.amazon.com/lake-formation/latest/dg/tag-based-access-control.html](https://docs.aws.amazon.com/lake-formation/latest/dg/tag-based-access-control.html)  
* [https://docs.aws.amazon.com/lake-formation/latest/dg/TBAC-assigning-tags.html](https://docs.aws.amazon.com/lake-formation/latest/dg/TBAC-assigning-tags.html)  
* [https://docs.aws.amazon.com/lake-formation/latest/APIReference/API\_UpdateLFTag.html](https://docs.aws.amazon.com/lake-formation/latest/APIReference/API_UpdateLFTag.html)  
* [https://docs.aws.amazon.com/lake-formation/latest/APIReference/API\_SearchTablesByLFTags.html](https://docs.aws.amazon.com/lake-formation/latest/APIReference/API_SearchTablesByLFTags.html)  
* [https://docs.aws.amazon.com/lake-formation/latest/APIReference/API\_SearchDatabasesByLFTags.html](https://docs.aws.amazon.com/lake-formation/latest/APIReference/API_SearchDatabasesByLFTags.html)

GCP:

* [https://cloud.google.com/dataplex/docs/release-notes](https://cloud.google.com/dataplex/docs/release-notes)  
* [https://cloud.google.com/dataplex/docs/transition-to-dataplex-catalog](https://cloud.google.com/dataplex/docs/transition-to-dataplex-catalog)  
* [https://cloud.google.com/bigquery/docs/labels-intro](https://cloud.google.com/bigquery/docs/labels-intro)  
* [https://cloud.google.com/bigquery/docs/column-level-security-intro](https://cloud.google.com/bigquery/docs/column-level-security-intro)  
* [https://cloud.google.com/dataplex/docs](https://cloud.google.com/dataplex/docs)

Apache Atlas:

* [https://atlas.apache.org/2.0.0/TypeSystem.html](https://atlas.apache.org/2.0.0/TypeSystem.html)  
* [https://atlas.apache.org/2.0.0/ClassificationPropagation.html](https://atlas.apache.org/2.0.0/ClassificationPropagation.html)  
* [https://atlas.apache.org/\#/SearchBasic](https://atlas.apache.org/#/SearchBasic)  
* [https://atlas.apache.org/\#/SearchAdvanced](https://atlas.apache.org/#/SearchAdvanced)  
* [https://atlas.apache.org/\#/Glossary](https://atlas.apache.org/#/Glossary)

[image1]: <assets/a041201bf35f0c3e7922dab300bfcf2ac92d5a05962e4c82fba45daf34c7e327.png>