import { Kafka }        from "kafkajs";
import { createClient } from "redis";
import { MongoClient, ObjectId } from "mongodb";
import { v4 as uuidv4 } from "uuid";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "").split(",");
const REDIS_HOST    = process.env.REDIS_HOST || "localhost";
const REDIS_PORT    = parseInt(process.env.REDIS_PORT || "6379");
const EVENT_TTL_SEC = parseInt(process.env.EVENT_TTL_SEC || "60");
const MONGO_URI     = process.env.MONGO_URI || "";
const MONGO_DB      = process.env.MONGO_DB  || "";

const ALLOWED_COLLECTIONS = new Set([
  "CollectionName1",
  "CollectionName2"
]);

// ── Mongo type cast field map ─────────────────────────────────────────────────
// Only applied on Couchbase → MongoDB path, after mongoId has been renamed to _id.
// objectId:      fields to wrap as { "$oid": "..." }
// date:          fields to wrap as { "$date": "..." }
// objectIdArray: array of ObjectId strings → [{ "$oid": "..." }, ...]
const MONGO_FIELD_TYPES = {
  CollectionName1: {
    objectId: ["_id", "anotherId"],
  },
  CollectionName2: {
    objectId:      ["_id"],
    date:          ["createdDate", "modifiedDate"],
    objectIdArray: ["userIds"],
  }
};

// ── Logger ────────────────────────────────────────────────────────────────────
// Every line carries: [cid] [source] [collection] [docId] - message
// Filter CloudWatch by cid to trace a single event end-to-end.
function makeLogger(cid, source, collection = "-", docId = "-") {
  const prefix = `[${cid}] [${source}] [${collection}] [${docId}]`;
  return {
    cid,
    info:  (msg) => console.log(`${prefix} - ${msg}`),
    warn:  (msg) => console.warn(`${prefix} - ⚠ ${msg}`),
    error: (msg) => console.error(`${prefix} - ✗ ERROR | ${msg}`),
    skip:  (msg) => console.log(`${prefix} - SKIPPED | ${msg}`),
    echo:  (msg) => console.log(`${prefix} - ECHO | ${msg}`),
    fwd:   (msg) => console.log(`${prefix} - ✓ FORWARDED | ${msg}`),
    del:   (msg) => console.log(`${prefix} - ✓ DELETE | ${msg}`),
  };
}

// ── Type cast for MongoDB sink ────────────────────────────────────────────────
function castForMongo(collection, doc) {
  const schema = MONGO_FIELD_TYPES[collection];
  if (!schema) return doc;

  const out = { ...doc };

  for (const field of (schema.objectId || [])) {
    const val = out[field];
    if (val && typeof val === "string") {
      out[field] = { "$oid": val };
    }
  }

  for (const field of (schema.date || [])) {
    const val = out[field];
    if (val && typeof val === "string") {
      out[field] = { "$date": val };
    }
  }

  for (const field of (schema.objectIdArray || [])) {
    const val = out[field];
    if (Array.isArray(val)) {
      out[field] = val.map(item =>
        item && typeof item === "string" ? { "$oid": item } : item
      );
    }
  }

  return out;
}

// ── Field renames ─────────────────────────────────────────────────────────────
// Couchbase Sync Gateway breaks on fields starting with underscore.
// _id     → mongoId  when writing to Couchbase
// mongoId → _id      when writing to MongoDB
// syncEventId has no underscore — safe for both databases as-is.
function renameForCouchbase(doc) {
  const out = { ...doc };
  out.mongoId = out._id;
  delete out._id;
  return out;
}

function renameForMongo(doc, fallbackKey) {
  const out = { ...doc };
  out._id = out.mongoId || fallbackKey;
  delete out.mongoId;
  return out;
}

// ── Persistent connections (reused across warm Lambda invocations) ─────────────
let producer    = null;
let redisClient = null;
let mongoClient = null;

async function getProducer(log) {
  if (!producer) {
    const kafka = new Kafka({ brokers: KAFKA_BROKERS, clientId: "logic-app-lambda" });
    producer    = kafka.producer();
    await producer.connect();
    log.info("Kafka producer connected (cold start)");
  }
  return producer;
}

async function getRedis(log) {
  if (!redisClient || !redisClient.isReady) {
    redisClient = createClient({ socket: { host: REDIS_HOST, port: REDIS_PORT } });
    redisClient.on("error", err => log.error(`Redis client error: ${err.message}`));
    await redisClient.connect();
    log.info("Redis connected (cold start)");
  }
  return redisClient;
}

async function getMongoClient(log) {
  if (!mongoClient) {
    mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    log.info("MongoDB client connected (cold start)");
  }
  return mongoClient;
}

function redisKey(eventId) {
  return `sync:event:${eventId}`;
}

// ── Delete forwarding ─────────────────────────────────────────────────────────
// Mongo → Couchbase: Kafka tombstone (key = docId, value = null)
//   couchbase-sink handles null value natively, no loop risk.
//
// Couchbase → Mongo: direct MongoDB SDK delete with ObjectId cast.
//   Connector cannot cast string key to ObjectId — SDK is the only clean path.
async function forwardDelete(source, collection, docId, kafkaProducer, log) {
  if (!ALLOWED_COLLECTIONS.has(collection)) {
    log.skip(`collection not whitelisted`);
    return { action: "skipped", reason: "not whitelisted" };
  }

  if (source === "mongo") {
    // Mongo → Couchbase: tombstone, plain string key
    const outputTopic = `logic.to.couchbase.${collection}`;
    await kafkaProducer.send({
      topic:    outputTopic,
      messages: [{ key: docId.toString(), value: null }]
    });
    log.del(`→ ${outputTopic} (tombstone)`);
    return { action: "forwarded", reason: "delete", outputTopic };

  } else {
    // Couchbase → Mongo: direct delete via MongoDB SDK with proper ObjectId
    // MongoDB client is lazy-initialized here — only created on first delete invocation
    const mongo  = await getMongoClient(log);
    const db     = mongo.db(MONGO_DB);
    const result = await db.collection(collection).deleteOne({ _id: new ObjectId(docId) });
    log.del(`MongoDB direct | collection: ${collection} | docId: ${docId} | deletedCount: ${result.deletedCount}`);
    return { action: "forwarded", reason: "delete", target: "mongodb-direct", deletedCount: result.deletedCount };
  }
}

// ── Process upsert message ────────────────────────────────────────────────────
async function processMessage(source, collection, doc, redis, kafkaProducer, log) {

  if (!ALLOWED_COLLECTIONS.has(collection)) {
    log.skip(`collection not whitelisted`);
    return { action: "skipped", reason: "not whitelisted" };
  }

  const docId           = source === "couchbase" ? (doc.mongoId || doc._id) : doc._id;
  const incomingEventId = doc.syncEventId;

  // Rebuild logger now that we have docId
  log = makeLogger(log.cid, source, collection, docId);

  log.info(`► RECEIVED | syncEventId: ${incomingEventId || "none"}`);

  // ── Loop detection ────────────────────────────────────────────
  if (incomingEventId) {
    log.info(`LOOP CHECK | looking up syncEventId: ${incomingEventId}`);
    const storedSource = await redis.get(redisKey(incomingEventId));

    if (storedSource) {
      log.info(`LOOP CHECK | found in Redis → storedSource: ${storedSource}`);

      if (storedSource !== source) {
        await redis.del(redisKey(incomingEventId));
        log.echo(`storedSource=${storedSource} vs incomingSource=${source} → dropping & deleting Redis key`);
        return { action: "dropped", reason: "echo" };
      } else {
        log.info(`LOOP CHECK | same source repeat → treating as genuine`);
      }
    } else {
      log.info(`LOOP CHECK | not found in Redis → genuine change`);
    }
  } else {
    log.info(`LOOP CHECK | no syncEventId on doc → genuine new change`);
  }

  // ── Genuine change ────────────────────────────────────────────
  const newEventId  = uuidv4();
  const outputTopic = source === "mongo"
    ? `logic.to.couchbase.${collection}`
    : `logic.to.mongo.${collection}`;

  await redis.setEx(redisKey(newEventId), EVENT_TTL_SEC, source);
  const verify = await redis.get(redisKey(newEventId));
  if (!verify) {
    log.warn(`Redis store FAILED for key: ${redisKey(newEventId)}`);
  } else {
    log.info(`Redis stored | key: ${newEventId} → ${source} TTL: ${EVENT_TTL_SEC}s`);
  }

  doc.syncEventId = newEventId;
  delete doc._collection;

  let forwardDoc = { ...doc };

  if (source === "mongo") {
    // Mongo → Couchbase: rename _id → mongoId
    forwardDoc = renameForCouchbase(forwardDoc);
    log.info(`Renamed _id → mongoId for Couchbase`);
  } else {
    // Couchbase → Mongo: rename mongoId → _id, cast types, add _collection for routing
    forwardDoc = renameForMongo(forwardDoc, docId);
    forwardDoc = castForMongo(collection, forwardDoc);
    forwardDoc._collection = collection;
    log.info(`Renamed mongoId → _id, applied type casting for MongoDB`);
  }

  await kafkaProducer.send({
    topic:    outputTopic,
    messages: [{
      key:   docId.toString(),
      value: JSON.stringify(forwardDoc),
      headers: { source, collection, eventId: newEventId }
    }]
  });

  log.fwd(`→ ${outputTopic} | newSyncEventId: ${newEventId}`);
  return { action: "forwarded", outputTopic, eventId: newEventId };
}

// ── Handler ───────────────────────────────────────────────────────────────────
export const handler = async (event) => {
  // Correlation ID — unique per invocation, use this to filter CloudWatch logs
  const cid    = uuidv4().split("-")[0]; // short 8-char ID, e.g. "a3f2bc91"
  const path   = event.rawPath || "/";
  const source = path.includes("couchbase") ? "couchbase" : "mongo";

  // Initial logger before collection/docId are known
  let log = makeLogger(cid, source);
  log.info(`► INVOKED | path: ${path}`);

  // ── Decode body ───────────────────────────────────────────────
  let bodyStr;
  if (event.isBase64Encoded) {
    bodyStr = Buffer.from(event.body, "base64").toString("utf8");
  } else {
    bodyStr = event.body || "{}";
  }

  // ── Parse body ────────────────────────────────────────────────
  let changeEvent;
  try {
    changeEvent = JSON.parse(bodyStr);
  } catch (e) {
    log.error(`Failed to parse body: ${e.message}`);
    return { statusCode: 400, body: JSON.stringify({ error: "Invalid JSON" }) };
  }

  // ── Connect ───────────────────────────────────────────────────
  // MongoDB client is NOT initialized here — only on first delete (lazy init)
  const [redis, kafkaProducer] = await Promise.all([
    getRedis(log),
    getProducer(log)
  ]);

  log.cid = cid;

  // ── Extract collection, document, handle deletes ──────────────
  let collection, doc;

  if (source === "mongo") {
    collection    = changeEvent.ns?.coll;
    const opType  = changeEvent.operationType;
    log = makeLogger(cid, source, collection);
    log.info(`operationType: ${opType}`);

    if (opType === "delete") {
      const docId = changeEvent.documentKey?._id;
      log = makeLogger(cid, source, collection, docId);
      log.info(`► RECEIVED DELETE`);
      const result = await forwardDelete(source, collection, docId, kafkaProducer, log);
      return { statusCode: 200, body: JSON.stringify(result) };
    }

    doc = changeEvent.fullDocument;

  } else {
    
    const payload = changeEvent.payload || changeEvent;

    // Drop Couchbase Sync Gateway internal documents
    if (payload.key && payload.key.startsWith("_sync")) {
      log = makeLogger(cid, source, "-", payload.key);
      log.skip(`Sync Gateway internal document`);
      return { statusCode: 200, body: JSON.stringify({ action: "skipped", reason: "_sync document" }) };
    }

    // Extract collection from _cbCollection: "bucket._default.User" → "User"
    const cbCollection = payload._cbCollection || "";
    collection = cbCollection.split(".").pop();
    log = makeLogger(cid, source, collection);

    if (!collection || collection === "_default") {
      log.skip(`_default collection`);
      return { statusCode: 200, body: JSON.stringify({ action: "skipped", reason: "_default" }) };
    }

    log.info(`event: ${payload.event}`);

    if (payload.event === "deletion") {
      const docId = payload.key;
      log = makeLogger(cid, source, collection, docId);
      log.info(`► RECEIVED DELETE`);
      const result = await forwardDelete(source, collection, docId, kafkaProducer, log);
      return { statusCode: 200, body: JSON.stringify(result) };
    }

    if (!payload.content) {
      log.error(`No content field in Couchbase message`);
      return { statusCode: 400, body: JSON.stringify({ error: "Missing content" }) };
    }

    try {
      const contentStr = Buffer.from(payload.content, "base64").toString("utf8");
      doc = JSON.parse(contentStr);
    } catch (e) {
      log.error(`Failed to decode base64 content: ${e.message}`);
      return { statusCode: 400, body: JSON.stringify({ error: "Invalid content" }) };
    }

    // Ensure mongoId is set — fall back to payload.key if missing from doc body
    if (!doc.mongoId) {
      doc.mongoId = payload.key;
    }
  }

  if (!collection || !doc) {
    log.error(`Missing collection or document after parsing`);
    return { statusCode: 400, body: JSON.stringify({ error: "Missing collection or document" }) };
  }

  // ── Process ───────────────────────────────────────────────────
  const result = await processMessage(source, collection, doc, redis, kafkaProducer, log);

  log.info(`■ DONE | action: ${result.action}${result.reason ? ` | reason: ${result.reason}` : ""}`);

  return {
    statusCode: 200,
    body: JSON.stringify(result)
  };
};