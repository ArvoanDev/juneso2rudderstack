require('dotenv').config();
const express = require('express');
const multer = require('multer');
const { BigQuery } = require('@google-cloud/bigquery');
const csv = require('csv-parser');
const stream = require('stream');
const path = require('path');
const { IDENTIFIES_SCHEMA, GROUPS_SCHEMA, PAGES_SCHEMA, TRACKS_SCHEMA, USERS_SCHEMA } = require('./schema');

const app = express();
const port = 3000;

const upload = multer({ storage: multer.memoryStorage() });
const bigquery = new BigQuery({ projectId: process.env.BIGQUERY_PROJECT_ID });
const datasetId = process.env.BIGQUERY_DATASET_ID;

// --- Helper Functions ---

const camelToSnakeCase = (str) => str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);

// A more robust function to sanitize event names into valid table names.
const sanitizeTableName = (name) => {
    if (!name) return 'unknown_event';
    const snakeCased = name
        .replace(/([A-Z])/g, '_$1')
        .replace(/[\s\-]+/g, '_')
        .replace(/[^a-zA-Z0-9_]/g, '');
    const cleaned = snakeCased
        .replace(/__+/g, '_')
        .toLowerCase()
        .replace(/^_|_$/g, '');
    return cleaned || 'unknown_event';
};


function flattenObject(obj, prefix = '') {
    const result = {};
    if (!obj || typeof obj !== 'object') return result;
    for (let [key, value] of Object.entries(obj)) {
        key = camelToSnakeCase(key);
        const newKey = prefix ? `${prefix}_${key}` : key;
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
            Object.assign(result, flattenObject(value, newKey));
        } else {
            result[newKey] = Array.isArray(value) ? JSON.stringify(value) : value;
        }
    }
    return result;
}

function inferBigQueryType(value) {
    if (typeof value === 'boolean') return 'BOOL';
    if (typeof value === 'number') return Number.isInteger(value) ? 'INT64' : 'FLOAT64';
    if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) return 'TIMESTAMP';
    return 'STRING';
}

// --- Core BigQuery Logic ---

async function manageSchemaAndInsert(tableId, baseSchema, rows, dynamicPropertyKey) {
    if (rows.length === 0) return;
    let table = bigquery.dataset(datasetId).table(tableId);

    const flattenedRows = rows.map(row => {
        const dynamicData = JSON.parse(row[dynamicPropertyKey] || '{}');
        const context = JSON.parse(row.context || '{}');
        
        const finalRow = {
            id: row.message_id, anonymous_id: row.anonymous_id, user_id: row.user_id,
            received_at: row.received_at, sent_at: row.sent_at, timestamp: row.timestamp,
            original_timestamp: row.original_timestamp, channel: row.channel, version: row.version,
            group_id: row.group_id,
        };

        // **THE FIX**: Only add 'event' and 'event_text' for track-related tables.
        if (baseSchema === TRACKS_SCHEMA) {
            finalRow.event = sanitizeTableName(row.name);
            finalRow.event_text = row.name;
        }

        Object.assign(finalRow, flattenObject(context, 'context'));
        Object.assign(finalRow, flattenObject(dynamicData));

        Object.keys(finalRow).forEach(key => finalRow[key] === undefined && delete finalRow[key]);
        return finalRow;
    });

    const discoveredFields = new Map();
    flattenedRows.forEach(row => {
        for (const [key, value] of Object.entries(row)) {
            const type = inferBigQueryType(value);
            if (!discoveredFields.has(key) || (discoveredFields.get(key) !== 'STRING' && type === 'STRING')) {
               discoveredFields.set(key, type);
            }
        }
    });
    
    baseSchema.forEach(field => discoveredFields.set(field.name, field.type));
    
    const finalSchema = Array.from(discoveredFields, ([name, type]) => ({ name, type }));

    const [exists] = await table.exists();
    if (!exists) {
        console.log(`[${tableId}] Table not found. Creating with complete schema...`);
        const [createdTable] = await bigquery.dataset(datasetId).createTable(tableId, { schema: finalSchema });
        console.log(`[${tableId}] createTable operation completed.`);
        table = createdTable;
    } else {
        const [metadata] = await table.getMetadata();
        const existingFields = new Set(metadata.schema.fields.map(f => f.name));
        const fieldsToAdd = finalSchema.filter(field => !existingFields.has(field.name));
        if (fieldsToAdd.length > 0) {
            console.log(`[${tableId}] Evolving schema, adding: ${fieldsToAdd.map(f=>f.name).join(', ')}`);
            metadata.schema.fields.push(...fieldsToAdd);
            await table.setMetadata(metadata);
        }
    }
    
    let attempts = 0;
    const maxAttempts = 5;
    const delay = 2000;

    while (attempts < maxAttempts) {
        try {
            console.log(`[${tableId}] Attempting to insert ${flattenedRows.length} rows (Attempt ${attempts + 1})...`);
            await table.insert(flattenedRows);
            console.log(`[${tableId}] Successfully inserted ${flattenedRows.length} rows.`);
            return;
        } catch (err) {
            if (err.code === 404 && attempts < maxAttempts - 1) {
                attempts++;
                console.warn(`[${tableId}] Insert failed because table was not found. Retrying in ${delay * attempts / 1000}s...`);
                await new Promise(resolve => setTimeout(resolve, delay * attempts));
            } else {
                console.error(`\n--- BigQuery Insert Error in table: ${tableId} ---`);
                console.error("Full error object:", JSON.stringify(err, null, 2));
                console.error("--- End of Error Details ---\n");
                throw err;
            }
        }
    }
}

// --- File Processors ---

function parseCsvToRows(file) {
    return new Promise((resolve, reject) => {
        const rows = [];
        const bufferStream = new stream.PassThrough();
        bufferStream.end(file.buffer);
        bufferStream.pipe(csv()).on('data', row => rows.push(row)).on('end', () => resolve(rows)).on('error', reject);
    });
}

async function processIdentifiesFile(file) {
    const rows = await parseCsvToRows(file);
    await manageSchemaAndInsert('identifies', IDENTIFIES_SCHEMA, rows, 'traits');
    await manageSchemaAndInsert('users', USERS_SCHEMA, rows, 'traits');
}

async function processGroupsFile(file) {
    const rows = await parseCsvToRows(file);
    await manageSchemaAndInsert('_groups', GROUPS_SCHEMA, rows, 'traits');
}

async function processEventsFile(file) {
    const allEvents = await parseCsvToRows(file);
    const pageRows = allEvents.filter(row => row.type === '0');
    const trackRows = allEvents.filter(row => row.type === '2');

    // Process main tables
    await manageSchemaAndInsert('pages', PAGES_SCHEMA, pageRows, 'properties');
    await manageSchemaAndInsert('tracks', TRACKS_SCHEMA, trackRows, 'properties');

    // Process track-specific tables
    const tracksByName = trackRows.reduce((acc, event) => {
        const tableName = sanitizeTableName(event.name);
        if (!acc[tableName]) acc[tableName] = [];
        acc[tableName].push(event);
        return acc;
    }, {});

    for (const [tableId, rows] of Object.entries(tracksByName)) {
        await manageSchemaAndInsert(tableId, TRACKS_SCHEMA, rows, 'properties');
    }
}

// --- Express Routes ---
app.post('/upload', upload.fields([
    { name: 'identifies', maxCount: 1 }, { name: 'groups', maxCount: 1 }, { name: 'events', maxCount: 1 },
]), async (req, res) => {
    if (!req.files) return res.status(400).send('No files were uploaded.');
    try {
        console.log("\n--- Starting New Upload Process ---");

        if (req.files.identifies) {
            console.log("\n[Main] Processing identifies file...");
            await processIdentifiesFile(req.files.identifies[0]);
            console.log("[Main] Finished processing identifies file.");
        }
        if (req.files.groups) {
            console.log("\n[Main] Processing groups file...");
            await processGroupsFile(req.files.groups[0]);
            console.log("[Main] Finished processing groups file.");
        }
        if (req.files.events) {
            console.log("\n[Main] Processing events file...");
            await processEventsFile(req.files.events[0]);
            console.log("[Main] Finished processing events file.");
        }

        console.log("\n--- Upload Process Completed Successfully ---\n");
        res.status(200).send('Files processed and uploaded to BigQuery successfully!');
    } catch (error) {
        console.error("\n--- An error occurred during the upload process ---");
        console.error("Final error caught in handler:", error.message);
        res.status(500).send(`An error occurred. Check the server console for detailed logs.`);
    }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(port, () => console.log(`Server running at http://localhost:${port}`));
// --- End of server.js ---