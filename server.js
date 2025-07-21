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
const sanitizeTableName = (name) => name ? camelToSnakeCase(name).replace(/[^a-zA-Z0-9_]/g, '_') : 'unknown_event';

function flattenObject(obj, prefix = '') {
    const result = {};
    if (!obj || typeof obj !== 'object') return result;
    for (let [key, value] of Object.entries(obj)) {
        key = camelToSnakeCase(key); // Convert key to snake_case
        const newKey = prefix ? `${prefix}_${key}` : key;
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
            Object.assign(result, flattenObject(value, newKey));
        } else {
            result[newKey] = value;
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
    const table = bigquery.dataset(datasetId).table(tableId);

    const discoveredFields = new Map();
    rows.forEach(row => {
        const dynamicData = JSON.parse(row[dynamicPropertyKey] || '{}');
        const flattenedDynamic = flattenObject(dynamicData);
        for (const [key, value] of Object.entries(flattenedDynamic)) {
            const type = inferBigQueryType(value);
            if (!discoveredFields.has(key) || (discoveredFields.get(key) !== 'STRING' && type === 'STRING')) {
               discoveredFields.set(key, type);
            }
        }
    });

    const newSchemaFields = Array.from(discoveredFields, ([name, type]) => ({ name, type }));
    const finalSchema = [...baseSchema, ...newSchemaFields];

    const [exists] = await table.exists();
    if (!exists) {
        console.log(`Table ${tableId} not found. Creating...`);
        await bigquery.dataset(datasetId).createTable(tableId, { schema: finalSchema });
    } else {
        const [metadata] = await table.getMetadata();
        const existingFields = new Set(metadata.schema.fields.map(f => f.name));
        const fieldsToAdd = finalSchema.filter(field => !existingFields.has(field.name));
        if (fieldsToAdd.length > 0) {
            console.log(`Evolving schema for table ${tableId}, adding: ${fieldsToAdd.map(f => f.name).join(', ')}`);
            metadata.schema.fields.push(...fieldsToAdd);
            await table.setMetadata(metadata);
        }
    }

    const flattenedRows = rows.map(row => {
        const dynamicData = JSON.parse(row[dynamicPropertyKey] || '{}');
        const context = JSON.parse(row.context || '{}');
        
        return {
            id: row.message_id,
            anonymous_id: row.anonymous_id,
            user_id: row.user_id,
            received_at: row.received_at,
            sent_at: row.sent_at,
            timestamp: row.timestamp,
            original_timestamp: row.original_timestamp,
            channel: row.channel,
            version: row.version,
            event: row.name,
            event_text: row.name,
            group_id: row.group_id,
            ...flattenObject(context, 'context'),
            ...flattenObject(dynamicData),
        };
    });
    
    await table.insert(flattenedRows);
    console.log(`Inserted ${rows.length} rows into ${tableId}`);
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

    await manageSchemaAndInsert('pages', PAGES_SCHEMA, pageRows, 'properties');
    await manageSchemaAndInsert('tracks', TRACKS_SCHEMA, trackRows, 'properties');

    const eventsByName = trackRows.reduce((acc, event) => {
        const tableName = sanitizeTableName(event.name);
        if (!acc[tableName]) acc[tableName] = [];
        acc[tableName].push(event);
        return acc;
    }, {});

    for (const [tableId, rows] of Object.entries(eventsByName)) {
        await manageSchemaAndInsert(tableId, TRACKS_SCHEMA, rows, 'properties');
    }
}

// --- Express Routes ---
app.post('/upload', upload.fields([
    { name: 'identifies', maxCount: 1 }, { name: 'groups', maxCount: 1 }, { name: 'events', maxCount: 1 },
]), async (req, res) => {
    if (!req.files) return res.status(400).send('No files were uploaded.');
    try {
        const promises = [];
        if (req.files.identifies) promises.push(processIdentifiesFile(req.files.identifies[0]));
        if (req.files.groups) promises.push(processGroupsFile(req.files.groups[0]));
        if (req.files.events) promises.push(processEventsFile(req.files.events[0]));
        await Promise.all(promises);
        res.status(200).send('Files processed and uploaded to BigQuery successfully!');
    } catch (error) {
        console.error('BIGQUERY_ERROR:', error.errors || error);
        res.status(500).send(`An error occurred: ${error.message}`);
    }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(port, () => console.log(`Server running at http://localhost:${port}`));