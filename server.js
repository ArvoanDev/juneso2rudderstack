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

// --- SQL Query Generation ---

function generateSqlQueries(tableNames) {
    const projectId = process.env.BIGQUERY_PROJECT_ID;
    const migrationDataset = process.env.BIGQUERY_DATASET_ID;
    const liveDataset = 'your_live_rudderstack_dataset'; // Placeholder
    const viewsDataset = 'analytics_views'; // Placeholder

    const viewQueries = [];
    const mergeQueries = [];

    const viewHeader = `-- =====================================================================================
-- Non-Destructive VIEW Queries (Recommended)
-- These create a virtual layer for analysis without moving or duplicating data.
-- Run these queries in a new dataset (e.g., '${viewsDataset}').
-- =====================================================================================`;

    const mergeHeader = `-- =====================================================================================
-- Destructive MERGE (Upsert) Queries
-- These physically copy data from the migration dataset to your live dataset.
-- Use with caution. Back up your live tables before running.
-- =====================================================================================`;

    for (const table of tableNames) {
        const viewSql = `-- For table: ${table}\n` +
        `CREATE OR REPLACE VIEW \`${projectId}.${viewsDataset}.${table}\` AS\n` +
        `SELECT * FROM \`${projectId}.${liveDataset}.${table}\`\n` +
        `UNION ALL\n` +
        `SELECT * FROM \`${projectId}.${migrationDataset}.${table}\`;`;
        viewQueries.push({ table, query: viewSql });

        const mergeSql = `-- For table: ${table}\n` +
        `MERGE \`${projectId}.${liveDataset}.${table}\` T\n` +
        `USING \`${projectId}.${migrationDataset}.${table}\` S\n` +
        `ON T.id = S.id\n` +
        `WHEN NOT MATCHED BY TARGET THEN\n` +
        `  INSERT ROW;`;
        mergeQueries.push({ table, query: mergeSql });
    }

    return {
        message: `SQL Generation successful! Here are the queries to merge your data.`,
        viewHeader,
        mergeHeader,
        viewQueries,
        mergeQueries
    };
}

// --- Core BigQuery Logic ---

async function manageSchemaAndInsert(tableId, baseSchema, rows, dynamicPropertyKey, processedTablesSet) {
    processedTablesSet.add(tableId);
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
        console.log(`[${tableId}] Table not found. Creating...`);
        [table] = await bigquery.dataset(datasetId).createTable(tableId, { schema: finalSchema });
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
                console.warn(`[${tableId}] Insert failed, retrying in ${delay * attempts / 1000}s...`);
                await new Promise(resolve => setTimeout(resolve, delay * attempts));
            } else {
                console.error(`\n--- BigQuery Insert Error in table: ${tableId} ---`);
                console.error("Full error object:", JSON.stringify(err, null, 2));
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

async function processFile(file, type, processedTablesSet, isDryRun) {
    const rows = await parseCsvToRows(file);
    switch (type) {
        case 'identifies':
            processedTablesSet.add('identifies').add('users');
            if (!isDryRun) {
                await manageSchemaAndInsert('identifies', IDENTIFIES_SCHEMA, rows, 'traits', processedTablesSet);
                await manageSchemaAndInsert('users', USERS_SCHEMA, rows, 'traits', processedTablesSet);
            }
            break;
        case 'groups':
            processedTablesSet.add('_groups');
            if (!isDryRun) {
                await manageSchemaAndInsert('_groups', GROUPS_SCHEMA, rows, 'traits', processedTablesSet);
            }
            break;
        case 'events':
            const pageRows = rows.filter(row => row.type === '0');
            const trackRows = rows.filter(row => row.type === '2');
            if (pageRows.length > 0) processedTablesSet.add('pages');
            if (trackRows.length > 0) processedTablesSet.add('tracks');
            
            const tracksByName = trackRows.reduce((acc, event) => {
                const tableName = sanitizeTableName(event.name);
                processedTablesSet.add(tableName);
                if (!acc[tableName]) acc[tableName] = [];
                acc[tableName].push(event);
                return acc;
            }, {});

            if (!isDryRun) {
                await manageSchemaAndInsert('pages', PAGES_SCHEMA, pageRows, 'properties', processedTablesSet);
                await manageSchemaAndInsert('tracks', TRACKS_SCHEMA, trackRows, 'properties', processedTablesSet);
                for (const [tableId, tableRows] of Object.entries(tracksByName)) {
                    await manageSchemaAndInsert(tableId, TRACKS_SCHEMA, tableRows, 'properties', processedTablesSet);
                }
            }
            break;
    }
}

// --- Express Routes ---
app.post('/upload', upload.fields([
    { name: 'identifies', maxCount: 1 }, { name: 'groups', maxCount: 1 }, { name: 'events', maxCount: 1 }, { name: 'dryRun' }
]), async (req, res) => {
    if (!req.files) return res.status(400).send('No files were uploaded.');
    
    const isDryRun = req.body.dryRun === 'true';
    const processedTables = new Set();

    try {
        console.log(`\n--- Starting New Upload Process (Dry Run: ${isDryRun}) ---`);

        if (req.files.identifies) {
            console.log("\n[Main] Processing identifies file...");
            await processFile(req.files.identifies[0], 'identifies', processedTables, isDryRun);
        }
        if (req.files.groups) {
            console.log("\n[Main] Processing groups file...");
            await processFile(req.files.groups[0], 'groups', processedTables, isDryRun);
        }
        if (req.files.events) {
            console.log("\n[Main] Processing events file...");
            await processFile(req.files.events[0], 'events', processedTables, isDryRun);
        }

        console.log("\n--- Process Completed Successfully ---\n");
        const sqlQueries = generateSqlQueries(processedTables);
        res.status(200).json(sqlQueries);
    } catch (error) {
        console.error("\n--- An error occurred during the upload process ---");
        console.error("Final error caught in handler:", error.message);
        res.status(500).send(`An error occurred. Check the server console for detailed logs.`);
    }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(port, () => console.log(`Server running at http://localhost:${port}`));
