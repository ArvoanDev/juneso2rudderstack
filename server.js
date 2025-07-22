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

// --- NEW: Tracking Plan Generation ---
function generateTrackingPlan(identifiesRows, groupsRows, eventsRows) {
    const plan = [];

    // Process Identifies
    const identifyProperties = new Set();
    identifiesRows.forEach(row => {
        const traits = JSON.parse(row.traits || '{}');
        Object.keys(flattenObject(traits)).forEach(prop => identifyProperties.add(prop));
    });
    if (identifiesRows.length > 0) {
        plan.push({ ruleType: 'identify', ruleName: 'Identify', properties: Array.from(identifyProperties).sort() });
    }

    // Process Groups
    const groupProperties = new Set();
    groupsRows.forEach(row => {
        const traits = JSON.parse(row.traits || '{}');
        Object.keys(flattenObject(traits)).forEach(prop => groupProperties.add(prop));
    });
    if (groupsRows.length > 0) {
        plan.push({ ruleType: 'group', ruleName: 'Group', properties: Array.from(groupProperties).sort() });
    }

    // Process Events (Pages and Tracks)
    const eventsByTypeAndName = {};
    eventsRows.forEach(row => {
        const type = row.type === '0' ? 'page' : 'track';
        let name = row.name;
        if (type === 'page') {
            try {
                const props = JSON.parse(row.properties || '{}');
                name = (props && typeof props.name === 'string' && props.name.trim()) ? props.name.trim() : '(Not Set)';
            } catch { name = '(Not Set)'; }
        }
        
        const key = `${type}:${name}`;
        if (!eventsByTypeAndName[key]) {
            eventsByTypeAndName[key] = { ruleType: type, ruleName: name, properties: new Set() };
        }
        
        const properties = JSON.parse(row.properties || '{}');
        Object.keys(flattenObject(properties)).forEach(prop => eventsByTypeAndName[key].properties.add(prop));
    });

    Object.values(eventsByTypeAndName).forEach(event => {
        plan.push({ ...event, properties: Array.from(event.properties).sort() });
    });

    return plan.sort((a, b) => a.ruleName.localeCompare(b.ruleName));
}


// --- SQL Query Generation ---

async function getTableSchema(table) {
    try {
        const [metadata] = await table.getMetadata();
        return metadata.schema.fields || [];
    } catch (e) {
        if (e.code !== 404) throw e;
        return [];
    }
}

async function generateSqlQueries(tableNames) {
    const projectId = process.env.BIGQUERY_PROJECT_ID;
    const migrationDatasetId = process.env.BIGQUERY_DATASET_ID;
    const liveDatasetId = 'your_live_rudderstack_dataset';
    const viewsDatasetId = 'analytics_views';

    const viewQueries = [];
    const mergeQueries = [];

    const viewHeader = `-- Non-Destructive VIEW Queries (Recommended)`;
    const mergeHeader = `-- Destructive MERGE (Upsert) Queries`;

    for (const table of tableNames) {
        const migrationTable = bigquery.dataset(migrationDatasetId).table(table);
        const migrationSchema = await getTableSchema(migrationTable);
        const allColumnNames = migrationSchema.map(f => f.name);

        if (allColumnNames.length === 0) continue;

        const columns = allColumnNames.map(c => `  ${c}`).join(',\n');
        const viewSql = `CREATE OR REPLACE VIEW \`${projectId}.${viewsDatasetId}.${table}\` AS\nSELECT\n${columns}\nFROM \`${projectId}.${liveDatasetId}.${table}\`\nUNION ALL\nSELECT\n${columns}\nFROM \`${projectId}.${migrationDatasetId}.${table}\`;`;
        viewQueries.push({ table, query: viewSql });

        const mergeSql = `MERGE \`${projectId}.${liveDatasetId}.${table}\` T\nUSING \`${projectId}.${migrationDatasetId}.${table}\` S\nON T.id = S.id\nWHEN NOT MATCHED BY TARGET THEN\n  INSERT (${allColumnNames.join(', ')})\n  VALUES (${allColumnNames.join(', ')});`;
        mergeQueries.push({ table, query: mergeSql });
    }

    return {
        message: `Upload successful! Here are the SQL queries to merge your migrated data.`,
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
        if (tableId === 'users') finalRow.id = row.user_id;
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

// --- Express Routes ---
app.post('/upload', upload.fields([
    { name: 'identifies', maxCount: 1 }, { name: 'groups', maxCount: 1 }, { name: 'events', maxCount: 1 }, { name: 'dryRun' }
]), async (req, res) => {
    if (!req.files) return res.status(400).send('No files were uploaded.');
    
    const isDryRun = req.body.dryRun === 'true';
    const processedTables = new Set();

    try {
        console.log(`\n--- Starting New Upload Process (Dry Run: ${isDryRun}) ---`);

        const identifiesRows = req.files.identifies ? await parseCsvToRows(req.files.identifies[0]) : [];
        const groupsRows = req.files.groups ? await parseCsvToRows(req.files.groups[0]) : [];
        const eventsRows = req.files.events ? await parseCsvToRows(req.files.events[0]) : [];

        if (identifiesRows.length > 0) {
            processedTables.add('identifies').add('users');
            if (!isDryRun) {
                await manageSchemaAndInsert('identifies', IDENTIFIES_SCHEMA, identifiesRows, 'traits', processedTables);
                await manageSchemaAndInsert('users', USERS_SCHEMA, identifiesRows, 'traits', processedTables);
            }
        }
        if (groupsRows.length > 0) {
            processedTables.add('groups');
            if (!isDryRun) {
                await manageSchemaAndInsert('groups', GROUPS_SCHEMA, groupsRows, 'traits', processedTables);
            }
        }
        if (eventsRows.length > 0) {
            const pageRows = eventsRows.filter(row => row.type === '0');
            const trackRows = eventsRows.filter(row => row.type === '2');
            if (pageRows.length > 0) processedTables.add('pages');
            if (trackRows.length > 0) processedTables.add('tracks');
            trackRows.forEach(event => processedTables.add(sanitizeTableName(event.name)));

            if (!isDryRun) {
                await manageSchemaAndInsert('pages', PAGES_SCHEMA, pageRows, 'properties', processedTables);
                await manageSchemaAndInsert('tracks', TRACKS_SCHEMA, trackRows, 'properties', processedTables);
                const tracksByName = trackRows.reduce((acc, event) => {
                    const tableName = sanitizeTableName(event.name);
                    if (!acc[tableName]) acc[tableName] = [];
                    acc[tableName].push(event);
                    return acc;
                }, {});
                for (const [tableId, tableRows] of Object.entries(tracksByName)) {
                    await manageSchemaAndInsert(tableId, TRACKS_SCHEMA, tableRows, 'properties', processedTables);
                }
            }
        }

        console.log("\n--- Process Completed Successfully ---\n");
        const sqlQueries = await generateSqlQueries(processedTables);
        const trackingPlan = generateTrackingPlan(identifiesRows, groupsRows, eventsRows);
        
        res.status(200).json({ ...sqlQueries, trackingPlan });
    } catch (error) {
        console.error("\n--- An error occurred during the upload process ---");
        console.error("Final error caught in handler:", error.message);
        res.status(500).send(`An error occurred. Check the server console for detailed logs.`);
    }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(port, () => console.log(`Server running at http://localhost:${port}`));
