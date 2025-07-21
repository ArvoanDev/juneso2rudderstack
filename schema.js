// schema.js

// Defines the universal base columns present in all RudderStack tables,
// including flattened context fields.
const BASE_SCHEMA = [
    { name: 'id', type: 'STRING' },
    { name: 'anonymous_id', type: 'STRING' },
    { name: 'user_id', type: 'STRING' },
    { name: 'received_at', type: 'TIMESTAMP' },
    { name: 'sent_at', type: 'TIMESTAMP' },
    { name: 'timestamp', type: 'TIMESTAMP' },
    { name: 'original_timestamp', type: 'TIMESTAMP' },
    { name: 'channel', type: 'STRING' },
    { name: 'loaded_at', type: 'TIMESTAMP' },
    { name: 'uuid_ts', type: 'TIMESTAMP' },
];

// Specific schemas only add fields that are not part of traits or properties
const IDENTIFIES_SCHEMA = [...BASE_SCHEMA];
const GROUPS_SCHEMA = [...BASE_SCHEMA, { name: 'group_id', type: 'STRING' }];
const PAGES_SCHEMA = [...BASE_SCHEMA, { name: 'name', type: 'STRING' }];
const TRACKS_SCHEMA = [...BASE_SCHEMA, { name: 'event', type: 'STRING' }, { name: 'event_text', type: 'STRING' }];
const USERS_SCHEMA = [{ name: 'user_id', type: 'STRING' }];

module.exports = {
    IDENTIFIES_SCHEMA,
    GROUPS_SCHEMA,
    PAGES_SCHEMA,
    TRACKS_SCHEMA,
    USERS_SCHEMA,
};