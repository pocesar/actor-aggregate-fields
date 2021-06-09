const Apify = require('apify');
const _ = require('lodash');

const { log } = Apify.utils;

Apify.main(async () => {
    const input = await Apify.getInput();

    if (!input || typeof input !== 'object') {
        throw new Error('Missing input');
    }

    const {
        datasetId,
        fieldNames = false,
        fields = [],
        split = {},
    } = input;

    if ((!fields || !fields.length) && !fieldNames) {
        throw new Error('Missing required "fields" or "fieldNames" parameters');
    }

    if (!datasetId) {
        throw new Error('You must specify "datasetId" parameter');
    }

    const dataset = await Apify.openDataset(datasetId);

    const { cleanItemCount } = await dataset.getInfo();

    const aggregate = {};
    /** @type {any} */
    const savedValues = (await Apify.getValue('SAVED_VALUES')) || {};
    /** @type {number} */
    let currentOffset = (await Apify.getValue('CURRENT_OFFSET')) || 0;

    const persistState = async () => {
        const serializable = {};

        Object.entries(aggregate).forEach(([k, v]) => {
            serializable[k] = [...v.values.values()]; // need to convert Set to array otherwise it will be an {}
        });

        await Apify.setValue('SAVED_VALUES', serializable);
        await Apify.setValue('CURRENT_OFFSET', currentOffset);
    };

    Apify.events.on('persistState', persistState);

    for (const field of (fieldNames ? ['__DEFAULT'] : fields)) {
        aggregate[field] = {
            values: new Set(field in savedValues ? savedValues[field] : []),
            count: 0,
            min: 0,
            max: 0,
            average: 0,
        };
    }

    const splitMap = (str, toSplit, cb) => str.split(toSplit).forEach(cb);

    log.info(`Aggregating data from ${cleanItemCount} items from index ${currentOffset}...`);

    await dataset.forEach(async (item) => {
        currentOffset++;

        if (fieldNames) {
            Object.keys(item).forEach((fieldName) => {
                aggregate['__DEFAULT'].values.add(fieldName); // eslint-disable-line dot-notation
            });
        } else {
            for (const field of fields) {
                const path = _.get(item, field);

                if (Array.isArray(path)) {
                    path.forEach((c) => {
                        if (field in split && typeof c === 'string') {
                            splitMap(c, split[field], (s) => {
                                aggregate[field].values.add(s);
                            });
                        } else {
                            aggregate[field].values.add(c);
                        }
                    });
                } else if (path) {
                    if (field in split && typeof path === 'string') {
                        splitMap(path, split[field], (s) => {
                            aggregate[field].values.add(s);
                        });
                    } else {
                        aggregate[field].values.add(path);
                    }
                }
            }
        }
    }, {}, currentOffset);

    log.info('Writing OUTPUT...');

    await Apify.setValue('OUTPUT', Object.entries(aggregate).reduce((acc, [field, set]) => {
        const values = [...set.values.values()];
        const lengths = values.map((s) => {
            if (typeof s === 'string' || Array.isArray(s)) {
                return s.length;
            }
            if (typeof s === 'number') {
                return s;
            }
            return `${s}`.length;
        });
        const min = lengths.reduce((o, i) => (i < o ? i : o), Infinity);
        const max = lengths.reduce((o, i) => (i > o ? i : o), -Infinity);
        const value = {
            values,
            count: values.length,
            min,
            max,
            average: values.length ? Math.round(lengths.reduce((o, v) => o + v, 0) / lengths.length) : 0,
        };

        if (fieldNames) {
            return value;
        }

        return {
            ...acc,
            [field]: {
                ...value,
                fieldName: field,
            },
        };
    }, {}));

    await persistState();

    log.info('Done, check OUTPUT on key value store');
});
